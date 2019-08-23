/*
 * Copyright 2019 Joyent, Inc.
 */

use std::convert::From;
use std::fmt::Debug;
use std::net::{AddrParseError, SocketAddr};
use std::rc::Rc;
use std::str::{FromStr};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use itertools::Itertools;
use futures::future::{ok, loop_fn, Future, FutureResult, Loop, lazy};
use serde_json;
use serde_json::Value;
use serde_json::error::Error as SerdeJsonError;
use tokio_zookeeper::*;
use tokio::prelude::*;
use url::{Url, ParseError};

// TODO remove once done debugging
use std::net::TcpStream;

use failure;


use cueball::backend::*;
use cueball::resolver::{
    BackendAddedMsg,
    BackendRemovedMsg,
    BackendMsg,
    Resolver
};

// TODO make sure set_error is being called everywhere there's a client-facing
// error

// An error type to be used internally.
// The "should_stop" field indicates whether the Resolver should stop() in
// response to the error -- if false, it will instead continue watching for
// cluster state changes.
#[derive(Debug, Clone)]
pub struct ResolverError {
    message: String,
    should_stop: bool
}

impl From<ParseError> for ResolverError {
    fn from(e: ParseError) -> Self {
        ResolverError {
            message: format!("{:?}", e),
            should_stop: false
        }
    }
}

impl From<SerdeJsonError> for ResolverError {
    fn from(e: SerdeJsonError) -> Self {
        ResolverError {
            message: format!("{:?}", e),
            should_stop: false
        }
    }
}

impl From<AddrParseError> for ResolverError {
    fn from(e: AddrParseError) -> Self {
        ResolverError {
            message: format!("{:?}", e),
            should_stop: false
        }
    }
}

#[derive(Debug, Clone)]
enum ResolverEvent {
    StopEvent,
    DataChangedEvent,
    SessionEvent,
}

#[derive(Debug, Clone)]
enum MachineState {
    NoSession,
    HandlingEvent,
    WaitingForEvent,
    Stopping,
}

#[derive(Debug)]
enum MachineEvent {
    Connect,
    ConnectError,
    Stop,
    ArmWatch,
    ReceiveEvent,
}

#[derive(Debug)]
enum LoopState {
    Connect,
    StopConnectError,
    Stop,
    ArmWatch,
    ReceiveEvent,
}

/// A simple state machine for internal use. Models the current state of the
/// ManateePrimaryResolver in regards to zookeeper sessions and watches.
#[derive(Debug)]
pub struct StateMachine {
    state: MachineState,
    zk: Option<ZooKeeper>,
}

impl StateMachine {
    fn new() -> Self {
        StateMachine {
            state: MachineState::NoSession,
            zk: None
        }
    }

    /// Given an event, transitions to the next state. The match statement below
    /// is the definitive source for state transitions.
    fn set_next_state(&mut self, event: MachineEvent) {
        let next = match (&self.state, event) {
            (MachineState::NoSession, MachineEvent::Connect) =>
                MachineState::HandlingEvent,
            (MachineState::NoSession, MachineEvent::ConnectError) =>
                MachineState::NoSession,
            (MachineState::HandlingEvent, MachineEvent::ConnectError) =>
                MachineState::NoSession,
            (MachineState::HandlingEvent, MachineEvent::Stop) =>
                MachineState::Stopping,
            (MachineState::HandlingEvent, MachineEvent::ArmWatch) =>
                MachineState::WaitingForEvent,
            (MachineState::WaitingForEvent, MachineEvent::ReceiveEvent) =>
                MachineState::HandlingEvent,
            (MachineState::WaitingForEvent, MachineEvent::ConnectError) =>
                MachineState::NoSession,
            (s, e) =>
                panic!(
                    "Invalid MachineState/MachineEvent pair: {:?}, {:?}", s, e
                ),
        };
        self.state = next;
    }

    fn get_state(&self) -> MachineState {
        return self.state.clone();
    }

    fn get_zk(&self) -> Option<ZooKeeper> {
        return self.zk.clone();
    }

    fn set_zk(&mut self, zk: &ZooKeeper) {
        self.zk = Some(zk.clone());
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ZKConnectString(Vec<SocketAddr>);

impl ToString for ZKConnectString {
    fn to_string(&self) -> String {
        self
            .0
            .iter()
            .map(|x| x.to_string())
            .intersperse(String::from(","))
            .collect()
    }
}

impl FromStr for ZKConnectString {
    type Err = std::net::AddrParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let acc: Result<Vec<SocketAddr>, Self::Err> = Ok(vec![]);
        s.split(',')
            .map(|x| SocketAddr::from_str(x))
            .fold(acc, |acc, x| {
                match (acc, x) {
                    (Ok(mut addrs), Ok(addr)) => {
                        addrs.push(addr);
                        Ok(addrs)
                    },
                    (Err(e), _) => Err(e),
                    (_, Err(e)) => Err(e)
                }
            })
            .and_then(|x| Ok(ZKConnectString(x)))
    }
}

#[derive(Debug)]
pub struct ManateePrimaryResolver {
    /// An instance of the ZKConnectString type that represents a Zookeeper
    /// connection string
    connect_string: ZKConnectString,
    /// The Zookeeper path for manatee cluster state for the shard. *e.g.*
    /// "/manatee/1.moray.coal.joyent.us/state"
    cluster_state_path: String,
    /// The communications channel with the cueball connection pool
    pool_tx: Option<Sender<BackendMsg>>,
    /// The Sender end of a channel for internal communication between threads
    event_tx: Sender<ResolverEvent>,
    /// The Receiver corresponding to `event_tx` above. Protected by an
    /// Arc/Mutex because the channel must be used by various threads in various
    /// methods, and, unlike with the Sender end, we can't handle Recevier
    /// ownership by cloning it.
    event_rx: Arc<Mutex<Receiver<ResolverEvent>>>,
    /// The last error that occurred. Protected by an Arc/Mutex because it is
    /// accessed by multiple internal threads.
    error: Arc<Mutex<Option<String>>>,
    /// The key representation of the last backend sent to the cueball
    /// connection pool. Protected by an Arc/Mutex because it is
    /// accessed by the background thread in run(), but must be persistently
    /// stored across starts/stops in this struct.
    last_backend: Arc<Mutex<Option<BackendKey>>>,
    /// Indicates if the resolver is running. Protected by an Arc/Mutex because
    /// it is accessed by multiple internal threads.
    running: Arc<Mutex<bool>>,
}

impl ManateePrimaryResolver {

    /// Creates a new ManateePrimaryResolver instance
    ///
    /// # Arguments
    ///
    /// * `connect_string` - a comma-separated list of the zookeeper instances
    ///   in the cluster
    /// * `path` - The path to the root node in zookeeper for the shard we're
    //    watching
    pub fn new(
        connect_string: ZKConnectString,
        path: String
    ) -> Self
    {
        // TODO change 'dean' to 'state' once you're done testing
        let cluster_state_path = [&path, "/dean"].concat();
        let (tx, rx) = channel();

        ManateePrimaryResolver {
            connect_string,
            cluster_state_path,
            pool_tx: None,
            event_tx: tx,
            event_rx: Arc::new(Mutex::new(rx)),
            error: Arc::new(Mutex::new(None)),
            last_backend: Arc::new(Mutex::new(None)),
            running: Arc::new(Mutex::new(false)),
        }
    }

    /// Sets the appropriate zookeeper watches and waits for changes to occur in
    /// a loop. For internal use; called by the start() method in a background
    /// thread.
    ///
    /// # Arguments
    ///
    /// * `connect_string` - A comma-separated list of the zookeeper instances
    ///   in the cluster
    /// * `path` - The path to the cluster state node in zookeeper for the shard
    ///   we're watching
    /// * `pool_tx` - The Sender that this function should use to communicate
    ///   with the cueball connection pool
    /// * `event_tx` - The sender that this function should pass to watch
    ///   handlers, so they can transmit events back
    /// * `event_rx` - The receiver paired with `event_tx`.
    ///   arbitrarily.
    /// * `last_backend` - The key representation of the last backend sent to
    ///   the cueball connection pool. It will be updated by process_value() if
    ///   we send a new backend over.
    fn run(
        connect_string: ZKConnectString,
        cluster_state_path: String,
        pool_tx: Sender<BackendMsg>,
        event_tx: Sender<ResolverEvent>,
        event_rx: Arc<Mutex<Receiver<ResolverEvent>>>,
        last_backend: Arc<Mutex<Option<BackendKey>>>,
        error_arcmut: Arc<Mutex<Option<String>>>
    ) {
        // let mut client;
        tokio::run(
            loop_fn(StateMachine::new(), move |machine| {
                println!("outer looping");
                let path_clone = cluster_state_path.clone();
                ZooKeeper::connect(&connect_string.to_string().parse().unwrap())
                    .and_then(move |(zk, default_watcher)| {
                        println!("Got zk client: {:?}", zk);
                        zk
                            .watch()
                            .get_data(&path_clone)
                            .and_then(move |data| {
                                // loop_fn(true, move |should_watch| {
                                //     println!("Inner loop: waiting for event");



                                    default_watcher.into_future().and_then(|(item, _)| {
                                        println!("{:?}", item);
                                        ok(Loop::Break(()))
                                    }).map_err(|e| failure::error::Error::from(e))
                                // })
                            })
                            .and_then(|_| {
                                ok(Loop::Break(()))
                            })
                    })
                    // .map(|_| ())
                    // .map_err(|e| panic!("{:?}", e))
                // match machine.get_state() {

                //     // NoSession: Connect to zookeeper
                //     MachineState::NoSession => {
                //         let cs = connect_string.to_string();
                //         println!("Attempting to connect");
                //         // TODO connect
                //         let machine_event = MachineEvent::Connect;
                //         machine.set_next_state(machine_event)
                //         ZooKeeper::connect(&connect_string.to_string().parse().unwrap())
                //             .and_then(|(zk, default_watcher)| {
                //                 // client = zk;
                //                 println!("{:?}", zk);
                //                 Ok((Loop::Continue(machine)))
                //             })
                //     }
                //     _ => panic!("DEAN!");

                    // // HandlingEvent: We are connected to zookeeper but haven't set
                    // // a watch yet. If we're here, it means that we've received an
                    // // event on the channel (except for the first iteration, which
                    // // is why we set curr_event artificially). Thus, we process
                    // // the event and get ready to receive another event. If the
                    // // event is a DataChangedEvent, that means that the watch was
                    // // fired, so we need to re-set the watch. Other event types do
                    // // not come from watches, so we can just transition to the next
                    // // state.
                    // MachineState::HandlingEvent => {
                    //     // The zk client must exist at this point
                    //     // TODO account for case where cluster_state_path doesn't
                    //     // exist in zk yet -- maybe poll every 30 seconds?
                    //     let machine_event;
                    //     // let zk = zk.as_ref().unwrap();
                    //     // match curr_event {
                    //     //     // The cluster topology has changed -- we thus
                    //     //     // get the new primary and call process_value.
                    //     //     ResolverEvent::DataChangedEvent => {
                    //     //         machine_event = || -> MachineEvent {
                    //     //             let curr_value;
                    //     //             // TODO get value
                    //     //             let pool_tx_clone = pool_tx.clone();
                    //     //             let backend_clone =
                    //     //                 Arc::clone(&last_backend);
                    //     //             match ManateePrimaryResolver::process_value(
                    //     //                 &pool_tx_clone,
                    //     //                 &curr_value,
                    //     //                 backend_clone) {
                    //     //                 Ok(_) => {
                    //     //                     return MachineEvent::ArmWatch;
                    //     //                 },
                    //     //                 Err(e) => {
                    //     //                     ManateePrimaryResolver::set_error(
                    //     //                         Arc::clone(&error_arcmut),
                    //     //                         &e
                    //     //                     );
                    //     //                     if e.should_stop {
                    //     //                         return MachineEvent::Stop;
                    //     //                     } else {
                    //     //                         return MachineEvent::ArmWatch;
                    //     //                     }
                    //     //                 }
                    //     //             }
                    //     //         }();
                    //     //     },
                    //     //     // The state of our session has changed.
                    //     //     ResolverEvent::SessionEvent => {
                    //     //         println!("   oooy!");
                    //     //         machine_event = MachineEvent::ArmWatch;
                    //     //     },
                    //     //     // stop() has been called.
                    //     //     ResolverEvent::StopEvent => {
                    //     //         machine_event = MachineEvent::Stop;
                    //     //     }
                    //     // }
                    //     // machine.set_next_state(machine_event);
                    //     machine.set_next_state(MachineEvent::ArmWatch);
                    // },

                    // // We have nothing to do but wait for something to happen!
                    // MachineState::WaitingForEvent => {
                    //     // match event_rx.recv() {
                    //     //     Ok(event) => {
                    //     //         curr_event = event;
                    //     //         machine.set_next_state(MachineEvent::ReceiveEvent);
                    //     //     },
                    //     //     Err(e) =>
                    //     //         panic!("Internal watcher channel error: {:?}", e)
                    //     // }
                    //     machine.set_next_state(MachineEvent::ReceiveEvent);
                    // },

                    // // stop() has been called or we've encountered an error whose
                    // // should_stop field is `true`
                    // MachineState::Stopping => {
                    //     // Point of no return -- we'll have to call start() again to
                    //     // begin receiving events.
                    //     //
                    //     // There is no need to close the zookeeper client here, as
                    //     // its drop() method closes it.
                    //     println!("Stopping! Bye bye.");
                    //     let machine_event = MachineEvent::Connect;
                    //     machine.set_next_state(machine_event)
                    // }
                // }
            }).and_then(|_| {
                println!("done");
                Ok(())
            })
            .map(|_| ())
            .map_err(|e| panic!("{:?}", e))
        );
        /*
            loop state:
                - statemachine state
                - zookeeper client
        */
        // let event_rx = event_rx.lock().unwrap();
        // let mut machine = StateMachine::new();
        // let mut zk = None;
        // // This is the current zookeeper event we're processing. To start, we
        // // artificially set this to NodeDataChanged because, upon first running,
        // // the resolver should notify the consumer of any available backends
        // let mut curr_event = ResolverEvent::DataChangedEvent;

        // // This is the main event-processing loop. In each loop iteration, we
        // // examine the current state of the state machine and take action
        // // accordingly.
        // loop {
        //     println!("Top of loop; state: {:?}", machine.get_state());
        //     match machine.get_state() {

        //         // NoSession: Connect to zookeeper
        //         MachineState::NoSession => {
        //             let cs = connect_string.to_string();
        //             println!("Attempting to connect");
        //             // TODO connect
        //             let machine_event = MachineEvent::Connect
        //             machine.set_next_state(machine_event)
        //         }

        //         // HandlingEvent: We are connected to zookeeper but haven't set
        //         // a watch yet. If we're here, it means that we've received an
        //         // event on the channel (except for the first iteration, which
        //         // is why we set curr_event artificially). Thus, we process
        //         // the event and get ready to receive another event. If the
        //         // event is a DataChangedEvent, that means that the watch was
        //         // fired, so we need to re-set the watch. Other event types do
        //         // not come from watches, so we can just transition to the next
        //         // state.
        //         MachineState::HandlingEvent => {
        //             let ten_millis = Duration::from_millis(1000);
        //             thread::sleep(ten_millis);

        //             // The zk client must exist at this point
        //             // TODO account for case where cluster_state_path doesn't
        //             // exist in zk yet -- maybe poll every 30 seconds?
        //             let machine_event;
        //             let zk = zk.as_ref().unwrap();
        //             match curr_event {
        //                 // The cluster topology has changed -- we thus
        //                 // get the new primary and call process_value.
        //                 ResolverEvent::DataChangedEvent => {
        //                     machine_event = || -> MachineEvent {
        //                         let curr_value;
        //                         // TODO get value
        //                         let pool_tx_clone = pool_tx.clone();
        //                         let backend_clone =
        //                             Arc::clone(&last_backend);
        //                         match ManateePrimaryResolver::process_value(
        //                             &pool_tx_clone,
        //                             &curr_value,
        //                             backend_clone) {
        //                             Ok(_) => {
        //                                 return MachineEvent::ArmWatch;
        //                             },
        //                             Err(e) => {
        //                                 ManateePrimaryResolver::set_error(
        //                                     Arc::clone(&error_arcmut),
        //                                     &e
        //                                 );
        //                                 if e.should_stop {
        //                                     return MachineEvent::Stop;
        //                                 } else {
        //                                     return MachineEvent::ArmWatch;
        //                                 }
        //                             }
        //                         }
        //                     }();
        //                 },
        //                 // The state of our session has changed.
        //                 ResolverEvent::SessionEvent => {
        //                     println!("   oooy!");
        //                     machine_event = MachineEvent::ArmWatch;
        //                 },
        //                 // stop() has been called.
        //                 ResolverEvent::StopEvent => {
        //                     machine_event = MachineEvent::Stop;
        //                 }
        //             }
        //             machine.set_next_state(machine_event);
        //         },

        //         // We have nothing to do but wait for something to happen!
        //         MachineState::WaitingForEvent => {
        //             match event_rx.recv() {
        //                 Ok(event) => {
        //                     curr_event = event;
        //                     machine.set_next_state(MachineEvent::ReceiveEvent);
        //                 },
        //                 Err(e) =>
        //                     panic!("Internal watcher channel error: {:?}", e)
        //             }
        //         },

        //         // stop() has been called or we've encountered an error whose
        //         // should_stop field is `true`
        //         MachineState::Stopping => {
        //             // Point of no return -- we'll have to call start() again to
        //             // begin receiving events.
        //             //
        //             // There is no need to close the zookeeper client here, as
        //             // its drop() method closes it.
        //             println!("Stopping! Bye bye.");
        //             break;
        //         }
        //     }
        // }
    }

    /// Parses the given zookeeper node data into a Backend object, compares it
    /// to the last Backend sent to the cueball connection pool, and sends it
    /// to the connection pool if the values differ.
    ///
    /// # Arguments
    ///
    /// * `pool_tx` - The Sender upon which to send the update message
    /// * `new_value` - The raw zookeeper data we've newly retrieved
    /// * `last_backend` - The last Backend we sent to the connection pool
    fn process_value(
        pool_tx: &Sender<BackendMsg>,
        new_value: &(Vec<u8>, Stat),
        last_backend: Arc<Mutex<Option<BackendKey>>>
    ) -> Result<(), ResolverError> {
        let v: Value = serde_json::from_slice(&new_value.0)?;

        // Parse out the ip. We expect the json fields to exist, and return an
        // error if they don't.
        let ip = match &v["primary"]["ip"] {
            Value::String(s) => BackendAddress::from_str(s)?,
            _ => {
                return Err(ResolverError {
                    message: "Malformed zookeeper primary data received for \
                        \"ip\" field".to_string(),
                    should_stop: false
                });
            }
        };

        // Parse out the port. We expect the json fields to exist, and return an
        // error if they don't.
        let port = match &v["primary"]["pgUrl"] {
            Value::String(s) => {
                match Url::parse(s)?.port() {
                    Some(port) => port,
                    None => {
                        return Err(ResolverError {
                            message: "primary's postgres URL contains no port"
                                .to_string(),
                            should_stop: false
                        })
                    }
                }
            },
            _ => {
                return Err(ResolverError {
                    message: "Malformed zookeeper primary data received for \
                        \"pgUrl\" field".to_string(),
                    should_stop: false
                })
            }
        };

        let backend = Backend::new(&ip, port);
        let backend_key = srv_key(&backend);
        let mut last_backend = last_backend.lock().unwrap();

        let should_send = match (*last_backend).clone() {
            Some(lb) => lb != backend_key,
            None => true,
        };

        // Send the new backend if it differs from the old one
        if should_send {
            if let Err(_) = pool_tx.send(BackendMsg::AddedMsg(BackendAddedMsg {
                key: backend_key.clone(),
                backend: backend
            })) {
                return Err(ResolverError {
                    message: "Connection pool shutting down".to_string(),
                    should_stop: true
                });
            }

            let lb_clone = (*last_backend).clone();
            *last_backend = Some(backend_key);

            // Notify the connection pool that the old backend should be
            // removed, if the old backend is not None
            if let Some(lbc) = lb_clone {
                if let Err(_) = pool_tx.send(BackendMsg::RemovedMsg(
                    BackendRemovedMsg(lbc))) {
                    return Err(ResolverError {
                        message: "Connection pool shutting down".to_string(),
                        should_stop: true
                    });
                }
            }
        }
        Ok(())
    }

    fn set_error<T: Debug>(
        error_arcmut: Arc<Mutex<Option<String>>>,
        new_err: &T) {
        let mut error = error_arcmut.lock().unwrap();
        println!("Setting error to: {:?}", new_err);
        *error = Some(format!("{:?}", new_err));
    }
}

impl Resolver for ManateePrimaryResolver {

    fn start(&mut self, s: Sender<BackendMsg>) {
        let mut is_running = self.running.lock().unwrap();
        if *is_running {
            return;
        }

        let s_clone = s.clone();
        let zk_connect_string = self.connect_string.clone();
        let cluster_state_path = self.cluster_state_path.clone();
        let tx_clone = self.event_tx.clone();
        let rx_clone = Arc::clone(&self.event_rx);
        let backend_clone = Arc::clone(&self.last_backend);
        let error_clone = Arc::clone(&self.error);

        // Spawn the background thread that watches for changes
        thread::spawn(move || {
            ManateePrimaryResolver::run(
                zk_connect_string,
                cluster_state_path,
                s_clone,
                tx_clone,
                rx_clone,
                backend_clone,
                error_clone)
        });

        self.pool_tx = Some(s);
        *is_running = true;
    }

    fn stop(&mut self) {
        let mut is_running = self.running.lock().unwrap();
        if !*is_running {
            return;
        }

        // Notify the background thread that it should stop
        if let Err(e) = self.event_tx.send(ResolverEvent::StopEvent) {
            panic!("Error sending stop message over internal channel: {:?}", e);
        }
        *is_running = false;
    }

    fn get_last_error(&self) -> Option<String> {
        if let Some(err) = &*self.error.lock().unwrap() {
                let err_str = format!("{}", err);
                Some(err_str)
        } else {
            None
        }
    }
}


// struct SessionEventWatcher {
//     event_tx: Sender<ResolverEvent>,
// }

// impl Watcher for SessionEventWatcher {
//     fn handle(&self, e: WatchedEvent) {
//         println!("{:?}", e);
//         // Zookeeper session state changes should have WatchedEventType::None,
//         // so we assert this fact
//         match e.event_type {
//             WatchedEventType::None =>
//                 // This is an internal channel that we manage, so we expect
//                 // sending to succeed
//                 self.event_tx.send(ResolverEvent::SessionEvent).unwrap(),
//             _ => panic!("Unexpected event type: {:?}", e.event_type)
//         };
//     }
// }

// struct ClusterStateWatcher {
//     event_tx: Sender<ResolverEvent>,
// }

// impl Watcher for ClusterStateWatcher {
//     fn handle(&self, e: WatchedEvent) {
//         // We only ever register this Watcher for data changes on the cluster
//         // state node, so we assert that the received event is of type
//         // WatchedEventType::NodeDataChanged
//         match e.event_type {
//             WatchedEventType::NodeDataChanged =>
//                 // This is an internal channel that we manage, so we expect
//                 // sending to succeed
//                 self.event_tx.send(ResolverEvent::DataChangedEvent).unwrap(),
//             _ => panic!("Unexpected event type: {:?}", e.event_type)
//         };
//     }
// }


#[cfg(test)]
mod test {
    use super::*;

    use std::iter;

    use quickcheck::{quickcheck, Arbitrary, Gen};

    impl Arbitrary for ZKConnectString {
        fn arbitrary<G: Gen>(g: &mut G) -> Self {
            let size = usize::arbitrary(g);
            ZKConnectString(
                iter::repeat(())
                    .map(|()| SocketAddr::arbitrary(g))
                    .take(size)
                    .collect()
            )
        }
    }

    quickcheck! {
        fn prop_zk_connect_string_parse(
            connect_string: ZKConnectString
        ) -> bool
        {
            // We expect an error only if the input string was zero-length
            match ZKConnectString::from_str(&connect_string.to_string()) {
                Ok(cs) => cs == connect_string,
                _ => connect_string.to_string() == ""
            }
        }
    }

    #[test]
    fn sand_test() {

        // loop {
        //     println!("{:?}", TcpStream::connect("10.77.77.92:2181"));
        //     let ten_millis = Duration::from_millis(1000);
        //     thread::sleep(ten_millis);
        // }

        let conn_str = ZKConnectString::from_str(
            "10.77.77.92:2181").unwrap();
        let path = "/manatee/1.moray.virtual.example.com".to_string();
        let mut resolver = ManateePrimaryResolver::new(conn_str, path);

        let (tx, rx) = channel();
        resolver.start(tx.clone());
        for _ in 0..4 {
            match rx.recv() {
                Ok(event) => {
                    match event {
                        BackendMsg::AddedMsg(msg) => {
                            println!("Added: {:?}", msg.backend);
                        },
                        BackendMsg::RemovedMsg(msg) => {
                            println!("Removed: {:?}", msg.0);
                        },
                        BackendMsg::StopMsg => panic!("Stop message received; how???")
                    }
                }
                Err(e) => {
                    println!("oy! {:?}", e);
                    break
                },
            }
        }
        resolver.stop();

        resolver.start(tx.clone());
        loop {
            match rx.recv() {
                Ok(event) => {
                    match event {
                        BackendMsg::AddedMsg(msg) => {
                            println!("Added: {:?}", msg.backend);
                        },
                        BackendMsg::RemovedMsg(msg) => {
                            println!("Removed: {:?}", msg.0);
                        },
                        BackendMsg::StopMsg => panic!("Stop message received; how???")
                    }
                }
                Err(e) => {
                    println!("oyoy! {:?}", e);
                    break
                },
            }
        }
    }
 }

/*
{ manatee:
   { path: '/manatee/1.moray.virtual.example.com',
     zk:
      { connStr: '10.77.77.91:2181,10.77.77.92:2181,10.77.77.93:2181',
        opts: [Object] },
     log:
      Logger {
        domain: null,
        _events: {},
        _eventsCount: 0,
        _maxListeners: undefined,
        _level: 30,
        streams: [Object],
        serializers: [Object],
        src: false,
        fields: [Object] } },
  pg:
   { connectTimeout: 4000,
     checkInterval: 90000,
     maxConnections: 16,
     maxIdleTime: 270000,
     user: 'moray' } }



/// Cueball backend resolver
///
/// `Resolver`s identify the available backends (*i.e.* nodes) providing a
/// service and relay information about those backends to the connection
/// pool. It should also inform the connection pool when a backend is no longer
/// available so that the pool can rebalance the connections on the remaining
/// backends.
pub trait Resolver: Send + 'static {
    /// Start the operation of the resolver. Begin querying for backends and
    /// notifying the connection pool using the provided `Sender`.
    fn start(&mut self, s: Sender<BackendMsg>);
    /// Shutdown the resolver. Cease querying for new backends. In the event
    /// that attempting to send a message on the `Sender` channel provided in
    /// [`start`]: #method.start fails with an error then this method should be
    /// called as it indicates the connection pool is shutting down.
    fn stop(&mut self);
    /// Return the last error if one has occurred.
    fn get_last_error(&self) -> Option<String>;
}


/// A type representing the different information about a Cueball backend.
#[derive(Clone, Debug)]
pub struct Backend {
    /// The concatenation of the backend address and port with a colon delimiter.
    pub name: BackendName,
    /// The address of the backend.
    pub address: BackendAddress,
    /// The port of the backend.
    pub port: BackendPort,
}

impl Backend {
    /// Return a new instance of `Backend` given a `BackendAddress` and `BackendPort`.
    pub fn new(address: &BackendAddress, port: BackendPort) -> Self {
        Backend {
            name: backend_name(address, port),
            address: *address,
            port,
        }
    }

/// Represents the message that should be sent to the connection pool when a new
/// backend is found.
pub struct BackendAddedMsg {
    /// A backend key
    pub key: backend::BackendKey,
    /// A `Backend` instance
    pub backend: backend::Backend,
}

/// Represents the message that should be sent to the backend when a backend is
/// no longer available.
pub struct BackendRemovedMsg(pub backend::BackendKey);

/// The types of messages that may be sent to the connection pool. `StopMsg` is
/// only for use by the connection pool when performing cleanup prior to
/// shutting down.
pub enum BackendMsg {
    /// Indicates a new backend was found by the resolver
    AddedMsg(BackendAddedMsg),
    /// Indicates a backend is no longer available to service connections
    RemovedMsg(BackendRemovedMsg),
    // For internal pool use only
    #[doc(hidden)]
    StopMsg,
}

/// Returned from the functions used by the connection pool to add or remove
/// backends based on the receipt of `BackedMsg`s by the pool.
pub enum BackendAction {
    /// Indicates a new backend was added by the connection pool.
    BackendAdded,
    /// Indicates an existing backend was removed by the connection pool.
    BackendRemoved,
}

/// A base64 encoded identifier based on the backend name, address, and port.
#[derive(Clone, Debug, Display, Eq, From, Hash, Into, Ord, PartialOrd, PartialEq)]
pub struct BackendKey(String);
/// The port number for a backend. This is a type alias for u16.
pub type BackendPort = u16;
/// The concatenation of the backend address and port with a colon
/// delimiter. This is a type alias for String.
pub type BackendName = String;
/// The IP address of the backend. This is a type alias for std::net::IpAddr.
pub type BackendAddress = IpAddr;





{
  "primary": {
    "id": "10.77.77.95:5432:12345",
    "ip": "10.77.77.95",
    "pgUrl": "tcp://postgres@10.77.77.95:5432/postgres",
    "zoneId": "15b43447-7b9e-4304-ac1a-6d03698d0e0a",
    "backupUrl": "http://10.77.77.95:12345"
  }
}

*/
