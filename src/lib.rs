/*
 * Copyright 2019 Joyent, Inc.
 */

use std::convert::From;
use std::fmt;
use std::fmt::{Debug, Display};
use std::mem;
use std::net::{AddrParseError, SocketAddr};
use std::str::{FromStr};
use std::sync::mpsc::{Sender};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use clap::{crate_name, crate_version};
use futures::future::{ok, loop_fn, Either, Future, Loop, lazy};
use itertools::Itertools;
use serde_json;
use serde_json::Value as SerdeJsonValue;
use serde_json::error::Error as SerdeJsonError;
use slog::{error, info, debug, o, Drain, Key, LevelFilter, Logger, Record, Serializer};
use slog::Result as SlogResult;
use slog::Value as SlogValue;
use tokio_zookeeper::*;
use tokio::prelude::*;
use tokio::runtime::Runtime;
use tokio::timer::Delay;
use url::{Url, ParseError};

use cueball::backend::*;
use cueball::resolver::{
    BackendAddedMsg,
    BackendRemovedMsg,
    BackendMsg,
    Resolver
};

const RECONNECT_DELAY: Duration = Duration::from_secs(10);
const RECONNECT_NODELAY: Duration = Duration::from_secs(0);
const INNER_LOOP_DELAY: Duration = Duration::from_secs(10);
const INNER_LOOP_NODELAY: Duration = Duration::from_secs(0);

// An error type to be used internally.
// The "should_stop" field indicates whether the Resolver should stop() in
// response to the error -- if false, it will instead continue watching for
// cluster state changes.
#[derive(Debug, Clone)]
pub struct ResolverError {
    message: String,
    should_stop: bool
}

impl Display for ResolverError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ResolverError: {:?}, {:?}", self.message, self.should_stop)
    }
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

/// `ZKConnectString` represents a list of zookeeper addresses to connect to.
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

struct LogItem<T>(T) where T: Debug;
impl<T: Debug> SlogValue for LogItem<T> {
    fn serialize(&self, _rec: &Record, key: Key, serializer: &mut dyn Serializer)
        -> SlogResult {
            serializer.emit_str(key, &format!("{:?}", self.0))
    }
}

#[derive(Debug)]
pub struct ManateePrimaryResolver {
    /// The addresses of the Zookeeper cluster the Resolver is connecting to
    connect_string: ZKConnectString,
    /// The Zookeeper path for manatee cluster state for the shard. *e.g.*
    /// "/manatee/1.moray.coal.joyent.us/state"
    cluster_state_path: String,
    /// The key representation of the last backend sent to the cueball
    /// connection pool. Persists across start()/stop() of a given
    /// ManateePrimaryResolver instance.
    last_backend: Arc<Mutex<Option<BackendKey>>>,
    /// The tokio Runtime struct that powers the Resolver. We save a handle so
    /// we can kill the background tasks when calling stop().
    /// The following invariant is maintained: If `runtime` is not None, the
    /// Resolver is running. if `runtime` is None, the resolver is stopped.
    runtime: Arc<Mutex<Option<Runtime>>>,
    /// The ManateePrimaryResolver's root log
    log: Logger
}

impl ManateePrimaryResolver {
    /// Creates a new ManateePrimaryResolver instance.
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
        let cluster_state_path = [&path, "/state"].concat();

        ManateePrimaryResolver {
            connect_string: connect_string.clone(),
            cluster_state_path: cluster_state_path.clone(),
            last_backend: Arc::new(Mutex::new(None)),
            runtime: Arc::new(Mutex::new(None)),
            log: Logger::root(
                Mutex::new(LevelFilter::new(
                    slog_bunyan::with_name(crate_name!(), std::io::stdout()).build(),
                    // config.log.level.into(),
                    // TODO replace with user-configurable (somehow) log level
                    slog::Level::Trace,
                ))
                .fuse(),
                o!("build-id" => crate_version!(),
                    "connect_string" => LogItem(connect_string.to_string()),
                    "path" => cluster_state_path),
            )
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
    /// * `last_backend` - The key representation of the last backend sent to
    ///   the cueball connection pool. It will be updated by process_value() if
    ///   we send a new backend over.
    fn run(
        connect_string: ZKConnectString,
        cluster_state_path: String,
        pool_tx: Sender<BackendMsg>,
        last_backend: Arc<Mutex<Option<BackendKey>>>,
        log: Logger,
    ) {
        // The inner event-processing loop returns this enum to the outer
        // connection managing loop to indicate whether the Resolver should
        // reconnect or stop.
        enum NextAction {
            // The Duration field is the amount of time to wait before
            // reconnecting.
            Reconnect(Duration),
            Stop,
        }

        struct InnerLoopState {
            watcher: Box<dyn futures::stream::Stream
                <Item = WatchedEvent, Error = ()> + Send>,
            curr_event: WatchedEvent,
            delay: Duration
        }

        info!(log, "run(): spawning background event-handling task");
        tokio::spawn(lazy(move ||{
            // Outer loop. Handles connecting to zookeeper. A new loop iteration
            // means a new zookeeper connection. Breaking from the loop means
            // that the client is stopping.
            //
            // Arg: Time to wait before attempting to connect. Initially 0s.
            //     Repeated iterations of the loop set a delay before
            //     connecting.
            // Loop::Break type: ()
            let at_log = log.clone();
            loop_fn(Duration::from_secs(0), move |wait_period| {
                let pool_tx = pool_tx.clone();
                let last_backend = Arc::clone(&last_backend);
                let connect_string = connect_string.clone();
                let cluster_state_path = cluster_state_path.clone();
                let log = log.clone();
                let oe_log = log.clone();
                let run_connect = move |_| {
                    info!(log, "Connecting to ZooKeeper cluster");
                    ZooKeeper::connect(
                        &connect_string.to_string().parse().unwrap())
                    .and_then(move |(zk, default_watcher)| {
                        // State: Connected
                        info!(log, "Connected to ZooKeeper cluster");

                        // Main change-watching loop. A new loop iteration means
                        // we're setting a new watch (if necessary) and waiting
                        // for a result. Breaking from the loop means that we've
                        // hit some error and are returning control to the outer
                        // loop.
                        //
                        // This loop can return from two states: before we've
                        // waited for the watch to fire (if we hit an error
                        // before waiting), or after we've waited for the watch
                        // to fire (this could be a success or an error). These
                        // two states require returning different Future types,
                        // so we wrap the returned values in a future::Either to
                        // satisfy the type checker.
                        //
                        // Arg: (watcher Stream object, event we're
                        //     currently processing) -- we set curr_event to
                        //     an artificially constructed WatchedEvent for the
                        //     first loop iteration, so the connection pool will
                        //     be initialized with the initial primary as its
                        //     backend.
                        // Loop::Break type: NextAction -- this value is used to
                        //     instruct the outer loop whether to try to
                        //     reconnect or terminate.
                        loop_fn(InnerLoopState {
                            watcher: Box::new(default_watcher),
                            curr_event: WatchedEvent {
                                event_type: WatchedEventType::NodeDataChanged,
                                // This initial artificial keeper_state doesn't
                                // necessarily reflect reality, but that's ok
                                // because it's paired with an artificial
                                // NodeDataChanged event, and our handling for
                                // this type of event doesn't involve the
                                // keeper_state field.
                                keeper_state: KeeperState::SyncConnected,
                                // We never use path, so we might as well set it
                                // to an empty string in our artificially
                                // constructed bootstrap WatchedEvent object.
                                path: "".to_string(),
                            },
                            delay: INNER_LOOP_NODELAY
                        } , move |loop_state| {
                            let watcher = loop_state.watcher;
                            let curr_event = loop_state.curr_event;
                            let delay = loop_state.delay;
                            let when = Instant::now() + delay;
                            let pool_tx = pool_tx.clone();
                            let last_backend = Arc::clone(&last_backend);
                            let cluster_state_path = cluster_state_path.clone();
                            let zk = zk.clone();
                            // TODO avoid mutex boilerplate from showing up in the log
                            let log = log.new(o!(
                                "curr_event" => LogItem(curr_event.clone()),
                                "delay" => LogItem(delay.clone()),
                                "last_backend" => LogItem(Arc::clone(&last_backend))
                            ));

                            info!(log, "Getting data");
                            let oe_log = log.clone();
                            // State: Watch unarmed

                            // We set the watch here. If the previous iteration
                            // of the loop ended because the keeper state
                            // changed rather than because the watch fired, the
                            // watch will already have been set, so we don't
                            // _need_ to set it here. With that said, it does
                            // no harm (zookeeper deduplicates watches on the
                            // server side), and it may not be worth the effort
                            // to optimize for this case, since keeper state
                            // changes (and, indeed, changes of any sort) should
                            // happen infrequently.
                            Delay::new(when)
                            .and_then(move |_| {
                                zk
                                .watch()
                                .get_data(&cluster_state_path)
                                .and_then(move |(_, data)| {
                                    match curr_event.event_type {
                                        // Keeper state has changed
                                        WatchedEventType::None => {
                                            match curr_event.keeper_state {
                                                // TODO will these cases ever happen?
                                                // because if the keeper state is "bad",
                                                // then the get_data will have failed
                                                // and we won't be here.
                                                KeeperState::Disconnected |
                                                KeeperState::AuthFailed |
                                                KeeperState::Expired => {
                                                    error!(log, "Keeper state changed; reconnecting";
                                                        "keeper_state" =>
                                                        LogItem(curr_event.keeper_state));
                                                    return Either::A(
                                                        ok(Loop::Break(
                                                        NextAction::Reconnect(
                                                        RECONNECT_NODELAY))));
                                                },
                                                KeeperState::SyncConnected |
                                                KeeperState::ConnectedReadOnly |
                                                KeeperState::SaslAuthenticated => {
                                                    info!(log, "Keeper state changed";
                                                        "keeper_state" =>
                                                        LogItem(curr_event.keeper_state));
                                                }
                                            }
                                        },
                                        // The data watch fired
                                        WatchedEventType::NodeDataChanged => {
                                            // We didn't  get the data, which
                                            // means the node doesn't exist yet.
                                            // We should wait a bit and try
                                            // again. We'll just use the same
                                            // event as before.
                                            if data.is_none() {
                                                info!(log, "ZK data does not \
                                                    exist yet");
                                                return Either::A(ok(Loop::Continue(
                                                    InnerLoopState {
                                                        watcher,
                                                        curr_event,
                                                        delay: INNER_LOOP_DELAY
                                                    })));
                                            }
                                            info!(log, "got data"; "data" =>
                                                LogItem(data.clone()));
                                            match ManateePrimaryResolver::process_value(
                                                &pool_tx.clone(),
                                                &data.unwrap(),
                                                Arc::clone(&last_backend),
                                                log.clone()) {
                                                Ok(_) => {},
                                                Err(e) => {
                                                    error!(log, ""; "error" => LogItem(e.clone()));
                                                    // The error is between the
                                                    // client and the
                                                    // outward-facing channel,
                                                    // not between the client
                                                    // and the zookeeper
                                                    // connection, so we don't
                                                    // have to attempt to
                                                    // reconnect here and can
                                                    // continue, unless the
                                                    // error tells us to stop.
                                                    if e.should_stop {
                                                        return Either::A(ok(
                                                            Loop::Break(
                                                            NextAction::Stop)));
                                                    }
                                                }
                                            }
                                        },
                                        e => panic!(
                                            "Unexpected event received: {:?}",
                                            e)
                                    };

                                    // If we got here, we're waiting for the watch
                                    // to fire. Before this point, we wrap the
                                    // return value in Either::A. After this point,
                                    // we wrap the return value in Either::B.
                                    info!(log, "Watching for change");
                                    let oe_log = log.clone();
                                    Either::B(watcher
                                        .into_future()
                                        .and_then(move |(event, watcher)| {
                                            let loop_next = match event {
                                                Some(e) => {
                                                    info!(log, "change event received; \
                                                        looping to process event";
                                                        "event" => LogItem(e.clone()));
                                                    Loop::Continue(InnerLoopState {
                                                        watcher,
                                                        curr_event: e,
                                                        delay: INNER_LOOP_NODELAY
                                                    })
                                                },
                                                // If we didn't get a valid
                                                // event, this means the Stream
                                                // got closed, which indicates
                                                // a connection issue -- so we
                                                // reconnect.
                                                None => {
                                                    error!(log, "Event stream closed; \
                                                        reconnecting");
                                                    Loop::Break(
                                                        NextAction::Reconnect(
                                                        RECONNECT_NODELAY))
                                                }
                                            };
                                            ok(loop_next)
                                        })
                                        .or_else(move |_| {
                                            // If we get an error from the event
                                            // Stream, we assume that something
                                            // went wrong with the zookeeper
                                            // connection and attempt to
                                            // reconnect.
                                            //
                                            // The stream's error type is (), so
                                            // there's no information to extract
                                            // from it.
                                            error!(oe_log, "Error received from event stream");
                                            ok(Loop::Break(
                                                NextAction::Reconnect(
                                                RECONNECT_NODELAY)))
                                        }))
                                })
                                // If some error occurred getting the data,
                                // we assume we should reconnect to the
                                // zookeeper server.
                                .or_else(move |error| {
                                    error!(oe_log, "Error getting data";
                                        "error" => LogItem(error));
                                    ok(Loop::Break(NextAction::Reconnect(
                                        RECONNECT_NODELAY)))
                                })
                            })
                            .map_err(|e| panic!("delay errored; err: {:?}", e))
                        })
                        .and_then(|next_action| {
                            ok(match next_action {
                                NextAction::Stop => Loop::Break(()),
                                // We reconnect immediately here instead of
                                // waiting, because if we're here it means that
                                // we came from the inner loop and thus we just
                                // had a valid connection terminate (as opposed
                                // to the `or_else` block below, were we've just
                                // tried to connect and failed), and thus
                                // there's no reason for us to delay trying to
                                // connect again.
                                NextAction::Reconnect(delay) => Loop::Continue(
                                    delay)
                            })
                        })
                    })
                    .or_else(move |error| {
                        error!(oe_log, "Error connecting to ZooKeeper cluster";
                            "error" => LogItem(error));
                        ok(Loop::Continue(RECONNECT_DELAY))
                    })
                };

                let when = Instant::now() + wait_period;
                Delay::new(when)
                .and_then(run_connect)
                .map_err(|e| panic!("delay errored; err: {:?}", e))
            }).and_then(move |_| {
                info!(at_log, "Event-processing task stopping");
                Ok(())
            })
            .map(|_| ())
        }));
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
        last_backend: Arc<Mutex<Option<BackendKey>>>,
        log: Logger
    ) -> Result<(), ResolverError> {
        debug!(log, "process_value() entered");

        let v: SerdeJsonValue = serde_json::from_slice(&new_value.0)?;

        // Parse out the ip. We expect the json fields to exist, and return an
        // error if they don't.
        let ip = match &v["primary"]["ip"] {
            SerdeJsonValue::String(s) => BackendAddress::from_str(s)?,
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
            SerdeJsonValue::String(s) => {
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
            info!(log, "New backend found; sending to connection pool";
                "backend" => LogItem(backend.clone()));
            if pool_tx.send(BackendMsg::AddedMsg(BackendAddedMsg {
                key: backend_key.clone(),
                backend
            })).is_err() {
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
                info!(log, "Notifying connection pool of removal of old backend");
                if pool_tx.send(BackendMsg::RemovedMsg(
                    BackendRemovedMsg(lbc))).is_err() {
                    return Err(ResolverError {
                        message: "Connection pool shutting down".to_string(),
                        should_stop: true
                    });
                }
            }
        } else {
            info!(log, "New backend value does not differ; not sending");
        }
        debug!(log, "process_value() returned successfully");
        Ok(())
    }
}

impl Resolver for ManateePrimaryResolver {

    fn start(&mut self, s: Sender<BackendMsg>) {
        debug!(self.log, "start() method entered");

        // If self.runtime is not None, the Resolver is running, so we can
        // return here without doing anything.
        let mut runtime = self.runtime.lock().unwrap();
        if (*runtime).is_some() {
            info!(self.log,
                "Resolver already running; start() method doing nothing");
            return;
        }

        let s = s.clone();
        let connect_string = self.connect_string.clone();
        let cluster_state_path = self.cluster_state_path.clone();
        let last_backend = Arc::clone(&self.last_backend);
        let task_log = self.log.clone();

        let mut rt = Runtime::new().unwrap();

        info!(self.log, "start(): starting runtime");
        // Start the background event-processing task
        rt.spawn(lazy(|| {
            ManateePrimaryResolver::run(
                connect_string,
                cluster_state_path,
                s,
                last_backend,
                task_log);
            Ok(())
        }));

        // Save the required persistent state
        *runtime = Some(rt);
        debug!(self.log, "start() returned successfully");
    }

    fn stop(&mut self) {
        debug!(self.log, "stop() method entered");
        let mut runtime = self.runtime.lock().unwrap();
        // If self.runtime is None, the Resolver is stopped, so we can return
        // here without doing anyting.
        if (*runtime).is_none() {
            info!(self.log,
                "Resolver not running; stop() method doing nothing");
            return;
        }

        // Fun with the borrow checker:
        //
        // Calling shutdown_now() moves the Runtime struct out of the mutex.
        // That's not allowed because Runtime doesn't implement Copy, and we
        // can't clone the Runtime because Runtime doesn't implement Clone. It's
        // actually be totally safe to move the Runtime out of the mutex here,
        // because we intend to write a new value to the mutex later in this
        // function, but the compiler doesn't know that. What we _can_ do is
        // use mem::replace to move the Runtime and write a new value in one
        // motion, so the mutex always has a valid value. Believe it or not,
        // this is all safe. Very cool, I think!
        let local_runtime = mem::replace(&mut *runtime, None);
        // Furthermore, if we got here, local_runtime isn't None, because
        // *runtime wasn't None, so we can unwrap local_runtime.
        //
        // We can also unwrap shutdown_now().wait(), because, as I understand
        // it, shutdown_now cannot return an error. Indeed, the error type is
        // ().
        info!(self.log, "Stopping runtime");
        local_runtime.unwrap().shutdown_now().wait().unwrap();
        debug!(self.log, "stop() returned successfully");
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use std::iter;
    use std::sync::mpsc::channel;

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
    fn sandbox_test() {

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
                        BackendMsg::StopMsg =>
                            panic!("Stop message received; how???")
                    }
                }
                Err(e) => {
                    println!("oy! {:?}", e);
                    break
                },
            }
        }
        println!("stopping");
        resolver.stop();
        println!("starting");
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
                        BackendMsg::StopMsg =>
                            panic!("Stop message received; how???")
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
