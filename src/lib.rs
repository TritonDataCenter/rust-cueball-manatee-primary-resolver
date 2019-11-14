/*
 * Copyright 2019 Joyent, Inc.
 */

use std::cmp::PartialEq;
use std::convert::From;
use std::fmt;
use std::fmt::{Debug, Display};
use std::net::{AddrParseError, SocketAddr};
use std::str::{FromStr};
use std::sync::mpsc::{Sender, TryRecvError};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use clap::{crate_name, crate_version};
use futures::future::{ok, loop_fn, Either, Future, Loop};
use itertools::Itertools;
use serde_json;
use serde_json::Value as SerdeJsonValue;
use serde_json::error::Error as SerdeJsonError;
use slog::{error, info, debug, o, Drain, Key, LevelFilter, Logger, Record,
    Serializer};
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

pub mod util;

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);

const RECONNECT_DELAY: Duration = Duration::from_secs(10);
const RECONNECT_NODELAY: Duration = Duration::from_secs(0);
const INNER_LOOP_DELAY: Duration = Duration::from_secs(10);
const INNER_LOOP_NODELAY: Duration = Duration::from_secs(0);

// An error type to be used internally.
#[derive(Clone, Debug, PartialEq)]
enum ResolverError {
    InvalidZkJson,
    InvalidZkData(ZkDataField),
    MissingZkData(ZkDataField),
    ConnectionPoolShutdown
}

impl ResolverError {
    fn should_stop(&self) -> bool {
        match self {
            ResolverError::ConnectionPoolShutdown => true,
            _ => false
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
enum ZkDataField {
    Ip,
    Port,
    PostgresUrl
}

/// `ZkConnectString` represents a list of zookeeper addresses to connect to.
#[derive(Debug, Clone, PartialEq)]
pub struct ZkConnectString(Vec<SocketAddr>);

impl ToString for ZkConnectString {
    fn to_string(&self) -> String {
        self
            .0
            .iter()
            .map(|x| x.to_string())
            .intersperse(String::from(","))
            .collect()
    }
}

impl FromStr for ZkConnectString {
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
            .and_then(|x| Ok(ZkConnectString(x)))
    }
}

struct LogItem<T>(T) where T: Debug;
impl<T: Debug> SlogValue for LogItem<T> {
    fn serialize(&self, _rec: &Record, key: Key,
        serializer: &mut dyn Serializer) -> SlogResult {
            serializer.emit_str(key, &format!("{:?}", self.0))
    }
}

#[derive(Debug)]
pub struct ManateePrimaryResolver {
    /// The addresses of the Zookeeper cluster the Resolver is connecting to
    connect_string: ZkConnectString,
    /// The Zookeeper path for manatee cluster state for the shard. *e.g.*
    /// "/manatee/1.moray.coal.joyent.us/state"
    cluster_state_path: String,
    /// The key representation of the last backend sent to the cueball
    /// connection pool. Persists across multiple calls to run().
    last_backend: Arc<Mutex<Option<BackendKey>>>,
    /// Indicates whether or not the resolver is running. This is slightly
    /// superfluous (this field is `true` for exactly the duration of each
    /// call to run(), and false otherwise), but could be useful if the caller
    /// wants to check if the resolver is running for some reason.
    is_running: bool,
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
        connect_string: ZkConnectString,
        path: String
    ) -> Self
    {
        let cluster_state_path = [&path, "/state"].concat();

        ManateePrimaryResolver {
            connect_string: connect_string.clone(),
            cluster_state_path: cluster_state_path.clone(),
            last_backend: Arc::new(Mutex::new(None)),
            is_running: false,
            log: Logger::root(
                Mutex::new(LevelFilter::new(
                    slog_bunyan::with_name(crate_name!(),
                        std::io::stdout()).build(),
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
        new_value: &Vec<u8>,
        last_backend: Arc<Mutex<Option<BackendKey>>>,
        log: Logger
    ) -> Result<(), ResolverError> {
        debug!(log, "process_value() entered");

        let v: SerdeJsonValue = match serde_json::from_slice(&new_value) {
            Ok(v) => v,
            Err(_) => {
                return Err(ResolverError::InvalidZkJson);
            }
        };

        // Parse out the ip. We expect the json fields to exist, and return an
        // error if they don't, or if they are of the wrong type.
        let ip = match &v["primary"]["ip"] {
            SerdeJsonValue::String(s) => {
                match BackendAddress::from_str(s) {
                    Ok(s) => s,
                    Err(_) => {
                        return Err(ResolverError::InvalidZkData(ZkDataField::Ip));
                    }
                }
            },
            SerdeJsonValue::Null => {
                return Err(ResolverError::MissingZkData(ZkDataField::Ip));
            },
            _ => {
                return Err(ResolverError::InvalidZkData(ZkDataField::Ip));
            }
        };

        // Parse out the port. We expect the json fields to exist, and return an
        // error if they don't, or if they are of the wrong type.
        let port = match &v["primary"]["pgUrl"] {
            SerdeJsonValue::String(s) => {
                match Url::parse(s) {
                    Ok(url) => {
                        match url.port() {
                            Some(port) => port,
                            None => {
                                return Err(ResolverError::MissingZkData(ZkDataField::Port));
                            }
                        }
                    },
                    Err(_) => {
                        return Err(ResolverError::InvalidZkData(ZkDataField::PostgresUrl))
                    }
                }
            },
            SerdeJsonValue::Null => {
                return Err(ResolverError::MissingZkData(ZkDataField::PostgresUrl));
            },
            _ => {
                return Err(ResolverError::InvalidZkData(ZkDataField::PostgresUrl));
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
                return Err(ResolverError::ConnectionPoolShutdown);
            }

            let lb_clone = (*last_backend).clone();
            *last_backend = Some(backend_key);

            // Notify the connection pool that the old backend should be
            // removed, if the old backend is not None
            if let Some(lbc) = lb_clone {
                info!(log,
                    "Notifying connection pool of removal of old backend");
                if pool_tx.send(BackendMsg::RemovedMsg(
                    BackendRemovedMsg(lbc))).is_err() {
                    return Err(ResolverError::ConnectionPoolShutdown);
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

    // The resolver object is not Sync, so we can assume that only one instance
    // of this function is running at once, because callers will have to control
    // concurrent access.
    //
    // If the connection pool closes the receiving end of the channel, this
    // function may not return right away -- this function will not notice that
    // the pool has disconnected until this function tries to send another
    // heartbeat, at which point this function will return. This means that the
    // time between disconnection and function return is at most the length of
    // HEARTBEAT_INTERVAL. Any change in the meantime will be picked up by the
    // next call to run()
    fn run(&mut self, s: Sender<BackendMsg>) {

        // Represents the action to be taken in the event of a connection error.
        enum NextAction {
            // The Duration field is the amount of time to wait before
            // reconnecting.
            Reconnect(Duration),
            Stop,
        }

        // Encapsulates the state that one iteration of the watch loop passes
        // to the next iteration.
        struct InnerLoopState {
            watcher: Box<dyn futures::stream::Stream
                <Item = WatchedEvent, Error = ()> + Send>,
            curr_event: WatchedEvent,
            delay: Duration
        }

        debug!(self.log, "run() method entered");
        let mut rt = Runtime::new().unwrap();
        // There's no need to check if the pool is already running and return
        // early, because multiple instances of this function _cannot_ be
        // running concurrently -- see this function's header comment.
        self.is_running = true;

        // Variables moved to tokio runtime thread:
        //
        // * `connect_string` - A comma-separated list of the zookeeper
        //   instances in the cluster
        // * `cluster_state_path` - The path to the cluster state node in
        //   zookeeper for the shard we're watching
        // * `pool_tx` - The Sender that this function should use to communicate
        //   with the cueball connection pool
        // * `last_backend` - The key representation of the last backend sent to
        //   the cueball connection pool. It will be updated by process_value()
        //   if we send a new backend over.
        // * log - A clone of the resolver's master log
        // * at_log - Another clone, used in the `and_then` portion of the loop
        let connect_string = self.connect_string.clone();
        let cluster_state_path = self.cluster_state_path.clone();
        let pool_tx = s.clone();
        let last_backend = Arc::clone(&self.last_backend);
        let log = self.log.clone();
        let at_log = self.log.clone();

        // Start the event-processing task
        info!(self.log, "run(): starting runtime");
        rt.spawn(
            // Outer loop. Handles connecting to zookeeper. A new loop iteration
            // means a new zookeeper connection. Breaking from the loop means
            // that the client is stopping.
            //
            // Arg: Time to wait before attempting to connect. Initially 0s.
            //     Repeated iterations of the loop set a delay before
            //     connecting.
            // Loop::Break type: ()
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
                                            // We didn't get the data, which
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
                                            // Discard the Stat, as we don't use
                                            // it.
                                            let data = data.unwrap().0;
                                            info!(log, "got data"; "data" =>
                                                LogItem(data.clone()));
                                            match ManateePrimaryResolver::process_value(
                                                &pool_tx.clone(),
                                                &data,
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
                                                    if e.should_stop() {
                                                        return Either::A(ok(
                                                            Loop::Break(
                                                            NextAction::Stop)));
                                                    }
                                                }
                                            }
                                        },
                                        WatchedEventType::NodeDeleted => {
                                            // Same behavior as the above case
                                            // where we didn't get the data
                                            // because the node doesn't exist.
                                            // See comment above.
                                            info!(log, "ZK node deleted");
                                            return Either::A(ok(Loop::Continue(
                                                InnerLoopState {
                                                    watcher,
                                                    curr_event,
                                                    delay: INNER_LOOP_DELAY
                                                })));
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
        );
        loop {
            if s.send(BackendMsg::HeartbeatMsg).is_err() {
                info!(self.log, "Connection pool channel closed");
                break;
            }
            thread::sleep(HEARTBEAT_INTERVAL);
        }
        info!(self.log, "Stopping runtime");
        // We shut down the background watch-looping thread. It may have already
        // exited by itself if it noticed that the connection pool closed its
        // channel, but there's no harm still calling shutdown_now() in that
        // case.
        rt.shutdown_now().wait().unwrap();
        info!(self.log, "Runtime stopped successfully");
        self.is_running = false;
        debug!(self.log, "run() returned successfully");
    }
}

// Unit tests
#[cfg(test)]
mod test {
    use super::*;

    use std::iter;
    use std::sync::{Arc, Mutex};
    use std::sync::mpsc::channel;
    use std::vec::Vec;

    use quickcheck::{quickcheck, Arbitrary, Gen};
    use tokio_zookeeper::{Acl, CreateMode};
    use uuid::Uuid;

    use super::util;

    impl Arbitrary for ZkConnectString {
        fn arbitrary<G: Gen>(g: &mut G) -> Self {
            let size = usize::arbitrary(g);
            ZkConnectString(
                iter::repeat(())
                    .map(|()| SocketAddr::arbitrary(g))
                    .take(size)
                    .collect()
            )
        }
    }

    // Test parsing ZkConnectString from string
    quickcheck! {
        fn prop_zk_connect_string_parse(
            connect_string: ZkConnectString
        ) -> bool
        {
            // We expect an error only if the input string was zero-length
            match ZkConnectString::from_str(&connect_string.to_string()) {
                Ok(cs) => cs == connect_string,
                _ => connect_string.to_string() == ""
            }
        }
    }

    fn test_log() -> Logger {
        Logger::root(
            Mutex::new(LevelFilter::new(
                slog_bunyan::with_name(crate_name!(),
                    std::io::stdout()).build(),
                slog::Level::Critical)).fuse(),
                o!("build-id" => crate_version!()))
    }

    #[derive(Clone)]
    struct BackendData {
        raw: Vec<u8>,
        object: Backend
    }

    impl BackendData {
        // Most of the data here isn't relevant, but real json from zookeeper
        // will include it.
        fn new(ip: &str, port: u16) -> Self {
            let raw = format!(r#" {{
                "generation": 1,
                "primary": {{
                    "id": "{ip}:{port}:12345",
                    "ip": "{ip}",
                    "pgUrl": "tcp://postgres@{ip}:{port}/postgres",
                    "zoneId": "f47c4766-1857-4bdc-97f0-c1fd009c955b",
                    "backupUrl": "http://{ip}:12345"
                }},
                "sync": {{
                    "id": "10.77.77.21:5432:12345",
                    "zoneId": "f8727df9-c639-4152-a861-c77a878ca387",
                    "ip": "10.77.77.21",
                    "pgUrl": "tcp://postgres@10.77.77.21:5432/postgres",
                    "backupUrl": "http://10.77.77.21:12345"
                }},
                "async": [],
                "deposed": [
                    {{
                        "id":"10.77.77.22:5432:12345",
                        "ip": "10.77.77.22",
                        "pgUrl": "tcp://postgres@10.77.77.22:5432/postgres",
                        "zoneId": "c7a64f9f-4d49-4e6b-831a-68fd6ebf1d3c",
                        "backupUrl": "http://10.77.77.22:12345"
                    }}
                ],
                "initWal": "0/16522D8"
            }}"#, ip = ip, port = port).as_bytes().to_vec();

            BackendData {
                raw,
                object: Backend::new(&BackendAddress::from_str(ip).unwrap(), port)
            }
        }

        fn raw(&self) -> Vec<u8> {
            self.raw.clone()
        }

        fn key(&self) -> BackendKey {
            srv_key(&self.object)
        }

        fn added_msg(&self) -> BackendAddedMsg {
            BackendAddedMsg {
                key: self.key(),
                backend: self.object.clone()
            }
        }

        fn removed_msg(&self) -> BackendRemovedMsg {
            BackendRemovedMsg(self.key())
        }
    }

    fn backend_ip1_port1() -> BackendData {
        BackendData::new("10.77.77.28", 5432)
    }

    fn backend_ip1_port2() -> BackendData {
        BackendData::new("10.77.77.28", 5431)
    }

    fn backend_ip2_port1() -> BackendData {
        BackendData::new("10.77.77.21", 5432)
    }

    fn backend_ip2_port2() -> BackendData {
        BackendData::new("10.77.77.21", 5431)
    }

    fn raw_invalid_json() -> Vec<u8> {
        "foo".as_bytes().to_vec()
    }

    fn raw_no_ip() -> Vec<u8> {
        r#" {
                "generation": 1,
                "primary": {
                    "id": "10.77.77.21:5432:12345",
                    "zoneId": "f8727df9-c639-4152-a861-c77a878ca387",
                    "pgUrl": "tcp://postgres@10.77.77.21:5432/postgres",
                    "backupUrl": "http://10.77.77.21:12345"
                },
                "sync": {
                    "id": "10.77.77.28:5432:12345",
                    "ip": "10.77.77.28",
                    "pgUrl": "tcp://postgres@10.77.77.28:5432/postgres",
                    "zoneId": "f47c4766-1857-4bdc-97f0-c1fd009c955b",
                    "backupUrl": "http://10.77.77.28:12345"
                },
                "async": [],
                "deposed": [
                    {
                        "id":"10.77.77.22:5432:12345",
                        "ip": "10.77.77.22",
                        "pgUrl": "tcp://postgres@10.77.77.22:5432/postgres",
                        "zoneId": "c7a64f9f-4d49-4e6b-831a-68fd6ebf1d3c",
                        "backupUrl": "http://10.77.77.22:12345"
                    }
                ],
                "initWal": "0/16522D8"
            }
        "#.as_bytes().to_vec()
    }

    fn raw_invalid_ip() -> Vec<u8> {
        r#" {
                "generation": 1,
                "primary": {
                    "id": "10.77.77.21:5432:12345",
                    "zoneId": "f8727df9-c639-4152-a861-c77a878ca387",
                    "ip": "foo",
                    "pgUrl": "tcp://postgres@10.77.77.21:5432/postgres",
                    "backupUrl": "http://10.77.77.21:12345"
                },
                "sync": {
                    "id": "10.77.77.28:5432:12345",
                    "ip": "10.77.77.28",
                    "pgUrl": "tcp://postgres@10.77.77.28:5432/postgres",
                    "zoneId": "f47c4766-1857-4bdc-97f0-c1fd009c955b",
                    "backupUrl": "http://10.77.77.28:12345"
                },
                "async": [],
                "deposed": [
                    {
                        "id":"10.77.77.22:5432:12345",
                        "ip": "10.77.77.22",
                        "pgUrl": "tcp://postgres@10.77.77.22:5432/postgres",
                        "zoneId": "c7a64f9f-4d49-4e6b-831a-68fd6ebf1d3c",
                        "backupUrl": "http://10.77.77.22:12345"
                    }
                ],
                "initWal": "0/16522D8"
            }
        "#.as_bytes().to_vec()
    }

    fn raw_wrong_type_ip() -> Vec<u8> {
        r#" {
                "generation": 1,
                "primary": {
                    "id": "10.77.77.21:5432:12345",
                    "zoneId": "f8727df9-c639-4152-a861-c77a878ca387",
                    "ip": true,
                    "pgUrl": "tcp://postgres@10.77.77.21:5432/postgres",
                    "backupUrl": "http://10.77.77.21:12345"
                },
                "sync": {
                    "id": "10.77.77.28:5432:12345",
                    "ip": "10.77.77.28",
                    "pgUrl": "tcp://postgres@10.77.77.28:5432/postgres",
                    "zoneId": "f47c4766-1857-4bdc-97f0-c1fd009c955b",
                    "backupUrl": "http://10.77.77.28:12345"
                },
                "async": [],
                "deposed": [
                    {
                        "id":"10.77.77.22:5432:12345",
                        "ip": "10.77.77.22",
                        "pgUrl": "tcp://postgres@10.77.77.22:5432/postgres",
                        "zoneId": "c7a64f9f-4d49-4e6b-831a-68fd6ebf1d3c",
                        "backupUrl": "http://10.77.77.22:12345"
                    }
                ],
                "initWal": "0/16522D8"
            }
        "#.as_bytes().to_vec()
    }

    fn raw_no_pg_url() -> Vec<u8> {
        r#" {
                "generation": 1,
                "primary": {
                    "id": "10.77.77.28:5432:12345",
                    "ip": "10.77.77.28",
                    "zoneId": "f47c4766-1857-4bdc-97f0-c1fd009c955b",
                    "backupUrl": "http://10.77.77.28:12345"
                },
                "sync": {
                    "id": "10.77.77.21:5432:12345",
                    "zoneId": "f8727df9-c639-4152-a861-c77a878ca387",
                    "ip": "10.77.77.21",
                    "pgUrl": "tcp://postgres@10.77.77.21:5432/postgres",
                    "backupUrl": "http://10.77.77.21:12345"
                },
                "async": [],
                "deposed": [
                    {
                        "id":"10.77.77.22:5432:12345",
                        "ip": "10.77.77.22",
                        "pgUrl": "tcp://postgres@10.77.77.22:5432/postgres",
                        "zoneId": "c7a64f9f-4d49-4e6b-831a-68fd6ebf1d3c",
                        "backupUrl": "http://10.77.77.22:12345"
                    }
                ],
                "initWal": "0/16522D8"
            }
        "#.as_bytes().to_vec()
    }

    fn raw_invalid_pg_url() -> Vec<u8> {
        r#" {
                "generation": 1,
                "primary": {
                    "id": "10.77.77.28:5432:12345",
                    "ip": "10.77.77.28",
                    "pgUrl": "foo",
                    "zoneId": "f47c4766-1857-4bdc-97f0-c1fd009c955b",
                    "backupUrl": "http://10.77.77.28:12345"
                },
                "sync": {
                    "id": "10.77.77.21:5432:12345",
                    "zoneId": "f8727df9-c639-4152-a861-c77a878ca387",
                    "ip": "10.77.77.21",
                    "pgUrl": "tcp://postgres@10.77.77.21:5432/postgres",
                    "backupUrl": "http://10.77.77.21:12345"
                },
                "async": [],
                "deposed": [
                    {
                        "id":"10.77.77.22:5432:12345",
                        "ip": "10.77.77.22",
                        "pgUrl": "tcp://postgres@10.77.77.22:5432/postgres",
                        "zoneId": "c7a64f9f-4d49-4e6b-831a-68fd6ebf1d3c",
                        "backupUrl": "http://10.77.77.22:12345"
                    }
                ],
                "initWal": "0/16522D8"
            }
        "#.as_bytes().to_vec()
    }

    fn raw_wrong_type_pg_url() -> Vec<u8> {
        r#" {
                "generation": 1,
                "primary": {
                    "id": "10.77.77.28:5432:12345",
                    "ip": "10.77.77.28",
                    "pgUrl": true,
                    "zoneId": "f47c4766-1857-4bdc-97f0-c1fd009c955b",
                    "backupUrl": "http://10.77.77.28:12345"
                },
                "sync": {
                    "id": "10.77.77.21:5432:12345",
                    "zoneId": "f8727df9-c639-4152-a861-c77a878ca387",
                    "ip": "10.77.77.21",
                    "pgUrl": "tcp://postgres@10.77.77.21:5432/postgres",
                    "backupUrl": "http://10.77.77.21:12345"
                },
                "async": [],
                "deposed": [
                    {
                        "id":"10.77.77.22:5432:12345",
                        "ip": "10.77.77.22",
                        "pgUrl": "tcp://postgres@10.77.77.22:5432/postgres",
                        "zoneId": "c7a64f9f-4d49-4e6b-831a-68fd6ebf1d3c",
                        "backupUrl": "http://10.77.77.22:12345"
                    }
                ],
                "initWal": "0/16522D8"
            }
        "#.as_bytes().to_vec()
    }

    fn raw_no_port_pg_url() -> Vec<u8> {
        r#" {
                "generation": 1,
                "primary": {
                    "id": "10.77.77.28:5432:12345",
                    "ip": "10.77.77.28",
                    "pgUrl": "tcp://postgres@10.77.77.22/postgres",
                    "zoneId": "f47c4766-1857-4bdc-97f0-c1fd009c955b",
                    "backupUrl": "http://10.77.77.28:12345"
                },
                "sync": {
                    "id": "10.77.77.21:5432:12345",
                    "zoneId": "f8727df9-c639-4152-a861-c77a878ca387",
                    "ip": "10.77.77.21",
                    "pgUrl": "tcp://postgres@10.77.77.21:5432/postgres",
                    "backupUrl": "http://10.77.77.21:12345"
                },
                "async": [],
                "deposed": [
                    {
                        "id":"10.77.77.22:5432:12345",
                        "ip": "10.77.77.22",
                        "pgUrl": "tcp://postgres@10.77.77.22:5432/postgres",
                        "zoneId": "c7a64f9f-4d49-4e6b-831a-68fd6ebf1d3c",
                        "backupUrl": "http://10.77.77.22:12345"
                    }
                ],
                "initWal": "0/16522D8"
            }
        "#.as_bytes().to_vec()
    }

    // Represents a process_value test case, including inputs and expected
    // outputs.
    struct ProcessValueFields {
        value: Vec<u8>,
        last_backend: BackendKey,
        expected_error: Option<ResolverError>,
        message_count: u32,
        added_backend: Option<BackendAddedMsg>,
        removed_backend: Option<BackendRemovedMsg>
    }

    // Run a process_value test case
    fn run_process_value_fields(input: ProcessValueFields) {
        let (tx, rx) = channel();
        let last_backend = Arc::new(Mutex::new(Some(input.last_backend)));

        let result = ManateePrimaryResolver::process_value(
            &tx.clone(),
            &input.value,
            last_backend,
            test_log());
        match input.expected_error {
            None => assert_eq!(result, Ok(())),
            Some(expected_error) => {
                assert_eq!(result, Err(expected_error))
            }
        }

        let mut received_messages = Vec::new();

        // Receive as many messages as we expect
        for i in 0..input.message_count {
            let channel_result = rx.try_recv();
            match channel_result {
                Err(e) => panic!("Unexpected error receiving on channel: {:?} -- Loop iteration: {:?}", e, i),
                Ok(result) => {
                    received_messages.push(result);
                }
            }
        }
        // Can't use assert_eq! here because BackendMsg doesn't implement Debug
        match rx.try_recv() {
            Err(TryRecvError::Empty) => (),
            _ => panic!("Unexpected message on resolver channel")
        }

        if let Some(msg) = input.added_backend {
            let msg = BackendMsg::AddedMsg(msg);
            match util::find_msg_match(&received_messages, &msg) {
                None => panic!("added_backend not found in received messages"),
                Some(index) => {
                    received_messages.remove(index);
                    ()
                }
            }
        }

        if let Some(msg) = input.removed_backend {
            let msg = BackendMsg::RemovedMsg(msg);
            match util::find_msg_match(&received_messages, &msg) {
                None => panic!("removed_backend not found in received messages"),
                Some(index) => {
                    received_messages.remove(index);
                    ()
                }
            }
        }
    }

    #[test]
    fn port_ip_change_test() {
        let data_1 = backend_ip1_port1();
        let data_2 = backend_ip2_port2();

        run_process_value_fields(ProcessValueFields{
            value: data_2.raw(),
            last_backend: data_1.key(),
            expected_error: None,
            message_count: 2,
            added_backend: Some(data_2.added_msg()),
            removed_backend: Some(data_1.removed_msg())
        });
    }

    #[test]
    fn port_change_test() {
        let data_1 = backend_ip1_port1();
        let data_2 = backend_ip2_port1();

        run_process_value_fields(ProcessValueFields{
            value: data_2.raw(),
            last_backend: data_1.key(),
            expected_error: None,
            message_count: 2,
            added_backend: Some(data_2.added_msg()),
            removed_backend: Some(data_1.removed_msg())
        });
    }

    #[test]
    fn ip_change_test() {
        let data_1 = backend_ip1_port1();
        let data_2 = backend_ip1_port2();

        run_process_value_fields(ProcessValueFields{
            value: data_2.raw(),
            last_backend: data_1.key(),
            expected_error: None,
            message_count: 2,
            added_backend: Some(data_2.added_msg()),
            removed_backend: Some(data_1.removed_msg())
        });
    }

    #[test]
    fn no_change_test() {
        let data = backend_ip1_port1();

        run_process_value_fields(ProcessValueFields{
            value: data.raw(),
            last_backend: data.key(),
            expected_error: None,
            message_count: 0,
            added_backend: None,
            removed_backend: None
        });
    }

    #[test]
    fn no_ip_test() {
        let filler = backend_ip1_port1();

        run_process_value_fields(ProcessValueFields{
            value: raw_no_ip(),
            last_backend: filler.key(),
            expected_error: Some(ResolverError::MissingZkData(ZkDataField::Ip)),
            message_count: 0,
            added_backend: None,
            removed_backend: None
        });
    }

    #[test]
    fn wrong_type_ip_test() {
        let filler = backend_ip1_port1();

        run_process_value_fields(ProcessValueFields{
            value: raw_wrong_type_ip(),
            last_backend: filler.key(),
            expected_error: Some(ResolverError::InvalidZkData(ZkDataField::Ip)),
            message_count: 0,
            added_backend: None,
            removed_backend: None
        });
    }

    #[test]
    fn invalid_ip_test() {
        let filler = backend_ip1_port1();

        run_process_value_fields(ProcessValueFields{
            value: raw_invalid_ip(),
            last_backend: filler.key(),
            expected_error: Some(ResolverError::InvalidZkData(ZkDataField::Ip)),
            message_count: 0,
            added_backend: None,
            removed_backend: None
        });
    }

    #[test]
    fn no_pg_url_test() {
        let filler = backend_ip1_port1();

        run_process_value_fields(ProcessValueFields{
            value: raw_no_pg_url(),
            last_backend: filler.key(),
            expected_error: Some(ResolverError::MissingZkData(ZkDataField::PostgresUrl)),
            message_count: 0,
            added_backend: None,
            removed_backend: None
        });
    }

    #[test]
    fn wrong_type_pg_url_test() {
        let filler = backend_ip1_port1();

        run_process_value_fields(ProcessValueFields{
            value: raw_wrong_type_pg_url(),
            last_backend: filler.key(),
            expected_error: Some(ResolverError::InvalidZkData(ZkDataField::PostgresUrl)),
            message_count: 0,
            added_backend: None,
            removed_backend: None
        });
    }

    #[test]
    fn invalid_pg_url_test() {
        let filler = backend_ip1_port1();

        run_process_value_fields(ProcessValueFields{
            value: raw_invalid_pg_url(),
            last_backend: filler.key(),
            expected_error: Some(ResolverError::InvalidZkData(ZkDataField::PostgresUrl)),
            message_count: 0,
            added_backend: None,
            removed_backend: None
        });
    }

    #[test]
    fn no_port_pg_url_test() {
        let filler = backend_ip1_port1();

        run_process_value_fields(ProcessValueFields{
            value: raw_no_port_pg_url(),
            last_backend: filler.key(),
            expected_error: Some(ResolverError::MissingZkData(ZkDataField::Port)),
            message_count: 0,
            added_backend: None,
            removed_backend: None
        });
    }

    #[test]
    fn invalid_json_test() {
        let filler = backend_ip1_port1();

        run_process_value_fields(ProcessValueFields{
            value: raw_invalid_json(),
            last_backend: filler.key(),
            expected_error: Some(ResolverError::InvalidZkJson),
            message_count: 0,
            added_backend: None,
            removed_backend: None
        });
    }
 }
