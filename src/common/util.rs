use std::convert::From;
use std::default::Default;
use std::env;
use std::io::Error as StdIoError;
use std::process::Command;
use std::str::FromStr;
use std::sync::Mutex;
use std::sync::mpsc::{Receiver, RecvError, RecvTimeoutError, TryRecvError};
use std::thread;
use std::time::{Duration, Instant};

use clap::{crate_name, crate_version};
use failure::Error as FailureError;
use slog::{Drain, Level, LevelFilter, Logger, o};
use tokio_zookeeper::*;
use tokio::prelude::*;
use tokio::runtime::Runtime;

use cueball::resolver::{
    BackendAddedMsg,
    BackendRemovedMsg,
    BackendMsg,
    Resolver
};

use crate::{
    ManateePrimaryResolver,
    ZkConnectString
};

use std::sync::mpsc::channel;

use tokio_zookeeper::{Acl, CreateMode};
use tokio_zookeeper::error as tokio_zk_error;
use uuid::Uuid;

use super::test_data;

pub const DEFAULT_LOG_LEVEL: Level = Level::Info;
pub const LOG_LEVEL_ENV_VAR: &str = "RESOLVER_LOG_LEVEL";

#[derive(Debug, PartialEq)]
pub enum ZkStatus {
    Enabled,
    Disabled
}

#[derive(Debug)]
pub enum TestError {
    RecvTimeout,
    UnexpectedMessage,
    BackendMismatch(String),
    ZkError(String),
    InvalidTimeout,
    DisconnectError,
    SubprocessError(String),
    IncorrectBehavior(String),
    InvalidLogLevel
}

impl From<RecvTimeoutError> for TestError {
    fn from(_: RecvTimeoutError) -> Self {
        TestError::RecvTimeout
    }
}

impl From<TryRecvError> for TestError {
    fn from(_: TryRecvError) -> Self {
        TestError::DisconnectError
    }
}

impl From<RecvError> for TestError {
    fn from(_: RecvError) -> Self {
        TestError::DisconnectError
    }
}

impl From<tokio_zk_error::Delete> for TestError {
    fn from(e: tokio_zk_error::Delete) -> Self {
        TestError::ZkError(format!("{:?}", e))
    }
}

impl From<tokio_zk_error::Create> for TestError {
    fn from(e: tokio_zk_error::Create) -> Self {
        TestError::ZkError(format!("{:?}", e))
    }
}

impl From<tokio_zk_error::SetData> for TestError {
    fn from(e: tokio_zk_error::SetData) -> Self {
        TestError::ZkError(format!("{:?}", e))
    }
}

impl From<FailureError> for TestError {
    fn from(e: FailureError) -> Self {
        TestError::ZkError(format!("{:?}", e))
    }
}

impl From<StdIoError> for TestError {
    fn from(e: StdIoError) -> Self {
        TestError::SubprocessError(format!("{:?}", e))
    }
}

pub struct TestContext {
    pub connect_string: ZkConnectString,
    pub root_path: String,
    rt: Runtime,
}

impl Default for TestContext {
    fn default() -> Self {
        let connect_string = ZkConnectString::from_str(
             "127.0.0.1:2181").unwrap();
        let root_path = "/test-".to_string() + &Uuid::new_v4().to_string();
        TestContext::new(connect_string, root_path)
    }
}

impl TestContext{
    pub fn new(
        connect_string: ZkConnectString,
        root_path: String
    ) -> Self {
        TestContext{
            connect_string,
            root_path,
            rt: Runtime::new().unwrap(),
        }
    }

    fn setup_zk_nodes(&mut self) -> Result<(), TestError> {
        let root_path = self.root_path.clone();
        let data_path = root_path.clone() + "/state";
        self.create_zk_node(root_path, Vec::new())?;
        self.create_zk_node(data_path, Vec::new())?;
        Ok(())
    }

    fn teardown_zk_nodes(&mut self) -> Result<(), TestError> {
        let root_path = self.root_path.clone();
        let data_path = root_path.clone() + "/state";

        // The order here matters -- we must delete the child before the parent!
        self.delete_zk_node(data_path)?;
        self.delete_zk_node(root_path)?;
        Ok(())
    }

    fn finalize(self) {
        // Wait for the futures to resolve
        // TODO are they guaranteed to have all resolved already at this point?
        // ^evidently not -- why does shutdown_on_idle block?
        self.rt.shutdown_now().wait().unwrap();
    }

    fn create_zk_node(&mut self, path: String, data: Vec<u8>)
        -> Result<(), TestError> {
        self.rt.block_on(
            ZooKeeper::connect(&self.connect_string.to_string().parse().unwrap())
            .and_then(move |(zk, _)| {
                zk.create(
                    &path,
                    data,
                    Acl::open_unsafe(),
                    CreateMode::Persistent
                ).map(|(_, res)| res)
            })
        //
        // A double question mark! The first one unwraps the error
        // returned by ZooKeeper::connect; the second one unwraps the error
        // returned by create().
        //
        )??;
        Ok(())
    }


    fn set_zk_data(&mut self, path: String, data: Vec<u8>)
        -> Result<(), TestError> {

        self.rt.block_on(
            ZooKeeper::connect(&self.connect_string.to_string().parse().unwrap())
            .and_then(move |(zk, _)| {
                zk.set_data(
                    &path,
                    None,
                    data
                ).map(|(_, res)| res)
            })
        //
        // Another double question mark! The first one unwraps the error
        // returned by ZooKeeper::connect; the second one unwraps the error
        // returned by set_data().
        //
        )??;

        Ok(())
    }

    fn delete_zk_node(&mut self, path: String) -> Result<(), TestError> {
        self.rt.block_on(
            ZooKeeper::connect(&self.connect_string.to_string().parse().unwrap())
            .and_then(move |(zk, _)| {
                zk.delete(
                    &path,
                    None
                ).map(|(_, res)| res)
            })
        )??;
        //
        // Another double question mark! The first one unwraps the error
        // returned by ZooKeeper::connect; the second one unwraps the error
        // returned by delete().
        //
        Ok(())
    }

    fn run_test_case(
        &mut self,
        input: TestAction,
        external_rx: Option<Receiver<BackendMsg>>
    ) -> Result<(), TestError> {
        let data_path_start = self.root_path.clone() + "/state";
        let data_path_end = self.root_path.clone() + "/state";
        let start_data = input.start_data;
        let end_data = input.end_data;

        //
        // Start a resolver if we didn't get an external rx
        //
        let log = log_from_env(DEFAULT_LOG_LEVEL)?;
        let rx = external_rx.unwrap_or_else(|| {
            let (tx, rx) = channel();
            let connect_string = self.connect_string.clone();
            let root_path_resolver = self.root_path.clone();
            thread::spawn(move || {
                let mut resolver = ManateePrimaryResolver::new(connect_string,
                    root_path_resolver, Some(log));
                resolver.run(tx);
            });
            rx
        });

        self.set_zk_data(data_path_start, start_data)?;

        //
        // We want this thread to not progress until the resolver_thread above
        // has time to set the watch and block. REALLY, this should be done with
        // thread priorities, but the thread-priority crate depends on some
        // pthread functions that _aren't in the libc crate for illumos_.
        // We should add these functions and upstream a change, but, for now,
        // here's this.
        //
        thread::sleep(Duration::from_secs(2));

        self.set_zk_data(data_path_end, end_data)?;

        //
        // The test runner starts out with no data at the zookeeper node. Thus,
        // if start_data represents a valid backend, setting the node's data
        // to start_data will produce an AddedMsg. This isn't related to the
        // transition between start_data and end_data, but we should still make
        // sure that the message is received as we expect. We do that here.
        //
        if let Some(msg) = input.added_start_backend {
            let m = recv_timeout_discard_heartbeats(&rx, Duration::from_secs(2))?;
            if m != BackendMsg::AddedMsg(msg) {
                return Err(TestError::BackendMismatch(
                    "Message received does not match expected \
                    added_start_backend message".to_string()))
            }
        }

        let mut received_messages = Vec::new();

        let expected_message_count = {
            let mut acc = 0;
            if input.added_backend.is_some() {
                acc += 1;
            }
            if input.removed_backend.is_some() {
                acc += 1;
            }
            acc
        };

        // Receive as many messages as we expect
        for _ in 0..expected_message_count {
            let m = recv_timeout_discard_heartbeats(&rx, Duration::from_secs(2))?;
            received_messages.push(m);
        }

        // Make sure there are no more messages waiting
        match recv_timeout_discard_heartbeats(&rx, Duration::from_secs(2)) {
            Err(TestError::RecvTimeout) => (),
            Err(e) => return Err(e),
            _ => return Err(TestError::UnexpectedMessage)
        }

        if let Some(msg) = input.added_backend {
            let msg = BackendMsg::AddedMsg(msg);
            match find_msg_match(&received_messages, &msg) {
                None => return Err(TestError::BackendMismatch(
                    "added_backend not found in received messages".to_string())),
                Some(index) => {
                    received_messages.remove(index);
                }
            }
        }

        if let Some(msg) = input.removed_backend {
            let msg = BackendMsg::RemovedMsg(msg);
            match find_msg_match(&received_messages, &msg) {
                None => return Err(TestError::BackendMismatch(
                    "removed_backend not found in received messages".to_string())),
                Some(index) => {
                    received_messages.remove(index);
                }
            }
        }

        Ok(())
    }

    //
    // A wrapper that consumes and finalizes a TestContext, runs a TestAction,
    // and panics if the test returned an error.
    //
    pub fn run_action_and_finalize(mut self, action: TestAction) {
        self.setup_zk_nodes().unwrap();
        let result = self.run_test_case(action, None);
        self.teardown_zk_nodes().unwrap();
        self.finalize();
        //
        // The test might have turned ZooKeeper off and not turned it back on,
        // so we do that here.
        //
        toggle_zookeeper(ZkStatus::Enabled).unwrap();
        if let Err(e) = result {
            panic!("{:?}", e);
        }
    }

    //
    // A wrapper that consumes and finalizes a TestContext, runs an arbitrary
    // function that takes the passed-in TestContext as an argument, and panics
    // if the function returned an error. The function can assume that it does
    // not have to handle the creation and deletion of the ZK nodes at the
    // beginning and end of the test, respectively.
    //
    pub fn run_func_and_finalize<F>(mut self, func: F)
        where F: FnOnce(&mut TestContext) -> Result<(), TestError>
    {
        self.setup_zk_nodes().unwrap();
        let result = func(&mut self);
        self.teardown_zk_nodes().unwrap();
        self.finalize();
        //
        // The test might have turned ZooKeeper off and not turned it back on,
        // so we do that here.
        //
        toggle_zookeeper(ZkStatus::Enabled).unwrap();
        if let Err(e) = result {
            panic!("{:?}", e);
        }
    }
}

//
// Enables/disables the zookeeper server according to the passed-in `status`
// argument.
//
pub fn toggle_zookeeper(status: ZkStatus) -> Result<(), TestError> {
    const SVCADM_PATH: &str = "/usr/sbin/svcadm";
    const SVCADM_ZOOKEEPER_SERVICE_NAME: &str = "zookeeper";

    let arg = match status {
        ZkStatus::Enabled => "enable",
        ZkStatus::Disabled => "disable"
    };

    let status = Command::new(SVCADM_PATH)
        .arg(arg)
        .arg("-s")
        .arg(SVCADM_ZOOKEEPER_SERVICE_NAME)
        .status()?;

    if !status.success() {
        let msg = match status.code() {
            Some(code) => format!("svcadm exited with status {}", code),
            None => "svcadm killed by signal before finishing".to_string()
        };
        return Err(TestError::SubprocessError(msg));
    }

    Ok(())
}

fn recv_timeout_discard_heartbeats(rx: &Receiver<BackendMsg>, timeout: Duration)
    -> Result<BackendMsg, TestError> {

    let start_time = Instant::now();

    const CHECK_INTERVAL: Duration = Duration::from_millis(10);

    //
    // Spawn a thread to send heartbeats over its own channel at a fine-grained
    // frequency, for use in the loop below.
    //
    let (failsafe_tx, failsafe_rx) = channel();
    thread::spawn(move || {
        loop {
            if failsafe_tx.send(BackendMsg::HeartbeatMsg).is_err() {
                break;
            }
            thread::sleep(CHECK_INTERVAL);
        }
    });


    //
    // This is a little wonky. We'd really like to do a select() between
    // rx and failsafe_rx, but rust doesn't have a stable select() function
    // that works with mpsc channels. Bummer! Instead, while the timeout hasn't
    // run out, we try_recv on rx. If we don't find a non-hearbeat message,
    // we wait on failsafe_rx, because we _know_ a message will come over it
    // every CHECK_INTERVAL milliseconds. In this way, we rate-limit our
    // repeated calls to try_recv. Oy!
    //
    while start_time.elapsed() < timeout {
        match rx.try_recv() {
            Err(TryRecvError::Empty) => (),
            Err(e) => return Err(TestError::from(e)),
            Ok(msg) => {
                match msg {
                    BackendMsg::HeartbeatMsg => (),
                    msg => return Ok(msg)
                }
            }
        }
        failsafe_rx.recv()?;
    }

    Err(TestError::RecvTimeout)
}

//
// Checks if a resolver is connected to zookeeper. This function assumes that
// the "<root path>/state" node already exists, and will return 'false' if it
// doesn't.
//
// This function attempts to get the resolver to send a message over the passed-
// in channel, and then receives that message. This means that the channel
// should be empty, so we don't receive a _different_ message than the one we
// expect. This in turn means that if other functions also intend to
// send/receive, they shouldn't be running at the same time as this one. In the
// long run, we should probably use a mutex to protect the channel.
//
pub fn resolver_connected(ctx: &mut TestContext, rx: &Receiver<BackendMsg>)
    -> Result<bool, TestError> {

    let data_path = ctx.root_path.clone() + "/state";
    let data_path_clone = data_path.clone();

    //
    //
    // Force a change in the data. If we get an error even trying to set the
    // change, we assume that zookeeper isn't running. If zookeeper isn't
    // running, then the resolver is not connected to it, so we can return
    // `false` here.
    //
    // It would be best to check that the error is the specific type we expect.
    // This is difficult, because failure::Error doesn't really let you extract
    // any structured information that we can compare to a known entity.
    //
    if let Err(_) = ctx.set_zk_data(data_path,
        test_data::backend_ip1_port1().raw_vec()) {
        return Ok(false);
    }

    //
    // If we got here, it means that the _above_ call to set_zk_data()
    // succeeded, which means that zookeeper is running. Thus, we don't expect
    // the second call to set_zk_data() to error, and panic if it does.
    //
    ctx.set_zk_data(data_path_clone, test_data::backend_ip2_port2().raw_vec())?;

    let channel_result =
            recv_timeout_discard_heartbeats(&rx, Duration::from_secs(2));

    //
    // Just verify that we receive some message. In other tests, we verify that
    // we receive exactly the messages we expect, so as long as we get
    // _something_, we know that the resolver is talking to the ZK server.
    //
    // TODO check for exact match
    match channel_result {
        Ok(_) => {
            Ok(true)
        },
        Err(TestError::RecvTimeout) => {
            Ok(false)
        },
        Err(e) => Err(e)
    }
}

///
/// Given a list of BackendMsg and a BackendMsg to find, returns the index of
/// the desired item in the list, or None if the item is not in the list. We
/// return the index, rather than the item itself, to allow callers of the
/// function to more easily manipulate the list afterward.
///
pub fn find_msg_match(list: &[BackendMsg], to_find: &BackendMsg)
    -> Option<usize> {
    for (index, item) in list.iter().enumerate() {
        if item == to_find {
            return Some(index);
        }
    }
    None
}

pub struct TestAction {
    // Data for a given zookeeper node
    pub start_data: Vec<u8>,
    // Another data string for the same node
    pub end_data: Vec<u8>,
    //
    // The message expected when transitioning from an empty zookeeper node to
    // start_data
    //
    pub added_start_backend: Option<BackendAddedMsg>,
    //
    // The messages expected when transitioning from start_data to end_data
    //
    pub added_backend: Option<BackendAddedMsg>,
    pub removed_backend: Option<BackendRemovedMsg>
}

pub fn parse_log_level(s: String) -> Result<Level, TestError> {
    match s.to_lowercase().as_str() {
       "trace" => Ok(Level::Trace),
       "debug" => Ok(Level::Debug),
       "info" => Ok(Level::Info),
       "warning" => Ok(Level::Warning),
       "error" => Ok(Level::Error),
       "critical" => Ok(Level::Critical),
       _ => Err(TestError::InvalidLogLevel)
    }
}

pub fn log_level_from_env() -> Result<Option<Level>, TestError> {
    let level_env = env::var_os(LOG_LEVEL_ENV_VAR);
    let level = match level_env {
        Some(level_str) => {
            Some(parse_log_level(level_str.into_string().unwrap())?)
        },
        None => None
    };
    Ok(level)
}

pub fn standard_log(l: Level) -> Logger {
    Logger::root(
        Mutex::new(LevelFilter::new(
            slog_bunyan::with_name(crate_name!(),
                std::io::stdout()).build(),
            l)).fuse(),
            o!("build-id" => crate_version!()))
}

pub fn log_from_env(default_level: Level) -> Result<Logger, TestError> {
    let level = log_level_from_env()?.unwrap_or(default_level);
    Ok(standard_log(level))
}
