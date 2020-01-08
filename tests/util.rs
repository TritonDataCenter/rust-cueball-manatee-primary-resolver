use std::convert::From;
use std::default::Default;
use std::env;
use std::fmt::{Debug, Display};
use std::io::Error as IoError;
use std::str::{FromStr};
use std::sync::mpsc::{RecvTimeoutError, Receiver, Sender};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use failure::Error as FailureError;

use futures::future::{Either};

use clap::{crate_name, crate_version, App, Arg};
use tokio_zookeeper::*;
use tokio::prelude::*;
use tokio::runtime::Runtime;

use cueball::backend::*;
use cueball::resolver::{
    BackendAddedMsg,
    BackendRemovedMsg,
    BackendMsg,
    Resolver
};

use cueball_manatee_primary_resolver::{
    util,
    ManateePrimaryResolver,
    ZkConnectString
};

use std::iter;
use std::panic;
use std::sync::mpsc::channel;

use tokio_zookeeper::{Acl, CreateMode};
use uuid::Uuid;

// TODO do anything other than this
#[path = "./test_data.rs"]
mod test_data;

// pub macro_rules! clean_exit {
//     ($self:expr) => ({
//         $self.teardown_zk_nodes();
//         $self.finalize();
//         panic!()
//     });
//     ($self:expr, $msg:expr) => ({
//         $self.teardown_zk_nodes();
//         $self.finalize();
//         panic!($msg)
//     });
//     ($self:expr, $fmt:expr, $($arg:tt)+) => ({
//         $self.teardown_zk_nodes();
//         $self.finalize();
//         panic!($fmt, $($arg)+)
//     });
// }

// TODO impl From for various errors instead of converting manually in functions
#[derive(Debug)]
pub enum TestError {
    RecvTimeout,
    UnexpectedMessage,
    BackendMismatch(String),
    ZkError(String),
    InvalidTimeout
}

impl From<RecvTimeoutError> for TestError {
    fn from(_: RecvTimeoutError) -> Self {
        TestError::RecvTimeout
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

    // TODO proper error-handling and cleanup on failure

    pub fn setup_zk_nodes(&mut self) -> Result<(), TestError> {
        let root_path = self.root_path.clone();
        let data_path = root_path.clone() + "/state";
        // TODO I don't think errors in calls to this function
        // get panicked upon
        self.set_zk_data(root_path, Vec::new())?;
        self.set_zk_data(data_path, Vec::new())?;
        Ok(())
    }

    pub fn teardown_zk_nodes(&mut self) -> Result<(), TestError> {
        let root_path = self.root_path.clone();
        let data_path = root_path.clone() + "/state";

        // The order here matters -- we must delete the child before the parent!
        self.delete_zk_node(data_path)?;
        self.delete_zk_node(root_path)?;
        Ok(())
    }

    pub fn finalize(self) {
        // Wait for the futures to resolve
        // TODO are they guaranteed to have all resolved already at this point?
        // TODO ^evidently not -- why does shutdown_on_idle block?
        // TODO should we unwrap or return an error here?
        self.rt.shutdown_now().wait().unwrap();
    }

    fn set_zk_data(&mut self, path: String, data: Vec<u8>) -> Result<(), TestError> {
        self.rt.block_on(
            ZooKeeper::connect(&self.connect_string.to_string().parse().unwrap())
            .and_then(move |(zk, watcher)| {
                zk.exists(&path)
                .and_then(move |(zk, stat)| {
                    match stat {
                        None => {
                            Either::A(zk.create(
                                &path,
                                data,
                                Acl::open_unsafe(),
                                CreateMode::Persistent
                            ).map(|_| ()))
                        }
                        Some(_) => {
                            Either::B(zk.set_data(
                                &path,
                                None,
                                data
                            ).map(|_| ()))
                        }
                    }
                })
            })
            .map_err(|e| TestError::ZkError(format!("{:?}", e)))
        )
    }

    fn delete_zk_node(&mut self, path: String) -> Result<(), TestError> {
        self.rt.block_on(
            ZooKeeper::connect(&self.connect_string.to_string().parse().unwrap())
            .and_then(move |(zk, watcher)| {
                zk.delete(
                    &path,
                    None
                )
            })
            .map(|_| ())
            .map_err(|e| TestError::ZkError(format!("{:?}", e)))
        )
    }
}

fn recv_timeout_discard_heartbeats(rx: &Receiver<BackendMsg>, timeout: Duration)
    -> Result<BackendMsg, TestError> {

    let start_time = Instant::now();
    let failsafe = Duration::from_secs(10);

    if timeout > failsafe {
        return Err(TestError::InvalidTimeout);
    }

    // TODO this is wonky because if the timeout is less than the heartbeat
    // interval then the elapsed time only gets checked every heartbeat-interval
    // seconds and that's not ideal
    while start_time.elapsed() < timeout {
        match rx.recv_timeout(failsafe)? {
            BackendMsg::HeartbeatMsg => continue,
            msg => return Ok(msg)
        }
    }

    Err(TestError::RecvTimeout)
}

// This assumes that the "<root path>/state" node already exists. TODO enforce this
pub fn resolver_connected(ctx: &mut TestContext, rx: &Receiver<BackendMsg>)
    -> Result<bool, TestError> {

    // fn is_connection_error(err: FailureError) -> bool {
    //     // TODO find where `146` is defined as ConnectionRefused and use that
    //     // instead
    //     err.compat() == IoError::from_raw_os_error(146)
    // }

    let data_path = ctx.root_path.clone() + "/state";
    let data_path_clone = data_path.clone();

    //
    //
    // Force a change in the data. If we get an error even trying to set the
    // change, we assume that zookeeper isn't running. If zookeeper isn't
    // running, then the resolver is not connected to it, so we can return
    // `false` here.
    //
    // TODO check that the error is the specific type we expect. This is
    // difficult, because failure::Error doesn't really let you extract any
    // structured information that we can compare to a known entity.
    if let Err(_) = ctx.set_zk_data(data_path, test_data::backend_ip1_port1().raw_vec()) {
        return Ok(false);
    }

    //
    // If we got here, it means that the _above_ call to set_zk_data()
    // succeeded, which means that zookeeper is running. Thus, we don't expect
    // the second call to set_zk_data() to error, and panic if it does.
    //
    ctx.set_zk_data(data_path_clone, test_data::backend_ip2_port2().raw_vec())?;

    // TODO flush channel somehow? i.e. recv in a loop until error
    let channel_result =
            recv_timeout_discard_heartbeats(&rx, Duration::from_secs(2));

    //
    // Just verify that we receive some message. In other tests, we verify that
    // we receive exactly the messages we expect, so as long as we get
    // _something_, we know that the resolver is talking to the ZK server.
    //
    match channel_result {
        Ok(m) => {
            Ok(true)
        },
        Err(e) => {
            Ok(false)
        }
    }
}

// Consumes a TestContext
pub fn run_test_case(
    ctx: &mut TestContext,
    input: TestAction,
    external_rx: Option<Receiver<BackendMsg>>
) -> Result<(), TestError> {
    let data_path_start = ctx.root_path.clone() + "/state";
    let data_path_end = ctx.root_path.clone() + "/state";
    let start_data = input.start_data;
    let end_data = input.end_data;

    ctx.setup_zk_nodes()?;

    //
    // Start a resolver if we didn't get an external rx
    //
    let rx = external_rx.unwrap_or_else(|| {
        let (tx, rx) = channel();
        let connect_string = ctx.connect_string.clone();
        let root_path_resolver = ctx.root_path.clone();
        let resolver_thread = thread::spawn(move || {
            let mut resolver = ManateePrimaryResolver::new(connect_string,
                root_path_resolver, None);
            resolver.run(tx);
        });
        rx
    });

    ctx.set_zk_data(data_path_start, start_data)?;

    //
    // We want this thread to not progress until the resolver_thread above
    // has time to set the watch and block. REALLY, this should be done with
    // thread priorities, but the thread-priority crate depends on some
    // pthread functions that _aren't in the libc crate for illumos_.
    // We should add these functions and upstream a change, but, for now,
    // here's this.
    //
    thread::sleep(Duration::from_secs(2));

    ctx.set_zk_data(data_path_end, end_data)?;

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
    for i in 0..expected_message_count {
        let m = recv_timeout_discard_heartbeats(&rx, Duration::from_secs(2))?;
        received_messages.push(m);
    }

    // Make sure there are no more messages waiting
    // Can't use assert_eq! here because BackendMsg doesn't implement Debug
    match recv_timeout_discard_heartbeats(&rx, Duration::from_secs(2)) {
        Err(TestError::RecvTimeout) => (),
        Err(e) => return Err(e),
        _ => return Err(TestError::UnexpectedMessage)
    }

    if let Some(msg) = input.added_backend {
        let msg = BackendMsg::AddedMsg(msg);
        match util::find_msg_match(&received_messages, &msg) {
            None => return Err(TestError::BackendMismatch(
                "added_backend not found in received messages".to_string())),
            Some(index) => {
                received_messages.remove(index);
            }
        }
    }

    if let Some(msg) = input.removed_backend {
        let msg = BackendMsg::RemovedMsg(msg);
        match util::find_msg_match(&received_messages, &msg) {
            None => return Err(TestError::BackendMismatch(
                "removed_backend not found in received messages".to_string())),
            Some(index) => {
                received_messages.remove(index);
            }
        }
    }

    ctx.teardown_zk_nodes()?;
    // TODO get this function to _not_ consume the testcontext, and then move the
    // call to finalize() out of this funtion and into run_test in watch_test.rs.
    // This will involve making this function return errors instead of panicking, and
    // then calling clean_exit! in run_test(), I think.
    // Ok(ctx.finalize())
    Ok(())
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
