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

macro_rules! clean_exit {
    ($self:expr) => ({
        $self.teardown_zk_nodes();
        $self.finalize();
        panic!()
    });
    ($self:expr, $msg:expr) => ({
        $self.teardown_zk_nodes();
        $self.finalize();
        panic!($msg)
    });
    ($self:expr, $fmt:expr, $($arg:tt)+) => ({
        $self.teardown_zk_nodes();
        $self.finalize();
        panic!($fmt, $($arg)+)
    });
}

// TODO rename to TestContext
pub struct TestRunner {
    pub connect_string: ZkConnectString,
    pub root_path: String,
    rt: Runtime,
}

impl Default for TestRunner {
    fn default() -> Self {
        let connect_string = ZkConnectString::from_str(
             "127.0.0.1:2181").unwrap();
        let root_path = "/test-".to_string() + &Uuid::new_v4().to_string();
        TestRunner::new(connect_string, root_path)
    }
}

impl TestRunner{
    pub fn new(
        connect_string: ZkConnectString,
        root_path: String
    ) -> Self {
        TestRunner{
            connect_string,
            root_path,
            rt: Runtime::new().unwrap(),
        }
    }

    // TODO proper error-handling and cleanup on failure
    // TODO refactor duplicate code

    pub fn setup_zk_nodes(&mut self) {
        let root_path = self.root_path.clone();
        let data_path = root_path.clone() + "/state";
        // TODO I don't think errors in calls to this function
        // get panicked upon
        self.set_zk_data(root_path, Vec::new());
        self.set_zk_data(data_path, Vec::new());
    }

    pub fn teardown_zk_nodes(&mut self) {
        let root_path = self.root_path.clone();
        let data_path = root_path.clone() + "/state";

        // The order here matters -- we must delete the child before the parent!
        self.delete_zk_node(data_path);
        self.delete_zk_node(root_path);
    }

    pub fn finalize(self) {
        // Wait for the futures to resolve
        // TODO are they guaranteed to have all resolved already at this point?
        // TODO ^evidently not -- why does shutdown_on_idle block?
        self.rt.shutdown_now().wait().unwrap();
    }

    fn set_zk_data(&mut self, path: String, data: Vec<u8>) -> Result<(), FailureError> {
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
        )
    }

    fn delete_zk_node(&mut self, path: String) {
        self.rt.block_on(
            ZooKeeper::connect(&self.connect_string.to_string().parse().unwrap())
            .and_then(move |(zk, watcher)| {
                zk.delete(
                    &path,
                    None
                )
            })
        ).unwrap();
    }
}

fn recv_timeout_discard_heartbeats(rx: &Receiver<BackendMsg>, timeout: Duration)
    -> Result<BackendMsg, RecvTimeoutError> {

    let start_time = Instant::now();
    let failsafe = Duration::from_secs(10);

    if timeout > failsafe {
        // TODO make this a clean exit.
        panic!("recv_timeout_discard_heartbeats called with timeout \
            greater than failsafe");
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

    Err(RecvTimeoutError::Timeout)
}

// This assumes that the "<root path>/state" node already exists. TODO enforce this
pub fn resolver_connected(ctx: &mut TestRunner, rx: &Receiver<BackendMsg>) -> bool {

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
        return false
    }

    //
    // If we got here, it means that the _above_ call to set_zk_data()
    // succeeded, which means that zookeeper is running. Thus, we don't expect
    // the second call to set_zk_data() to error, and panic if it does.
    //
    ctx.set_zk_data(data_path_clone, test_data::backend_ip2_port2().raw_vec()).unwrap();

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
            true
        },
        Err(e) => {
            false
        }
    }
}

// Consumes a TestRunner
pub fn run_test_case(mut ctx: TestRunner, input: TestAction) {
    let data_path_start = ctx.root_path.clone() + "/state";
    let data_path_end = ctx.root_path.clone() + "/state";
    let start_data = input.start_data;
    let end_data = input.end_data;

    let (tx, rx) = channel();

    ctx.setup_zk_nodes();

    ctx.set_zk_data(data_path_start, start_data);

    let connect_string = ctx.connect_string.clone();
    let root_path_resolver = ctx.root_path.clone();
    let tx_clone = tx.clone();
    let resolver_thread = thread::spawn(move || {
        let mut resolver = ManateePrimaryResolver::new(connect_string,
            root_path_resolver, None);
        resolver.run(tx_clone);
    });

    //
    // We want this thread to not progress until the resolver_thread above
    // has time to set the watch and block. REALLY, this should be done with
    // thread priorities, but the thread-priority crate depends on some
    // pthread functions that _aren't in the libc crate for illumos_.
    // We should add these functions and upstream a change, but, for now,
    // here's this.
    //
    thread::sleep(Duration::from_secs(2));

    ctx.set_zk_data(data_path_end, end_data);

    //
    // The test runner starts out with no data at the zookeeper node. Thus,
    // if start_data represents a valid backend, setting the node's data
    // to start_data will produce an AddedMsg. This isn't related to the
    // transition between start_data and end_data, but we should still make
    // sure that the message is received as we expect. We do that here.
    //
    if let Some(msg) = input.added_start_backend {
        let channel_result =
            recv_timeout_discard_heartbeats(&rx, Duration::from_secs(2));
        match channel_result {
            Err(e) => clean_exit!(ctx, "Unexpected error receiving on channel: {:?}", e),
            Ok(m) => {
                if m != BackendMsg::AddedMsg(msg) {
                    clean_exit!(ctx, "Message received does not match expected added_start_backend message")
                }
            }
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
        let channel_result =
            recv_timeout_discard_heartbeats(&rx, Duration::from_secs(2));
        match channel_result {
            Err(e) => clean_exit!(ctx, "Unexpected error receiving on channel: {:?} -- Loop iteration: {:?}", e, i),
            Ok(result) => {
                received_messages.push(result);
            }
        }
    }

    // Can't use assert_eq! here because BackendMsg doesn't implement Debug
    match recv_timeout_discard_heartbeats(&rx, Duration::from_secs(2)) {
        Err(RecvTimeoutError::Timeout) => (),
        Err(e) => clean_exit!(ctx, "Unexpected error on resolver channel: {:?}", e),
        _ => clean_exit!(ctx, "Unexpected message on resolver channel")
    }

    if let Some(msg) = input.added_backend {
        let msg = BackendMsg::AddedMsg(msg);
        match util::find_msg_match(&received_messages, &msg) {
            None => clean_exit!(ctx, "added_backend not found in received messages"),
            Some(index) => {
                received_messages.remove(index);
                ()
            }
        }
    }

    if let Some(msg) = input.removed_backend {
        let msg = BackendMsg::RemovedMsg(msg);
        match util::find_msg_match(&received_messages, &msg) {
            None => clean_exit!(ctx, "removed_backend not found in received messages"),
            Some(index) => {
                received_messages.remove(index);
                ()
            }
        }
    }

    ctx.teardown_zk_nodes();
    // TODO get this function to _not_ consume the testcontext, and then move the
    // call to finalize() out of this funtion and into run_test in watch_test.rs.
    // This will involve making this function return errors instead of panicking, and
    // then calling clean_exit! in run_test(), I think.
    ctx.finalize()
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
