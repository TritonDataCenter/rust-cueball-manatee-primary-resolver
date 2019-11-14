use std::fmt::{Debug, Display};
use std::str::{FromStr};
use std::sync::mpsc::{RecvTimeoutError, Receiver, Sender};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use clap::{crate_name, crate_version};
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

use cueball_manatee_primary_resolver::{ManateePrimaryResolver, ZkConnectString};

use cueball_manatee_primary_resolver::util;

use std::iter;
use std::sync::mpsc::channel;

use tokio_zookeeper::{Acl, CreateMode};
use uuid::Uuid;

struct TestRunner{
    connect_string: ZkConnectString,
    root_path: String,
    rt: Runtime,
    tx: Sender<BackendMsg>,
    rx: Receiver<BackendMsg>
}

impl TestRunner{
    fn new(
        connect_string: ZkConnectString,
        root_path: String
    ) -> Self {
        let (tx, rx) = channel();
        TestRunner{
            connect_string,
            root_path,
            rt: Runtime::new().unwrap(),
            tx,
            rx
        }
    }

    // TODO proper error-handling and cleanup on failure
    // TODO refactor duplicate code

    fn setup(&mut self) {
        let root_path = self.root_path.clone();
        let root_path_clone = self.root_path.clone();
        let data_path = root_path + "/state";
        // TODO I don't think errors here (or in any of these similar cases)
        // get panicked upon
        self.rt.block_on(
            ZooKeeper::connect(&self.connect_string.to_string().parse().unwrap())
            .and_then(move |(zk, watcher)| {
                zk.create(
                    &root_path_clone,
                    &b""[..],
                    Acl::open_unsafe(),
                    CreateMode::Persistent
                )
            })
        ).unwrap();
        self.rt.block_on(
            ZooKeeper::connect(&self.connect_string.to_string().parse().unwrap())
            .and_then(move |(zk, watcher)| {
                zk.create(
                    &data_path,
                    &b""[..],
                    Acl::open_unsafe(),
                    CreateMode::Persistent
                )
            })
        ).unwrap();
    }

    fn teardown(mut self) {
        let root_path = self.root_path.clone();
        let root_path_clone = self.root_path.clone();
        let data_path = root_path + "/state";

        self.rt.block_on(
            ZooKeeper::connect(&self.connect_string.to_string().parse().unwrap())
            .and_then(move |(zk, watcher)| {
                zk.delete(
                    &data_path,
                    None
                )
            })
        ).unwrap();

        self.rt.block_on(
            ZooKeeper::connect(&self.connect_string.to_string().parse().unwrap())
            .and_then(move |(zk, watcher)| {
                zk.delete(
                    &root_path_clone,
                    None
                )
            })
        ).unwrap();

        // Wait for the futures to resolve
        // TODO are they guaranteed to have all resolved already at this point?
        self.rt.shutdown_on_idle().wait().unwrap();
        println!("Done!");
    }

    fn recv_timeout_discard_heartbeats(&self, timeout: Duration)
        -> Result<BackendMsg, RecvTimeoutError> {

        let start_time = Instant::now();
        let failsafe = Duration::from_secs(10);

        if timeout > failsafe {
            panic!("recv_timeout_discard_heartbeats called with timeout \
                greater than failsafe");
        }

        while start_time.elapsed() < timeout {
            match self.rx.recv_timeout(failsafe)? {
                BackendMsg::HeartbeatMsg => continue,
                msg => return Ok(msg)
            }
        }

        Err(RecvTimeoutError::Timeout)
    }

    fn run_test(&mut self, input: TestAction<'static>) {
        let data_path_start = self.root_path.clone() + "/state";
        let data_path_end = self.root_path.clone() + "/state";
        let start_data = input.start_data.clone();
        let end_data = input.end_data.clone();

        let (tx, rx) = channel();

        // Set the data to `start_data`
        let zk = self.rt.block_on(
            ZooKeeper::connect(&self.connect_string.to_string().parse().unwrap())
            .and_then(move |(zk, watcher)| {
                zk.set_data(
                    &data_path_start,
                    None,
                    start_data
                )
            })
        ).unwrap();

        let connect_string = self.connect_string.clone();
        let root_path_resolver = self.root_path.clone();
        let resolver_thread = thread::spawn(move || {
            let mut resolver = ManateePrimaryResolver::new(connect_string,
                root_path_resolver);
            resolver.run(tx);
        });

        // We want this thread to not progress until the resolver_thread above
        // has time to set the watch and block. REALLY, this should be done with
        // thread priorities, but the thread-priority crate depends on some
        // pthread functions that _aren't in the libc crate for illumos_.
        // We should add these functions and upstream a change, but, for now,
        // here's this.
        thread::sleep(Duration::from_secs(2));

        // Set the data to `end_data`
        let zk = self.rt.block_on(
            ZooKeeper::connect(&self.connect_string.to_string().parse().unwrap())
            .and_then(move |(zk, watcher)| {
                zk.set_data(
                    &data_path_end,
                    None,
                    end_data
                )
            })
        ).unwrap();

        let mut received_messages = Vec::new();

        // Receive as many messages as we expect
        for i in 0..input.message_count {
            let channel_result =
                self.recv_timeout_discard_heartbeats(Duration::from_secs(2));
            match channel_result {
                Err(e) => panic!("Unexpected error receiving on channel: {:?} -- Loop iteration: {:?}", e, i),
                Ok(result) => {
                    received_messages.push(result);
                }
            }
        }

        // Can't use assert_eq! here because BackendMsg doesn't implement Debug
        match self.recv_timeout_discard_heartbeats(Duration::from_secs(2)) {
            Err(RecvTimeoutError::Timeout) => (),
            Err(e) => panic!("Unexpected error on resolver channel: {:?}", e),
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
        // match input.event {
        //     Some(event) => {
        //         match rx.recv_timeout(Duration::from_secs(2)) {
        //             Ok(_) => {

        //             },
        //             Err(RecvTimeoutError::Timeout) => {
        //                 panic!("No event received");
        //             },
        //             Err(RecvTimeoutError::Disconnected) => {
        //                 panic!("Resolver disconnected unexpectedly");
        //             }
        //         }
        //     },
        //     None => {
        //         match rx.recv_timeout(Duration::from_secs(2)) {
        //             Err(RecvTimeoutError::Timeout) => {

        //             },
        //             Err(RecvTimeoutError::Disconnected) => {
        //                 panic!("Resolver disconnected unexpectedly");
        //             },
        //             Ok(event) => {
        //                 match event {
        //                     BackendMsg::AddedMsg(_) => println!("added"),
        //                     BackendMsg::RemovedMsg(_) => println!("removed"),
        //                     // TODO account for heartbeat messages
        //                     BackendMsg::HeartbeatMsg => println!("fed"),
        //                     BackendMsg::StopMsg => println!("feq!"),
        //                 }
        //                 panic!("Unexpected event received");
        //             }
        //         }
        //     }
        // }
    }
}

struct TestAction<'a> {
    // Data for a given zookeeper node
    start_data: &'a [u8],
    // Another data string for the same node
    end_data: &'a [u8],
    message_count: u32,
    added_backend: Option<BackendAddedMsg>,
    removed_backend: Option<BackendRemovedMsg>
}

#[test]
fn testy_test() {
    // TODO don't hardcode zk string
    let conn_str = ZkConnectString::from_str(
         "10.77.77.6:2181").unwrap();
    let root_path = "/test-".to_string() + &Uuid::new_v4().to_string();
    let mut runner = TestRunner::new(
        conn_str,
        root_path
    );
    // TODO do some refactoring around who calls setup/run_test/teardown, or at
    // least write a check to make sure that run_test isn't called without
    // calling the other two.
    runner.setup();
    runner.run_test(TestAction {
        start_data: r#" {
                "generation": 1,
                "primary": {
                    "id": "10.77.77.21:5432:12345",
                    "ip": "10.77.77.21",
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
        "#.as_bytes(),
        end_data: r#" {
                "generation": 1,
                "primary": {
                    "id": "10.77.77.21:5432:12345",
                    "ip": "10.77.77.21",
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
        "#.as_bytes(),
        message_count: 0,
        added_backend: None,
        removed_backend: None
    });
    runner.teardown();
}