use std::env;
use std::fmt::{Debug, Display};
use std::process::Command;
use std::str::{FromStr};
use std::sync::mpsc::{RecvTimeoutError, Receiver, Sender};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

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
    ManateePrimaryResolver,
    ZkConnectString,
    HEARTBEAT_INTERVAL,
    RECONNECT_DELAY,
    SESSION_TIMEOUT,
    TCP_CONNECT_TIMEOUT,
};

use std::iter;
use std::panic;
use std::sync::mpsc::channel;

use tokio_zookeeper::{Acl, CreateMode};
use uuid::Uuid;

use util::{TestContext};

#[derive(Debug, PartialEq)]
enum ZkStatus {
    Enabled,
    Disabled
}

pub mod util;
pub mod test_data;

//
// Tests that the resolver exits immediately if the receiver is closed when the
// resolver starts
//
#[test]
fn channel_test_start_with_closed_rx() {

    let (tx, rx) = channel();

    let connect_string = ZkConnectString::from_str(
         "10.77.77.6:2181").unwrap();
    let root_path = "/test-".to_string() + &Uuid::new_v4().to_string();

    let root_path_resolver = root_path.clone();
    let tx_clone = tx.clone();

    let pair = Arc::new((Mutex::new(false), Condvar::new()));
    let pair2 = pair.clone();

    // Close the receiver before the resolver even starts
    drop(rx);

    let resolver_thread = thread::spawn(move || {
        let (lock, cvar) = &*pair2;
        let mut resolver = ManateePrimaryResolver::new(connect_string,
            root_path_resolver, None);
        resolver.run(tx_clone);
        let mut resolver_exited = lock.lock().unwrap();
        *resolver_exited = true;
        cvar.notify_all();
    });


    let (lock, cvar) = &*pair;
    let mut resolver_exited = lock.lock().unwrap();

    while !*resolver_exited {
        let result =
            cvar.wait_timeout(resolver_exited, Duration::from_secs(1)).unwrap();
        resolver_exited = result.0;
        if result.1.timed_out() {
            panic!("Resolver did not immediately exit upon starting with closed receiver");
        }
    }
}

//
// Tests that the resolver exits within HEARTBEAT_INTERVAL if the receiver is
// closed while the resolver is running.
//
#[test]
fn channel_test_exit_upon_closed_rx() {

    let (tx, rx) = channel();

    let connect_string = ZkConnectString::from_str(
         "127.0.0.1:2181").unwrap();
    let root_path = "/test-".to_string() + &Uuid::new_v4().to_string();

    let root_path_resolver = root_path.clone();
    let tx_clone = tx.clone();

    let pair = Arc::new((Mutex::new(false), Condvar::new()));
    let pair2 = pair.clone();

    let resolver_thread = thread::spawn(move || {
        let (lock, cvar) = &*pair2;
        let mut resolver = ManateePrimaryResolver::new(connect_string,
            root_path_resolver, None);
        resolver.run(tx_clone);
        let mut resolver_exited = lock.lock().unwrap();
        *resolver_exited = true;
        cvar.notify_all();
    });

    //
    // We want this thread to not progress until the resolver_thread above has
    // time to set the watch and block. REALLY, this should be done with thread
    // priorities, but the thread-priority crate depends on some pthread
    // functions that _aren't in the libc crate for illumos_. We should add
    // these functions and upstream a change, but, for now, here's this.
    //
    thread::sleep(Duration::from_secs(2));

    // Close the receiver once the resolver has started
    drop(rx);

    let (lock, cvar) = &*pair;
    let mut resolver_exited = lock.lock().unwrap();

    while !*resolver_exited {
        //
        // We should wait a little longer than HEARTBEAT_INTERVAL for the
        // resolver to notice that the receiver got closed.
        //
        let result = cvar.wait_timeout(
            resolver_exited,
            Duration::from_millis(10) + HEARTBEAT_INTERVAL
        ).unwrap();
        resolver_exited = result.0;
        if result.1.timed_out() {
            panic!("Resolver did not exit upon closure of receiver");
        }
    }
}
