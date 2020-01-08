use std::env;
use std::fmt::{Debug, Display};
use std::process::Command;
use std::str::{FromStr};
use std::sync::mpsc::{RecvTimeoutError, Receiver, Sender};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use clap::{crate_name, crate_version, App, Arg};
use serial_test::serial;
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

mod util;
mod test_data;

fn toggle_zookeeper(status: ZkStatus) {
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
        .status()
        .expect("failed to execute process");

    if !status.success() {
        let msg = match status.code() {
            Some(code) => format!("svcadm exited with status {}", code),
            None => "svcadm killed by signal before finishing".to_string()
        };
        panic!(msg);
    }
}

#[test]
#[serial]
fn connection_test_start_with_unreachable_zookeeper() {

    // TODO exit cleanly in this function

    let (tx, rx) = channel();

    let mut ctx = TestContext::default();
    ctx.setup_zk_nodes();

    let connect_string_resolver = ctx.connect_string.clone();
    let root_path_resolver = ctx.root_path.clone();

    let tx_clone = tx.clone();
    toggle_zookeeper(ZkStatus::Disabled);

    // We expect resolver not to connect at this point
    let resolver_thread = thread::spawn(move || {
        let mut resolver = ManateePrimaryResolver::new(connect_string_resolver,
            root_path_resolver, None);
        resolver.run(tx_clone);
    });

    // Wait for resolver to start up
    thread::sleep(Duration::from_secs(1));

    assert!(!util::resolver_connected(&mut ctx, &rx).unwrap(),
        "Resolver should not be connected (is ZooKeeper running, somehow?)");

    toggle_zookeeper(ZkStatus::Enabled);

    //
    // Wait the maximum possible amount of time that could elapse without the
    // resolver reconnecting, plus a little extra to be safe.
    //
    thread::sleep(RECONNECT_DELAY + TCP_CONNECT_TIMEOUT +
        Duration::from_secs(1));

    assert!(util::resolver_connected(&mut ctx, &rx).unwrap(),
        "Resolver should be connected to Zookeeper");

    ctx.teardown_zk_nodes();
    ctx.finalize();
}

#[test]
#[serial]
fn connection_test_reconnect_after_zk_hiccup() {

    // TODO exit cleanly in this function

    toggle_zookeeper(ZkStatus::Enabled);

    let (tx, rx) = channel();

    let mut ctx = TestContext::default();
    ctx.setup_zk_nodes();

    let connect_string_resolver = ctx.connect_string.clone();
    let root_path_resolver = ctx.root_path.clone();

    let tx_clone = tx.clone();

    let resolver_thread = thread::spawn(move || {
        let mut resolver = ManateePrimaryResolver::new(connect_string_resolver,
            root_path_resolver, None);
        resolver.run(tx_clone);
    });

    // Wait for resolver to start up
    thread::sleep(Duration::from_secs(1));

    assert!(util::resolver_connected(&mut ctx, &rx).unwrap(),
        "Resolver should be connected to Zookeeper");

    toggle_zookeeper(ZkStatus::Disabled);

    assert!(!util::resolver_connected(&mut ctx, &rx).unwrap(),
        "Resolver should not be connected (is ZooKeeper running, somehow?)");

    toggle_zookeeper(ZkStatus::Enabled);

    //
    // Wait the maximum possible amount of time that could elapse without the
    // resolver reconnecting, plus a little extra to be safe.
    //
    thread::sleep(RECONNECT_DELAY + TCP_CONNECT_TIMEOUT +
        Duration::from_secs(1));

    assert!(util::resolver_connected(&mut ctx, &rx).unwrap(),
        "Resolver should be connected to Zookeeper");

    ctx.teardown_zk_nodes();
    ctx.finalize();
}
