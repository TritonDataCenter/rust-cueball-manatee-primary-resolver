use std::env;
use std::fmt::{Debug, Display};
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
    ZkConnectString
};

use std::iter;
use std::panic;
use std::sync::mpsc::channel;

use tokio_zookeeper::{Acl, CreateMode};
use uuid::Uuid;

mod util;
mod test_data;

use util::{TestRunner, TestAction, run_test_case};

fn run_test(input: TestAction) {
    let conn_str = ZkConnectString::from_str(
         "127.0.0.1:2181").unwrap();
    let root_path = "/test-".to_string() + &Uuid::new_v4().to_string();
    let runner = TestRunner::new(
        conn_str,
        root_path
    );

    run_test_case(runner, input);
}
    // TODO arg-parsing
    // let matches = App::new("Manatee Primary Resolver Integration Test Suite")
    //     .arg(Arg::with_name("zookeeper connection string")
    //              .short("z")
    //              .long("zookeeper")
    //              .takes_value(true)
    //              .help("A comma-separated list of zookeeper IP addresses"))
    //     .get_matches();

    // let conn_str = match matches.value_of("zookeeper connection string") {
    //     None => panic!("Error: Connection string not supplied"),
    //     Some(conn_str) => {
    //         match ZkConnectString::from_str(conn_str) {
    //             Error(e) => panic!("Error: connection string malformed: {:?}", e),
    //             Ok(conn_str) => conn_str
    //         }
    //     }
    // };

#[test]
fn port_ip_change_test() {
    let data_1 = test_data::backend_ip1_port1();
    let data_2 = test_data::backend_ip2_port2();

    run_test(TestAction {
        start_data: data_1.raw_vec(),
        end_data: data_2.raw_vec(),
        added_start_backend: Some(data_1.added_msg()),
        added_backend: Some(data_2.added_msg()),
        removed_backend: Some(data_1.removed_msg())
    });
}

#[test]
fn ip_change_test() {
    let data_1 = test_data::backend_ip1_port1();
    let data_2 = test_data::backend_ip2_port1();

    run_test(TestAction {
        start_data: data_1.raw_vec(),
        end_data: data_2.raw_vec(),
        added_start_backend: Some(data_1.added_msg()),
        added_backend: Some(data_2.added_msg()),
        removed_backend: Some(data_1.removed_msg())
    });
}

#[test]
fn port_change_test() {
    let data_1 = test_data::backend_ip1_port1();
    let data_2 = test_data::backend_ip1_port2();

    run_test(TestAction {
        start_data: data_1.raw_vec(),
        end_data: data_2.raw_vec(),
        added_start_backend: Some(data_1.added_msg()),
        added_backend: Some(data_2.added_msg()),
        removed_backend: Some(data_1.removed_msg())
    });
}

#[test]
fn no_change_test() {
    run_test(TestAction {
        start_data: test_data::backend_ip1_port1().raw_vec(),
        end_data: test_data::backend_ip1_port1().raw_vec(),
        added_start_backend: Some(test_data::backend_ip1_port1().added_msg()),
        added_backend: None,
        removed_backend: None
    });
}

#[test]
fn invalid_to_valid_test() {
    let data = test_data::backend_ip1_port1();

    run_test(TestAction {
        start_data: test_data::invalid_json_vec(),
        end_data: data.raw_vec(),
        added_start_backend: None,
        added_backend: Some(data.added_msg()),
        removed_backend: None
    });
}

#[test]
fn invalid_to_invalid_test() {
    //
    // It doesn't really matter what the start and end data are, as long as
    // they're different from each other and both invalid
    //
    run_test(TestAction {
        start_data: test_data::invalid_json_vec(),
        end_data: test_data::no_ip_vec(),
        added_start_backend: None,
        added_backend: None,
        removed_backend: None
    });
}

#[test]
fn valid_to_no_ip_test() {
    let data = test_data::backend_ip1_port1();

    run_test(TestAction {
        start_data: data.raw_vec(),
        end_data: test_data::no_ip_vec(),
        added_start_backend: Some(data.added_msg()),
        added_backend: None,
        removed_backend: None
    });
}

#[test]
fn valid_to_invalid_ip_test() {
    let data = test_data::backend_ip1_port1();

    run_test(TestAction {
        start_data: data.raw_vec(),
        end_data: test_data::invalid_ip_vec(),
        added_start_backend: Some(data.added_msg()),
        added_backend: None,
        removed_backend: None
    });
}

#[test]
fn valid_to_wrong_type_ip_test() {
    let data = test_data::backend_ip1_port1();

    run_test(TestAction {
        start_data: data.raw_vec(),
        end_data: test_data::wrong_type_ip_vec(),
        added_start_backend: Some(data.added_msg()),
        added_backend: None,
        removed_backend: None
    });
}

#[test]
fn valid_to_no_pg_url_test() {
    let data = test_data::backend_ip1_port1();

    run_test(TestAction {
        start_data: data.raw_vec(),
        end_data: test_data::no_pg_url_vec(),
        added_start_backend: Some(data.added_msg()),
        added_backend: None,
        removed_backend: None
    });
}

#[test]
fn valid_to_invalid_pg_url_test() {
    let data = test_data::backend_ip1_port1();

    run_test(TestAction {
        start_data: data.raw_vec(),
        end_data: test_data::invalid_pg_url_vec(),
        added_start_backend: Some(data.added_msg()),
        added_backend: None,
        removed_backend: None
    });
}

#[test]
fn valid_to_wrong_type_pg_url_test() {
    let data = test_data::backend_ip1_port1();

    run_test(TestAction {
        start_data: data.raw_vec(),
        end_data: test_data::wrong_type_pg_url_vec(),
        added_start_backend: Some(data.added_msg()),
        added_backend: None,
        removed_backend: None
    });
}

#[test]
fn valid_to_no_port_pg_url_test() {
    let data = test_data::backend_ip1_port1();

    run_test(TestAction {
        start_data: data.raw_vec(),
        end_data: test_data::no_port_pg_url_vec(),
        added_start_backend: Some(data.added_msg()),
        added_backend: None,
        removed_backend: None
    });
}

#[test]
fn valid_to_invalid_json_test() {
    let data = test_data::backend_ip1_port1();

    run_test(TestAction {
        start_data: data.raw_vec(),
        end_data: test_data::invalid_json_vec(),
        added_start_backend: Some(data.added_msg()),
        added_backend: None,
        removed_backend: None
    });
}
