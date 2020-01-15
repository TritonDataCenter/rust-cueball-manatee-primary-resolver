
use cueball_manatee_primary_resolver::common::{util, test_data};
use util::{TestContext, TestAction};

// fn watch_test_nonexistent_node() {
//     let (tx, rx) = channel();

// TestContext::default().    let mut ctx =
//     let connect_string_resolver = ctx.connect_string.clone();
//     let root_path_resolver = ctx.root_path.clone();

//    let log = util::log_from_env(util::DEFAULT_LOG_LEVEL).unwrap();
//     let resolver_thread = thread::spawn(move || {
//         let mut resolver = ManateePrimaryResolver::new(connect_string_resolver,
//             root_path_resolver, Some(log));
//         resolver.run(tx);
//     });

//     // Wait for resolver to start up
//     thread::sleep(Duration::from_secs(1));

//     //
//     // The watched node doesn't exist yet -- so verify that the resolver doesn't
//     // send any messages
//     //
//     if let Ok(_) =
//         recv_timeout_discard_heartbeats(&rx, Duration::from_secs(2)) {
//         clean_exit!(ctx,
//             "ZK node does not exist, so resolver should not send any messages");
//     }

//     ctx.setup_zk_nodes();

//     assert!(!util::resolver_connected(&mut ctx, &rx),
//         "Resolver should not be connected (is ZooKeeper running, somehow?)");

//     let channel_result =
//             recv_timeout_discard_heartbeats(&rx, Duration::from_secs(2));
//         match channel_result {
//             Err(e) => clean_exit!(ctx, "Unexpected error receiving on channel: {:?}", e),
//             Ok(m) => {
//                 if m != BackendMsg::AddedMsg(msg) {
//                     clean_exit!(ctx, "Message received does not match expected added_start_backend message")
//                 }
//             }
//         }

//     //
//     // Wait for the resolver to try getting the data again, plus a little extra
//     // to be safe.
//     //
//     thread::sleep(WATCH_LOOP_DELAY + Duration::from_secs(1));

//     assert!(util::resolver_connected(&mut ctx, &rx),
//         "Resolver should be connected to Zookeeper");

//     ctx.teardown_zk_nodes();
//     ctx.finalize();
// }

#[test]
fn watch_test_port_ip_change() {
    let data_1 = test_data::backend_ip1_port1();
    let data_2 = test_data::backend_ip2_port2();

    TestContext::default().run_action_and_finalize(TestAction {
        start_data: data_1.raw_vec(),
        end_data: data_2.raw_vec(),
        added_start_backend: Some(data_1.added_msg()),
        added_backend: Some(data_2.added_msg()),
        removed_backend: Some(data_1.removed_msg())
    });
}

#[test]
fn watch_test_ip_change() {
    let data_1 = test_data::backend_ip1_port1();
    let data_2 = test_data::backend_ip2_port1();

    TestContext::default().run_action_and_finalize(TestAction {
        start_data: data_1.raw_vec(),
        end_data: data_2.raw_vec(),
        added_start_backend: Some(data_1.added_msg()),
        added_backend: Some(data_2.added_msg()),
        removed_backend: Some(data_1.removed_msg())
    });
}

#[test]
fn watch_test_port_change() {
    let data_1 = test_data::backend_ip1_port1();
    let data_2 = test_data::backend_ip1_port2();

    TestContext::default().run_action_and_finalize(TestAction {
        start_data: data_1.raw_vec(),
        end_data: data_2.raw_vec(),
        added_start_backend: Some(data_1.added_msg()),
        added_backend: Some(data_2.added_msg()),
        removed_backend: Some(data_1.removed_msg())
    });
}

#[test]
fn watch_test_no_change() {
    TestContext::default().run_action_and_finalize(TestAction {
        start_data: test_data::backend_ip1_port1().raw_vec(),
        end_data: test_data::backend_ip1_port1().raw_vec(),
        added_start_backend: Some(test_data::backend_ip1_port1().added_msg()),
        added_backend: None,
        removed_backend: None
    });
}

#[test]
fn watch_test_invalid_to_valid() {
    let data = test_data::backend_ip1_port1();

    TestContext::default().run_action_and_finalize(TestAction {
        start_data: test_data::invalid_json_vec(),
        end_data: data.raw_vec(),
        added_start_backend: None,
        added_backend: Some(data.added_msg()),
        removed_backend: None
    });
}

#[test]
fn watch_test_invalid_to_invalid() {
    //
    // It doesn't really matter what the start and end data are, as long as
    // they're different from each other and both invalid
    //
    TestContext::default().run_action_and_finalize(TestAction {
        start_data: test_data::invalid_json_vec(),
        end_data: test_data::no_ip_vec(),
        added_start_backend: None,
        added_backend: None,
        removed_backend: None
    });
}

#[test]
fn watch_test_valid_to_no_ip() {
    let data = test_data::backend_ip1_port1();

    TestContext::default().run_action_and_finalize(TestAction {
        start_data: data.raw_vec(),
        end_data: test_data::no_ip_vec(),
        added_start_backend: Some(data.added_msg()),
        added_backend: None,
        removed_backend: None
    });
}

#[test]
fn watch_test_valid_to_invalid_ip() {
    let data = test_data::backend_ip1_port1();

    TestContext::default().run_action_and_finalize(TestAction {
        start_data: data.raw_vec(),
        end_data: test_data::invalid_ip_vec(),
        added_start_backend: Some(data.added_msg()),
        added_backend: None,
        removed_backend: None
    });
}

#[test]
fn watch_test_valid_to_wrong_type_ip() {
    let data = test_data::backend_ip1_port1();

    TestContext::default().run_action_and_finalize(TestAction {
        start_data: data.raw_vec(),
        end_data: test_data::wrong_type_ip_vec(),
        added_start_backend: Some(data.added_msg()),
        added_backend: None,
        removed_backend: None
    });
}

#[test]
fn watch_test_valid_to_no_pg_url() {
    let data = test_data::backend_ip1_port1();

    TestContext::default().run_action_and_finalize(TestAction {
        start_data: data.raw_vec(),
        end_data: test_data::no_pg_url_vec(),
        added_start_backend: Some(data.added_msg()),
        added_backend: None,
        removed_backend: None
    });
}

#[test]
fn watch_test_valid_to_invalid_pg_url() {
    let data = test_data::backend_ip1_port1();

    TestContext::default().run_action_and_finalize(TestAction {
        start_data: data.raw_vec(),
        end_data: test_data::invalid_pg_url_vec(),
        added_start_backend: Some(data.added_msg()),
        added_backend: None,
        removed_backend: None
    });
}

#[test]
fn watch_test_valid_to_wrong_type_pg_url() {
    let data = test_data::backend_ip1_port1();

    TestContext::default().run_action_and_finalize(TestAction {
        start_data: data.raw_vec(),
        end_data: test_data::wrong_type_pg_url_vec(),
        added_start_backend: Some(data.added_msg()),
        added_backend: None,
        removed_backend: None
    });
}

#[test]
fn watch_test_valid_to_no_port_pg_url() {
    let data = test_data::backend_ip1_port1();

    TestContext::default().run_action_and_finalize(TestAction {
        start_data: data.raw_vec(),
        end_data: test_data::no_port_pg_url_vec(),
        added_start_backend: Some(data.added_msg()),
        added_backend: None,
        removed_backend: None
    });
}

#[test]
fn watch_test_valid_to_invalid_json() {
    let data = test_data::backend_ip1_port1();

    TestContext::default().run_action_and_finalize(TestAction {
        start_data: data.raw_vec(),
        end_data: test_data::invalid_json_vec(),
        added_start_backend: Some(data.added_msg()),
        added_backend: None,
        removed_backend: None
    });
}
