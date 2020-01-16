use std::sync::mpsc::channel;
use std::thread;
use std::time::Duration;

use cueball::resolver::Resolver;

use cueball_manatee_primary_resolver::{
    ManateePrimaryResolver,
    WATCH_LOOP_DELAY
};
use cueball_manatee_primary_resolver::common::{util, test_data};
use util::{TestContext, TestAction};

#[test]
fn watch_test_nonexistent_node() {
    TestContext::default().run_func_and_finalize(|ctx| {
        // Delete the test nodes
        ctx.teardown_zk_nodes()?;
        let (tx, rx) = channel();

        let connect_string_resolver = ctx.connect_string.clone();
        let root_path_resolver = ctx.root_path.clone();

        // Start the resolver
        let log = util::log_from_env(util::DEFAULT_LOG_LEVEL).unwrap();
        thread::spawn(move || {
            let mut resolver = ManateePrimaryResolver::new(
                connect_string_resolver, root_path_resolver, Some(log));
            resolver.run(tx);
        });

        // Wait for resolver to start up and discover that the node is missing
        thread::sleep(Duration::from_secs(1));

        // Re-create the nodes
        ctx.setup_zk_nodes()?;

        // Wait for resolver to notice that the node now exists
        thread::sleep(WATCH_LOOP_DELAY);

        // Run a basic test case to make sure all is well
        let data_1 = test_data::backend_ip1_port1();
        let data_2 = test_data::backend_ip2_port2();
        ctx.run_test_case(TestAction {
            start_data: data_1.raw_vec(),
            end_data: data_2.raw_vec(),
            added_backend: Some(data_2.added_msg()),
            removed_backend: Some(data_1.removed_msg())
        }, Some(&rx))
    });
}

#[test]
fn watch_test_disappearing_node() {
    TestContext::default().run_func_and_finalize(|ctx| {
        let (tx, rx) = channel();

        let connect_string_resolver = ctx.connect_string.clone();
        let root_path_resolver = ctx.root_path.clone();

        // Start the resolver
        let log = util::log_from_env(util::DEFAULT_LOG_LEVEL).unwrap();
        thread::spawn(move || {
            let mut resolver = ManateePrimaryResolver::new(
                connect_string_resolver, root_path_resolver, Some(log));
            resolver.run(tx);
        });

        // Wait for resolver to start up
        thread::sleep(Duration::from_secs(1));

        // Run a basic test case to make sure all is well
        let data_1 = test_data::backend_ip1_port1();
        let data_2 = test_data::backend_ip2_port2();
        ctx.run_test_case(TestAction {
            start_data: data_1.raw_vec(),
            end_data: data_2.raw_vec(),
            added_backend: Some(data_2.added_msg()),
            removed_backend: Some(data_1.removed_msg())
        }, Some(&rx))?;

        // Delete the test nodes, causing the resolver to drop its watch
        ctx.teardown_zk_nodes()?;

        // Re-create the nodes
        ctx.setup_zk_nodes()?;

        // Wait for resolver to notice that the nodes were recreated
        thread::sleep(WATCH_LOOP_DELAY);
        println!("heeehaw");
        // Run the test case again
        ctx.run_test_case(TestAction {
            start_data: data_1.raw_vec(),
            end_data: data_2.raw_vec(),
            added_backend: Some(data_2.added_msg()),
            removed_backend: Some(data_1.removed_msg())
        }, Some(&rx))
    });
}

#[test]
fn watch_test_port_ip_change() {
    let data_1 = test_data::backend_ip1_port1();
    let data_2 = test_data::backend_ip2_port2();

    TestContext::default().run_action_and_finalize(TestAction {
        start_data: data_1.raw_vec(),
        end_data: data_2.raw_vec(),
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
        added_backend: Some(data_2.added_msg()),
        removed_backend: Some(data_1.removed_msg())
    });
}

#[test]
fn watch_test_no_change() {
    TestContext::default().run_action_and_finalize(TestAction {
        start_data: test_data::backend_ip1_port1().raw_vec(),
        end_data: test_data::backend_ip1_port1().raw_vec(),
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
        added_backend: None,
        removed_backend: None
    });
}
