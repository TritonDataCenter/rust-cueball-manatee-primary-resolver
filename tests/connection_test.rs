use std::sync::mpsc::channel;
use std::thread;
use std::time::Duration;

use serial_test::serial;

use cueball::resolver::Resolver;

use cueball_manatee_primary_resolver::{
    ManateePrimaryResolver,
    RECONNECT_DELAY,
    TCP_CONNECT_TIMEOUT,
};
use cueball_manatee_primary_resolver::common::util::{
    self,
    TestContext,
    TestError,
    ZkStatus
};

#[test]
#[serial]
fn connection_test_start_with_unreachable_zookeeper() {
    TestContext::default().run_func_and_finalize(|mut ctx| {
        let (tx, rx) = channel();

        let connect_string_resolver = ctx.connect_string.clone();
        let root_path_resolver = ctx.root_path.clone();

        let tx_clone = tx.clone();
        util::toggle_zookeeper(ZkStatus::Disabled)?;

        // We expect resolver not to connect at this point
        let log = util::log_from_env(util::DEFAULT_LOG_LEVEL).unwrap();
        thread::spawn(move || {
            let mut resolver = ManateePrimaryResolver::new(connect_string_resolver,
                root_path_resolver, Some(log));
            resolver.run(tx_clone);
        });

        // Wait for resolver to start up
        thread::sleep(Duration::from_secs(1));

        let connected = util::resolver_connected(&mut ctx, &rx)?;
        if connected {
            return Err(TestError::IncorrectBehavior(
                "Resolver should not be connected \
                (is ZooKeeper running, somehow?)".to_string()));
        }

        util::toggle_zookeeper(ZkStatus::Enabled)?;

        //
        // Wait the maximum possible amount of time that could elapse without the
        // resolver reconnecting, plus a little extra to be safe.
        //
        thread::sleep(RECONNECT_DELAY + TCP_CONNECT_TIMEOUT +
            Duration::from_secs(1));

        let connected = util::resolver_connected(&mut ctx, &rx)?;
        if !connected {
            return Err(TestError::IncorrectBehavior(
                "Resolver should be connected to ZooKeeper".to_string()));
        }
        Ok(())
    });
}

#[test]
#[serial]
fn connection_test_reconnect_after_zk_hiccup() {
    TestContext::default().run_func_and_finalize(|mut ctx| {
        util::toggle_zookeeper(ZkStatus::Enabled)?;

        let (tx, rx) = channel();

        let connect_string_resolver = ctx.connect_string.clone();
        let root_path_resolver = ctx.root_path.clone();

        let tx_clone = tx.clone();

        let log = util::log_from_env(util::DEFAULT_LOG_LEVEL).unwrap();
        thread::spawn(move || {
            let mut resolver = ManateePrimaryResolver::new(connect_string_resolver,
                root_path_resolver, Some(log));
            resolver.run(tx_clone);
        });

        // Wait for resolver to start up
        thread::sleep(Duration::from_secs(1));

        let connected = util::resolver_connected(&mut ctx, &rx)?;
        if !connected {
            return Err(TestError::IncorrectBehavior(
                "Resolver should be connected to ZooKeeper".to_string()));
        }

        util::toggle_zookeeper(ZkStatus::Disabled)?;

        let connected = util::resolver_connected(&mut ctx, &rx)?;
        if connected {
            return Err(TestError::IncorrectBehavior(
                "Resolver should not be connected \
                (is ZooKeeper running, somehow?)".to_string()));
        }

        util::toggle_zookeeper(ZkStatus::Enabled)?;

        //
        // Wait the maximum possible amount of time that could elapse without the
        // resolver reconnecting, plus a little extra to be safe.
        //
        thread::sleep(RECONNECT_DELAY + TCP_CONNECT_TIMEOUT +
            Duration::from_secs(1));

        let connected = util::resolver_connected(&mut ctx, &rx)?;
        if !connected {
            return Err(TestError::IncorrectBehavior(
                "Resolver should be connected to ZooKeeper".to_string()));
        }
        Ok(())
    });
}
