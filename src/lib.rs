/*
 * Copyright 2019 Joyent, Inc.
 */

use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use std::sync::mpsc::Sender;
use std::thread;
use std::time::Duration;

use itertools::Itertools;
use zookeeper::{
    KeeperState,
    Watcher,
    WatchedEvent,
    WatchedEventType,
    ZooKeeper
};

use cueball::backend::*;
use cueball::resolver::{BackendAddedMsg, BackendMsg, Resolver};
use cueball::error::Error;


#[derive(Debug, Clone, PartialEq)]
pub struct ZKConnectString(Vec<SocketAddr>);

impl ToString for ZKConnectString {
    fn to_string(&self) -> String {
        self
            .0
            .iter()
            .map(|x| x.to_string())
            .intersperse(String::from(","))
            .collect()
    }
}

impl FromStr for ZKConnectString {
    type Err = std::net::AddrParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let acc: Result<Vec<SocketAddr>, Self::Err> = Ok(vec![]);
        s.split(',')
            .map(|x| SocketAddr::from_str(x))
            .fold(acc, |acc, x| {
                match (acc, x) {
                    (Ok(mut addrs), Ok(addr)) => {
                        addrs.push(addr);
                        Ok(addrs)
                    },
                    (Err(e), _) => Err(e),
                    (_, Err(e)) => Err(e)
                }
            })
            .and_then(|x| Ok(ZKConnectString(x)))
    }
}

pub struct ManateePrimaryResolver {
    /// An instance of the ZKConnectString type that represents a Zookeeper
    /// connection string
    connect_string: ZKConnectString,
    /// The Zookeeper path for primary leader election for the shard. *e.g.*
    /// /manatee/1.moray.coal.joyent.us/election
    election_path: String,
    /// The Zookeeper path for manatee cluster state for the shard. *e.g.*
    /// /manatee/1.moray.coal.joyent.us/state
    cluster_state_path: String,
    /// The communications channel with the cueball connection pool
    pool_tx: Option<Sender<BackendMsg>>,
    /// The last error that occurred
    error: Option<Error>,
    /// Indicates if the pool is in the started state
    started: bool
}

impl ManateePrimaryResolver {
    pub fn new(
        connect_string: ZKConnectString,
        path: String
    ) -> Self
    {
        let election_path = [&path, "/election"].concat();
        let cluster_state_path = [&path, "/state"].concat();

        ManateePrimaryResolver {
            connect_string,
            election_path,
            cluster_state_path,
            pool_tx: None,
            error: None,
            started: false
        }
    }
}

fn zookeeper_manager(
    connect_string: ZKConnectString,
    pool_tx: Sender<BackendMsg>
) {
    let mut run = true;

    while run {
        // Create a channel to communicate with the session manager thread

        // Spawn session manager thread

        // Receive on channel
    }


}

impl Resolver for ManateePrimaryResolver {
    fn start(&mut self, s: Sender<BackendMsg>) {
        if !self.started {
            // Spawn a thread to manage the zookeeper connection and possible
            // failures. This thread will in turn spawn a thread to manage a
            // Zookeeper session. If the session expires then the session
            // manager thread will notify the zookeeper manager thread and
            // exit. The manager thread will then create a new Zookeeper session
            // thread. The Zookeeper session thread is responsible for setting
            // the watches on the manatee cluster state and the manatee primary
            // election.

            let zk_connect_string = self.connect_string.clone();
            let s_clone = s.clone();
            let zk_mgr_thread = thread::spawn(move || zookeeper_manager(zk_connect_string, s_clone));

            let sew = SessionEventWatcher(true);
            // let zk = ZooKeeper::connect(&self.connect_string.to_string(),
            //                             Duration::from_secs(30),
            //                             sew)
            //     .unwrap();

            // let watch_state =
            //     zk.get_data_w(&self.cluster_state_path, ClusterStateWatcher);

            // let watch_election =
            //     zk.get_children_w(&self.election_path, ElectionWatcher);

            // self.zookeeper = Some(zk);
            self.pool_tx = Some(s);
            self.started = true;
        }
    }

    fn stop(&mut self) {
        self.started = false;
    }

    fn get_last_error(&self) -> Option<String> {
        if let Some(err) = &self.error {
                let err_str = format!("{}", err);
                Some(err_str)
        } else {
            None
        }
    }
}


struct SessionEventWatcher(bool);

impl Watcher for SessionEventWatcher {
    fn handle(&self, e: WatchedEvent) {
        // Only handle KeeperState changes
        if let WatchedEventType::None = e.event_type {
            match e.keeper_state {
                KeeperState::Disconnected => (),
                KeeperState::SyncConnected => (),
                KeeperState::AuthFailed => (),
                KeeperState::ConnectedReadOnly => (),
                KeeperState::SaslAuthenticated => (),
                KeeperState::Expired => ()
            }
        }
    }
}

struct ElectionWatcher;

impl Watcher for ElectionWatcher {
    fn handle(&self, e: WatchedEvent) {
        println!("{:?}", e)
    }
}

struct ClusterStateWatcher;

impl Watcher for ClusterStateWatcher {
    fn handle(&self, e: WatchedEvent) {
        println!("{:?}", e)
    }
}


#[cfg(test)]
mod test {
    use super::*;

    use std::iter;

    use quickcheck::{quickcheck, Arbitrary, Gen};
    use rand::Rng;

    impl Arbitrary for ZKConnectString {
        fn arbitrary<G: Gen>(g: &mut G) -> Self {
            let size = usize::arbitrary(g);
            ZKConnectString(
                iter::repeat(())
                    .map(|()| SocketAddr::arbitrary(g))
                    .take(size)
                    .collect()
            )
        }
    }

    quickcheck! {
        fn prop_zk_connect_string_parse(
            connect_string: ZKConnectString
        ) -> bool
        {
            match ZKConnectString::from_str(&connect_string.to_string()) {
                Ok(cs) => cs == connect_string,
                _ => false
            }
        }
    }
 }
