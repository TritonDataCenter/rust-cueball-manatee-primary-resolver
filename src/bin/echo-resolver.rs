//
// Copyright 2019 Joyent, Inc.
//

//
// Runs a manatee primary resolver and prints its output on stdout. Requires a
// running ZooKeeper instance to connect to.
//
// Manually edit the ZooKeeper node data and watch the changes come through!
//
// The -l argument is useful for controlling the amount of log spam. Set it to
// "critical" to see only the data that a client would.
//

use std::fmt::Debug;
use std::process::exit;
use std::sync::Mutex;
use std::sync::mpsc::channel;
use std::thread;

use clap::{Arg, App, crate_name, crate_version};
use cueball::resolver::{
    BackendAddedMsg,
    BackendRemovedMsg,
    BackendMsg,
    Resolver
};
use slog::{Drain, Level, LevelFilter, Logger, o};

use cueball_manatee_primary_resolver::{
    ManateePrimaryResolver,
    ZkConnectString
};

// TODO remove this once done testing
#[path = "../../tests/util.rs"]
mod util;
use util::{TestRunner};

fn parse_log_level(s: String) -> Result<Level, String> {
    match s.as_str() {
       "trace" => Ok(Level::Trace),
       "debug" => Ok(Level::Debug),
       "info" => Ok(Level::Info),
       "warning" => Ok(Level::Warning),
       "error" => Ok(Level::Error),
       "critical" => Ok(Level::Critical),
       _ => Err("Invalid log level".to_string())
    }
}

fn main() {
    let matches = App::new("Manatee Primary Resolver CLI")
        .version("0.1.0")
        .author("Isaac Davis <isaac.davis@joyent.com>")
        .about("Echoes resolver output to command line")
        .arg(Arg::with_name("connect string")
                 .short("c")
                 .long("connect-string")
                 .takes_value(true)
                 .help("Comma-separated list of ZooKeeper address:port pairs"))
        .arg(Arg::with_name("root path")
                 .short("p")
                 .long("root-path")
                 .takes_value(true)
                 .help("Root path of manatee cluster in ZooKeeper \
                    (NOT including '/state' node)"))
        .arg(Arg::with_name("log level")
                 .short("l")
                 .long("log-level")
                 .takes_value(true)
                 .help("Log level: trace|debug|info|warning|error|critical"))
        .get_matches();

    let s = matches.value_of("connect string").unwrap_or("127.0.0.1:2181")
        .parse::<ZkConnectString>().unwrap();
    let p = matches.value_of("root path")
        .unwrap_or("/manatee/1.boray.virtual.example.com").parse::<String>()
        .unwrap();
    let l = parse_log_level(
        matches.value_of("log level").unwrap_or("info").to_string()).unwrap();

    std::process::exit(
        match run(s, p, l) {
            Ok(_) => 0,
            Err(err) => {
                eprintln!("error: {:?}", err);
                1
            }
        }
    );
}

fn run(s: ZkConnectString, p: String, l: Level) -> Result<(), String> {
    let log = Logger::root(
        Mutex::new(LevelFilter::new(
            slog_bunyan::with_name(crate_name!(),
                std::io::stdout()).build(),
            l)).fuse(),
            o!("build-id" => crate_version!()));

    let (tx, rx) = channel();
    let mut ctx = TestRunner::new(s.clone(), p.clone());

    let resolver_thread = thread::spawn(move || {
        let mut resolver = ManateePrimaryResolver::new(s,
            p, Some(log));
        resolver.run(tx);
    });


    println!("{:?}", util::resolver_connected(&mut ctx, &rx));

    loop {
        match rx.recv() {
            Ok(msg) => {
                match msg {
                    BackendMsg::AddedMsg(msg) => println!("Added msg: {:?}", msg.key),
                    BackendMsg::RemovedMsg(msg) => println!("Removed msg: {:?}", msg.0),
                    BackendMsg::StopMsg => {
                        println!("Stop msg");
                        break;
                    },
                    BackendMsg::HeartbeatMsg => println!("Heartbeat msg")
                }
            },
            Err(e) => {
                return Err(format!("Error receiving on channel: {:?}", e));
            }
        }
    }

    Ok(())
}
