// Copyright 2019 Joyent, Inc.

use cueball::resolver::{
    BackendMsg,
};

fn msgs_equal(a: &BackendMsg, b: &BackendMsg) -> bool {
    match (a, b) {
        (BackendMsg::AddedMsg(a), BackendMsg::AddedMsg(b)) => {
            a.key == b.key
        },
        (BackendMsg::RemovedMsg(a), BackendMsg::RemovedMsg(b)) => {
            a.0 == b.0
        },
        (BackendMsg::StopMsg, BackendMsg::StopMsg) => {
            true
        },
        (BackendMsg::HeartbeatMsg, BackendMsg::HeartbeatMsg) => {
            true
        },
        _ => false
    }

}

pub fn find_msg_match(list: &Vec<BackendMsg>, to_find: &BackendMsg)
    -> Option<usize> {
    for (index, item) in list.iter().enumerate() {
        if msgs_equal(item, to_find) {
            return Some(index);
        }
    }
    None
}
