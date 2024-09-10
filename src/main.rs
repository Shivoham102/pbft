mod node;
mod client;
mod message;

use std::{collections::HashMap, io};
use std::sync::{Arc, Mutex};
use tokio::runtime::Runtime;
use node::Node;
use client::Client;

fn main() {
    println!("PBFT Simulator Started\n");

    let rt = Runtime::new().unwrap();
    rt.block_on(run());
}

async fn run() {
    
    let mut input: String = String::new();

    println!("Enter the value of total number of nodes n");
    io::stdin().read_line(&mut input).expect("Failed to read (n)");
    let n:u64 = input.trim().parse().expect("Please enter a valid integer");

    input.clear();

    println!("Enter the value of total number of byzantine nodes (f)");
    io::stdin().read_line(&mut input).expect("Failed to read f");
    let f:u64 = input.trim().parse().expect("Please enter a valid integer");
    

    let node_list: Arc<Mutex<HashMap<u64, u64>>> = Arc::new(Mutex::new(HashMap::new()));
    let base_port = 5000;
    for i in 0..n {
        let port = base_port + i;
        let mut node_list_lock = node_list.lock().unwrap();
        node_list_lock.insert(i, port);
    }

    let v = 10;
    let p = v % n;

    for i in 0..n {
        if let Some(&port) = node_list.lock().unwrap().get(&i) {
            let node = Node::new(i, if i == p {true} else {false}, port, Arc::clone(&node_list));
            tokio::spawn(async move {
                node.start().await.expect("Failed to start node");
            });
        }
        else {
            println!("Node ID {} not found in the node list", i);
        }
    }

    let client = Client::new(0);
    tokio::spawn(async move {
        client.start().await.expect("Failed to start client");
    });

    tokio::signal::ctrl_c().await.expect("Failed to listen for ctrl-c");
    println!("Shutting down PBFT simulator");

    
    
}
