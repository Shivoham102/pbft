mod node;
mod client;
mod message;

use std::process::{self, exit};
use std::{collections::HashMap, io};
use message::MessageType;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use std::sync::Arc;
use tokio::runtime::Runtime;
use node::{LogEntry, Node};
use client::Client;

fn main() {
    println!("PBFT Simulator Started\n");

    //Create a seperate async runtime as main() cannot be async
    let rt = Runtime::new().unwrap();
    rt.block_on(run());
}

async fn run() {
    
    //Get input from user
    let mut input: String = String::new();

    println!("Enter the value of total number of nodes n");
    io::stdin().read_line(&mut input).expect("Failed to read (n)");
    let n:u64 = input.trim().parse().expect("Please enter a valid integer");

    input.clear();

    println!("Enter the value of total number of byzantine nodes (f)");
    io::stdin().read_line(&mut input).expect("Failed to read f");
    let f:u64 = input.trim().parse().expect("Please enter a valid integer");

    if n < 3 * f + 1 {
        eprintln!("Exiting execution because n < 3 * f + 1");
        process::exit(1);
    }
    
    //Initialize data structures used by nodes to perform actions
    let node_list: Arc<Mutex<HashMap<u64, u64>>> = Arc::new(Mutex::new(HashMap::new()));
    let log: Arc<RwLock<HashMap<(String, u64), LogEntry>>> = Arc::new(RwLock::new(HashMap::new()));
    let prepare_count: Arc<RwLock<HashMap<u64, u64>>> =  Arc::new(RwLock::new(HashMap::new()));
    let commit_count: Arc<RwLock<HashMap<u64, u64>>> =  Arc::new(RwLock::new(HashMap::new()));

    //Create a mapping of nodes and their ports to use in multicasting later
    let base_port = 5000;
    for i in 0..n {
        let port = base_port + i;
        let mut node_list_lock = node_list.lock().await;
        node_list_lock.insert(i, port);
    }


    let v = 1;
    let p = v % n;

    //Spawn the replicas
    for i in 0..n { 
        let node = Node::new(i, if i == p {true} else {false}, Arc::clone(&node_list), Arc::clone(&log), v, f, Arc::clone(&prepare_count), Arc::clone(&commit_count));
        tokio::spawn(async move {
            node.start().await.expect("Failed to start node");
        }); 
    }

    //Spawn the client and give it the leader's port for communication
    match node_list.lock().await.get(&p) {
        
        Some(&leader_port) => {
            let client = Client::new(100,  leader_port);
            tokio::spawn(async move {
                if let Err(e) = client.start().await {
                    eprintln!("Error starting client: {}", e);
                }
            });
        }

        None => {
            println!("Leader not found in node list");
        }
    }
    

    tokio::signal::ctrl_c().await.expect("Failed to listen for ctrl-c");
    println!("Shutting down PBFT simulator");

    
    
}
