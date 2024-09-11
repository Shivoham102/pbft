use std::collections::HashMap;
use tokio::sync::Mutex;
use std::sync::Arc;
use serde_json::to_vec;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use anyhow::Result;
use crate::message::{Message, MessageType};

#[derive(Clone)]
pub struct Node {
    id: u64,
    is_leader: bool,
    node_list: Arc<Mutex<HashMap<u64, u64>>>, 
    // view: u64,
    // log: Vec<Message>,
}

impl Node {
    pub fn new(id: u64, is_leader: bool, node_list: Arc<Mutex<HashMap<u64, u64>>>) -> Self {
        Self {
            id,
            is_leader,           
            node_list
        }
    }

    //Function to initialize the node and make it listen to the network
    pub async fn start(&self) -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind(format!("127.0.0.1:{}", 5000 + self.id)).await?;
        println!("Node {} is listening on port {}", self.id, listener.local_addr()?.port());
        if self.is_leader {
            println!("Node {} is the leader", self.id);
        }

        loop {
            let (mut socket, _) = listener.accept().await?;
            let node_clone = self.clone();
            tokio::spawn(async move {
                node_clone.handle_connection(&mut socket).await;
            });
        }
    }

    pub async fn multicast(&self, msg: Message, nodes: &HashMap<u64, String>) -> Result<(), Box<dyn std::error::Error>> {
        let serialized_msg = serde_json::to_string(&msg)?;

        for (node_id, addr) in nodes.iter() {
            if *node_id != self.id { // Don't send to self
                let mut stream = TcpStream::connect(addr).await?;
                stream.write_all(serialized_msg.as_bytes()).await?;
                println!("Node {} sent a {:?} message to Node {}", self.id, msg.msg_type, node_id);
            }
        }
        Ok(())
    }

    //Function to handle incoming messages and take action
    async fn handle_connection(&self, socket: &mut TcpStream) {

        // Buffer to store the incoming data
        let mut buf = [0; 1024];
        
        // Read the data from the socket
        match socket.read(&mut buf).await {
            Ok(size) => {
                if size == 0 {
                    return; // Connection was closed
                }

                // Deserialize the message
                let msg: Message = serde_json::from_slice(&buf[0..size]).expect("Failed to deserialize message");
                println!("Node {} received a message: {:?}", self.id, msg);


                // Process the message based on its type
                match &msg.msg_type {

                    MessageType::Request {o, t} => {

                        // Compute the digest of the message
                        let digest = msg.compute_digest();
                        println!("Node {} computed digest: {}", self.id, digest);

                        // Send PrePrepare after computing digest
                        let _ = self.pre_prepare(12, 1, digest.clone(), msg.clone()).await;
                    },
                    MessageType::PrePrepare {v, n, d, m} => {
                        // Handle PrePrepare message
                    },
                    MessageType::Prepare {v, n, d, i} => {
                        // Handle Prepare message
                    },
                    MessageType::Commit {v, n, d, i} => {
                        // Handle Commit message
                    },
                    MessageType::Reply {v, t, i, r} => {
                        // Handle Reply message
                    },
                }
            }
            Err(e) => {
                println!("Failed to read from socket; err = {:?}", e);
            }
        }
    
    }

    async fn pre_prepare(&self, v: u64, n: u64, d: String, m: Message) -> Result<()> {

        // Create the proper Pre-Prepare Message
        let pre_prepare_msg = Message::new(
            MessageType::PrePrepare { 
                v: v,
                n: n,
                d: d,
                m: Box::new(m)
            },
            self.id
        );

        //Serialize the message to prepare it for sending
        let serialized_msg = to_vec(&pre_prepare_msg)?;
        let node_list = self.node_list.lock().await;

        //Multicast <<PRE-PREPARE, v, n, d>, m> to all other replicas
        for (node_id, port) in node_list.iter() {
            if *node_id != self.id {
                let address = format!("127.0.0.1:{}", port);
                let mut stream = TcpStream::connect(address).await?;
                stream.write_all(&serialized_msg).await?;
                println!("Node {} sent a Pre-Prepare message to Node {}", self.id, node_id);
            }
        }

        Ok(())

    }
}
