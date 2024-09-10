use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use crate::message::{Message, MessageType};

#[derive(Clone)]
pub struct Node {
    id: u64,
    is_leader: bool,
    port: u64,
    node_list: Arc<Mutex<HashMap<u64, u64>>>, 
    // view: u64,
    // log: Vec<Message>,
}

impl Node {
    pub fn new(id: u64, is_leader: bool, port: u64, node_list: Arc<Mutex<HashMap<u64, u64>>>) -> Self {
        Self {
            id,
            is_leader,
            port,
            node_list
        }
    }

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

    async fn handle_connection(&self, socket: &mut TcpStream) {
        // Handle incoming connections
        println!("Handling connection for Node {}", self.id);

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
                println!("Node {} received a {:?} message: {:?}", self.id, msg.msg_type, msg);

                // Process the message based on its type
                match msg.msg_type {
                    MessageType::PrePrepare => {
                        // Handle PrePrepare message
                    },
                    MessageType::Prepare => {
                        // Handle Prepare message
                    },
                    MessageType::Commit => {
                        // Handle Commit message
                    },
                    MessageType::Reply => {
                        // Handle Reply message
                    },
                }
            }
            Err(e) => {
                println!("Failed to read from socket; err = {:?}", e);
            }
        }
    
    }
}
