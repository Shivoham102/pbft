use std::collections::HashMap;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use std::sync::Arc;
use serde_json::to_vec;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use anyhow::Result;
use crate::message::{Message, MessageType};


#[derive(Debug, Clone)]
pub struct LogEntry {
    pub message: Message,
    pub digest: String,
}


#[derive(Clone)]
pub struct Node {
    id: u64,
    is_leader: bool,
    node_list: Arc<Mutex<HashMap<u64, u64>>>, 
    log: Arc<RwLock<HashMap<(String, u64), LogEntry>>>, //Message type and view numbers are keys for the HaskMap
    view_number: u64, //Tells the current view the node is in
    f: u64, //number of faulty nodes
    is_faulty: bool,
    prepare_counts: Arc<RwLock<HashMap<u64, u64>>>,    
    commit_counts: Arc<RwLock<HashMap<u64, u64>>>,    
}

impl Node {
    pub fn new(id: u64, is_leader: bool, node_list: Arc<Mutex<HashMap<u64, u64>>>, log: Arc<RwLock<HashMap<(String, u64), LogEntry>>>, view_number: u64, f: u64, is_faulty: bool, prepare_counts: Arc<RwLock<HashMap<u64, u64>>>, commit_counts: Arc<RwLock<HashMap<u64, u64>>>
) -> Self {
        Self {
            id,
            is_leader,
            node_list,
            log,
            view_number,
            f,
            is_faulty,
            prepare_counts,
            commit_counts
        }
    }

    //Function to initialize the node and make it listen to the network
    pub async fn start(&self) -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind(format!("127.0.0.1:{}", 5000 + self.id)).await?;
        println!("Node {} is listening on port {}", self.id, listener.local_addr()?.port());
        
        if self.is_faulty {
            println!("Node {} is faulty and will ignore messages", self.id);
        }

        if self.is_leader {
            println!("Node {} is the primary", self.id);
        }

        loop {
            let (mut socket, _) = listener.accept().await?;
            let node_clone = self.clone();
            tokio::spawn(async move {
                node_clone.handle_connection(&mut socket).await;
            });
        }
    }

    //Function to handle incoming messages and take action
    async fn handle_connection(&self, socket: &mut TcpStream) {

         // Simulating a faulty node by not processing any incoming messages
        if self.is_faulty {
            // println!("Faulty Node {} is ignoring the message.", self.id);
            return;
        }

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


                // Process the message based on its type
                match &msg.msg_type[..] {
                    
                    "Request" => {
                        // Compute the digest of the message
                        let digest = msg.compute_digest();
                        println!("\nPrimary Node {} computed digest of Request: {}", self.id, digest);
                        println!("Going into PrePrepare Phase\n");

                        // Send PrePrepare after computing digest
                        let _ = self.send_pre_prepare(self.view_number, 1, digest.clone(), msg.clone()).await;
                    },

                    "PrePrepare" => {                                                         
                        match &msg.msg_content {
                            MessageType::PrePrepare { v, n, d, m } => {
                                let computed_digest = m.compute_digest();

                                if d == &computed_digest && *v == self.view_number {

                                    // Clone the message to store in the log
                                    let log_entry = LogEntry {
                                        message: *m.clone(),  // Clone the message to store it in the log
                                        digest: d.clone(),
                                    };

                                    // Store the PrePrepare in the log
                                    let key = ("PrePrepare".to_string(), *v);

                                    {    
                                        let mut log_lock = self.log.write().await;
                                        log_lock.insert(key, log_entry);
                                    }

                                    // Send Prepare message                                 
                                    let _ = self.send_prepare(*v, *n, d.clone(), self.id).await;
                                } else {
                                    println!("Digest mismatch at Node {}! Expected: {}, Got: {}", self.id, d, computed_digest);
                                }
                                
                            }
                            _ => {
                                    println!("Unexpected message type in PrePrepare message!");
                                }
                        }

                      
                    },

                    "Prepare" => {
                        // println!("Node {} received Prepare msg", self.id);

                        match &msg.msg_content {
                            MessageType::Prepare { v, n, d, i: _ } => {

                                //Skip processing if already received at least 2f Prepare messages
                                {
                                    let prepare_counts = self.prepare_counts.read().await;
                                    if let Some(count) = prepare_counts.get(&*v) {
                                        if *count >= 2 * self.f + 1 {                                   
                                            return;
                                        }
                                    }
                                }
                            
                                let received_digest = d.clone();

                                // Key for the PrePrepare log entry
                                let key = ("PrePrepare".to_string(), self.view_number);

                                // Acquire the read lock for the log
                                let log_entry = {
                                    let log_lock = self.log.read().await;
                                    log_lock.get(&key).cloned() // Clone log entry only if it exists
                                };

                                // Check if the log entry exists
                                if let Some(log_entry) = log_entry {
                                    // Check digest match
                                    if log_entry.digest == received_digest && *v == self.view_number {
                                        // println!("Node {} approved Prepare msg from Node {}", self.id, msg.sender_id);

                                        // Prepare to write to the log
                                        let key = ("Prepare".to_string(), *v);
                                        let new_entry = LogEntry {
                                            message: msg.clone(),
                                            digest: log_entry.digest.clone(),
                                        };

                                        // Write to the log
                                        {
                                            let mut log_lock = self.log.write().await;
                                            log_lock.insert(key, new_entry);
                                        }

                                        // Send Commit message

                                         // Update prepare message count
                                        {
                                            let mut prepare_counts = self.prepare_counts.write().await;
                                            let count = prepare_counts.entry(*v).or_insert(0);
                                            *count += 1;

                                            
                                        }
                                        // Check if replica received enough Prepare messages
                                        {
                                            let prepare_counts = self.prepare_counts.read().await;
                                            if let Some(count) = prepare_counts.get(&*v) {
                                                if *count >= 2 * self.f + 1 {
                                                    println!("Node {} received 2f Prepare messages, going into Commit Phase", self.id);
                                                    if let Err(e) = self.send_commit(*v, *n, d.clone(), self.id).await {
                                                        eprintln!("Failed to send Commit message: {:?}", e);
                                                    }
                                                }
                                            }
                                        }
                                    } else {
                                        println!("Digest mismatch at Node {}! Expected: {}, Got: {}", self.id, log_entry.digest, received_digest);
                                    }
                                } else {
                                    println!("Log entry not found for key: {:?}", key);
                                }
                            }
                            _ => {
                                println!("Unexpected message type in Prepare message!");
                            }
                        }
                    },

                    "Commit" => {
                        
                        match &msg.msg_content {
                            MessageType::Commit { v, n: _, d, i : _} => {
                                // println!("Node {} received Commit msg from Node {}", self.id, i);
                                 //Skip processing if already received at least 2f Commit messages
                                {
                                    let commit_counts = self.commit_counts.read().await;
                                    if let Some(count) = commit_counts.get(&*v) {
                                        if *count >= 2 * self.f + 1 {
                                            // println!("Node {} already received 2f Commit messages for view {}, skipping.", self.id, v);
                                            return;
                                        }
                                    }
                                }

                                let received_digest = d.clone();

                                // Key for the PrePrepare log entry
                                let key = ("PrePrepare".to_string(), self.view_number);

                                // Acquire the read lock for the log
                                let log_entry_opt = {
                                    let log_lock = self.log.read().await;
                                    log_lock.get(&key).cloned() // Clone log entry only if it exists
                                };

                                // Check if the log entry exists
                                if let Some(ref log_entry) = log_entry_opt {
                                    // Check digest and view number match
                                    if log_entry.digest == received_digest && *v == self.view_number {
                                        // println!("Node {} approved Commit msg from Node {}", self.id, msg.sender_id);

                                        // Prepare to write to the log
                                        let key = ("Commit".to_string(), *v);
                                        let new_entry = LogEntry {
                                            message: msg.clone(),
                                            digest: log_entry.digest.clone(),
                                        };

                                        // Write to the log
                                        {
                                            let mut log_lock = self.log.write().await;
                                            log_lock.insert(key, new_entry);
                                        }
                                    
                                         // Update commit message count
                                        {
                                            let mut commit_counts = self.commit_counts.write().await;
                                            let count = commit_counts.entry(*v).or_insert(0);
                                            *count += 1;
                                        }

                                        // Check if node has received enough Commit messages
                                        {
                                            let commit_counts = self.commit_counts.read().await;
                                            if let Some(count) = commit_counts.get(&*v) {
                                                if *count >= 2 * self.f + 1 {                                               

                                                    //Get the operation to be performed from the PrePrepare Message
                                                                                                    
                                                    if let Some(log_entry) = log_entry_opt {                                                        
                                                        match &log_entry.message.msg_content {                                                         
                                                            
                                                            MessageType::Request { o, t } => {
                                                                let result: u64 = o.0 + o.1;
                                                                // println!("\n\n o: ({}, {}) and t: {t} for Node {}", o.0, o.1, self.id);
                                                                // println!("Result is {result}");
                                                                println!("Node {} received 2f Commit messages, sending result to Client", self.id);
                                                                if let Err(e) = self.send_reply(*v, *t, self.id, result).await {
                                                                    eprintln!("Failed to send Commit message: {:?}", e);
                                                                }
                                                            }
                                                            _ => {
                                                                println!("Request message not found");
                                                            }                                                                                                                    
                                                            
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    } else {
                                        println!("Digest mismatch at Node {}! Expected: {}, Got: {}", self.id, log_entry.digest, received_digest);
                                    }
                                } else {
                                    println!("Log entry not found for key: {:?}", key);
                                }
                            }
                            _ => {
                                println!("Unexpected message type in Commit message!");
                            }
                        }
                    },
                    "Reply" => {
                        // Handle Reply message
                    },
                    _ => {
                        
                    }
                }
            }
            Err(e) => {
                println!("Failed to read from socket; err = {:?}", e);
            }
        }
    
    }

    async fn send_pre_prepare(&self, v: u64, n: u64, d: String, m: Message) -> Result<()> {

        // Create the Pre-Prepare Message

        let msg_type = "PrePrepare".to_string();

        let pre_prepare_msg = Message::new(
            msg_type.clone(),
            MessageType::PrePrepare { 
                v: v,
                n: n,
                d: d.clone(),
                m: Box::new(m.clone())
            },
            self.id
        );

        let log_entry = LogEntry {
            message: pre_prepare_msg.clone(),
            digest: d.clone(),
        };

        //Store the pre-prepare in the primary's log
       {
            let key = (msg_type.clone(), v);
            let mut log_lock = self.log.write().await;
            log_lock.insert(key, log_entry);
        }

        //Serialize the message to prepare it for sending
        let serialized_msg = to_vec(&pre_prepare_msg)?;
        let node_list = {
            let node_list_lock = self.node_list.lock().await;
            node_list_lock.clone() // Clone the node_list so it has a 'static lifetime
        };

        let self_id = self.id;
        //Multicast <<PRE-PREPARE, v, n, d>, m> to all other replicas
        for (node_id, port) in node_list.into_iter() {
            if node_id != self_id {
                let address = format!("127.0.0.1:{}", port);
                let serialized_msg_clone = serialized_msg.clone(); // Clone the message for each async task
    
                // Perform async I/O without holding locks
                tokio::spawn(async move {
                    match TcpStream::connect(address).await {
                        Ok(mut stream) => {
                            if let Err(e) = stream.write_all(&serialized_msg_clone).await {
                                eprintln!("Failed to send PrePrepare message to node {}: {:?}", node_id, e);
                            }
                            if let Err(e) = stream.flush().await {
                                eprintln!("Failed to flush stream to node {}: {:?}", node_id, e);
                            } else {
                                // println!("Node {} sent a Pre Prepare message to Node {}", self_id, node_id);
                            }
                        }
                        Err(e) => {
                            eprintln!("Failed to connect to node {}: {:?}", node_id, e);
                        }
                    }
                });
            }
        }

        Ok(())

    }


    async fn send_prepare(&self, v: u64, n: u64, d: String, i: u64) -> Result<()> {
    
        // Create the Prepare message
        let prepare_msg = Message::new(
            "Prepare".to_string(),
            MessageType::Prepare {
                v: v,
                n: n,
                d: d.clone(),
                i: i,
            },
            self.id,
        );
    
        // Create the log entry before acquiring the lock
        let log_entry = LogEntry {
            message: prepare_msg.clone(),
            digest: d.clone(),
        };
    
        let key = ("Prepare".to_string(), self.id);
    
        {
            // Store the prepare in the log with minimal locking duration
            let mut log_lock = self.log.write().await;
            log_lock.insert(key, log_entry);
        } // Mutex released here
    
        // Serialize the message to prepare it for sending
        let serialized_msg = to_vec(&prepare_msg)?;
    
        // Clone node_list outside of the lock scope to avoid holding the lock during the send operations
        let node_list = {
            let node_list_lock = self.node_list.lock().await;
            node_list_lock.clone() // Clone the node_list so it has a 'static lifetime
        };
    
        // Clone `self.id` because `self` can't be moved into the async block
        let self_id = self.id;
    
        // Multicast <PREPARE, v, n, d, i> to all other replicas
        for (node_id, port) in node_list.into_iter() {
            if node_id != self_id {
                let address = format!("127.0.0.1:{}", port);
                let serialized_msg_clone = serialized_msg.clone(); // Clone the message for each async task
    
                // Perform async I/O without holding locks
                tokio::spawn(async move {
                    match TcpStream::connect(address).await {
                        Ok(mut stream) => {
                            if let Err(e) = stream.write_all(&serialized_msg_clone).await {
                                eprintln!("Failed to send message to node {}: {:?}", node_id, e);
                            }
                            if let Err(e) = stream.flush().await {
                                eprintln!("Failed to flush stream to node {}: {:?}", node_id, e);
                            } else {
                                // println!("Node {} sent a Prepare message to Node {}", self_id, node_id);
                            }
                        }
                        Err(e) => {
                            eprintln!("Failed to connect to node {}: {:?}", node_id, e);
                        }
                    }
                });
            }
        }
    
        Ok(())
    }
    
    

    async fn send_commit(&self, v: u64, n: u64, d: String, i: u64) -> Result<()> {
        //Create the Commit message
        let commit_msg = Message::new(
            "Commit".to_string(),
            MessageType::Commit { 
                v: v,
                n: n,
                d: d,
                i: i   
            },
            self.id
        );

        //Serialize the message to prepare it for sending
        let serialized_msg = to_vec(&commit_msg)?;
        let node_list = {
            let node_list_lock = self.node_list.lock().await;
            node_list_lock.clone() // Clone the node_list so it has a 'static lifetime
        };
    
        // Clone `self.id` because `self` can't be moved into the async block
        let self_id = self.id;

        //Multicast <COMMIT, v, n, d, i> to all other replicas
        for (node_id, port) in node_list.into_iter() {
            if node_id != self_id {
                let address = format!("127.0.0.1:{}", port);
                let serialized_msg_clone = serialized_msg.clone(); // Clone the message for each async task
    
                // Perform async I/O without holding locks
                tokio::spawn(async move {
                    match TcpStream::connect(address).await {
                        Ok(mut stream) => {
                            if let Err(e) = stream.write_all(&serialized_msg_clone).await {
                                eprintln!("Failed to send message to node {}: {:?}", node_id, e);
                            }
                            if let Err(e) = stream.flush().await {
                                eprintln!("Failed to flush stream to node {}: {:?}", node_id, e);
                            } else {
                                // println!("Node {} sent a Commit message to Node {}", self_id, node_id);
                            }
                        }
                        Err(e) => {
                            eprintln!("Failed to connect to node {}: {:?}", node_id, e);
                        }
                    }
                });
            }
        }

        Ok(())
    }

    async fn send_reply(&self, v: u64, t: u64, i: u64, r: u64) -> Result<()> {
        let reply_msg = Message::new(
            "Reply".to_string(),
            MessageType::Reply { 
                v: v,
                t: t,
                i: i,
                r: r   
            },
            self.id
        );

        //Serialize the message to prepare it for sending
        let serialized_msg = to_vec(&reply_msg)?;
         
        let client_address = format!("127.0.0.1:{}", 5100);
        let mut stream = TcpStream::connect(client_address).await?;
        stream.write_all(&serialized_msg).await?;

        Ok(())
    }

    
}
