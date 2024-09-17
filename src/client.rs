use std::collections::HashMap;
use std::process;
use std::time::{SystemTime, UNIX_EPOCH};
use serde_json::to_vec;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use crate::message::{Message, MessageType};
use anyhow::Result;
use tokio::io::AsyncReadExt;
use tokio::sync::Mutex;
use std::sync::Arc;
use tokio::time::{self, Duration};

#[derive(Clone)]
pub struct Client {
    id: u64,
    leader_port: u64,
    reply_list: Arc<Mutex<HashMap<u64, (u64, u64)>>>,
    f: u64
}


impl Client {
    pub fn new(id: u64, leader_port: u64, reply_list: Arc<Mutex<HashMap<u64, (u64, u64)>>>, f: u64) -> Self {
        let _ = reply_list;
        Self {
            id,
            leader_port,
            reply_list:Arc::new(Mutex::new(HashMap::new())),
            f
        }
    }

    //Initialize the client
    pub async fn start(&self) -> Result<()> {
        let listener = TcpListener::bind(format!("127.0.0.1:{}", 5000 + self.id)).await?;
        println!("Client {} is listening on port {}", self.id, listener.local_addr()?.port());

        //Set the timeout duration for consensus
        let timeout_duration = Duration::from_secs(10);

        // Send the request
        self.request((5, 8), SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()).await?;

        // Wait for replies with timeout
        let result = tokio::select! {
            _ = self.listen(listener) => {
                Ok(())
            }
            _ = time::sleep(timeout_duration) => {
                println!("\x1b[31mNo consensus reached within the timeout. Terminating program.\x1b[0m");
                process::exit(0);
            }
        };

        result

        // Ok(())

        
    }

    async fn listen(&self, listener: TcpListener) -> Result<()> {        
        loop {
            let (socket, _) = listener.accept().await?;
            let client_clone = self.clone();
            tokio::spawn(async move {
                client_clone.handle_connection(socket).await;
            });
        }
    }

    

    async fn handle_connection(&self, mut socket: TcpStream) {
        // Buffer to read data
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
                    "Reply" => {
                        if let MessageType::Reply { v: _, t, i: _, r } = msg.msg_content {
                            // Process the reply result
                            // println!("\n\n Received Reply with result: {}", r);
                       
                            
                            let mut replies = self.reply_list.lock().await;

                            //Get result with same timestamp and increment counter; or create new entry with the timestamp
                            let entry = replies.entry(t).or_insert_with(|| (r, 0));
                            if entry.0 == r {
                                entry.1 += 1;
                            }

                            //Check if client received f + 1 replies and finish execution
                            if entry.1 >= self.f + 1 {
                                println!(
                                    "\x1b[32m\n\n Client accepted result = {} as at least f + 1 ({}) replies were received \n\n\x1b[0m",
                                    r,
                                    entry.1
                                );
                                process::exit(0);                                
                            }
                        } else {
                            println!("Unexpected message format for Reply");
                        }
                    }
                    // Handle other message types (e.g., Request, Commit, etc.)
                    _ => {
                        println!("Unhandled message type: {:?}", msg.msg_type);
                    }
                }
            }
            Err(e) => {
                eprintln!("Failed to read from socket: {}", e);
            }
        }
    }


    //Function to send <REQUEST, o, t, c> to the Primary
    pub async fn request (&self, o: (u64, u64), t: u64) -> Result<()> {
        let message =  Message::new(
            "Request".to_string(),
            MessageType::Request { 
                o: o,
                t: t
            },
            self.id);

        let serialized_msg = to_vec(&message)?;
        
        let leader_address = format!("127.0.0.1:{}", self.leader_port);
        let mut stream = TcpStream::connect(leader_address).await?;
        stream.write_all(&serialized_msg).await?;

        println!("\nClient {} sent request to leader on port {}: {:?}", self.id, self.leader_port, message);

        Ok(())
    }
}