use std::time::{SystemTime, UNIX_EPOCH};
use serde_json::to_vec;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use crate::message::{Message, MessageType};
use anyhow::Result;

#[derive(Clone)]
pub struct Client {
    id: u64,
    leader_port: u64
}


impl Client {
    pub fn new(id: u64, leader_port: u64) -> Self {
        Self {
            id,
            leader_port
        }
    }

    //Initialize the client
    pub async fn start(&self) -> Result<()> {
        let listener = TcpListener::bind(format!("127.0.0.1:{}", 5000 + self.id)).await?;
        println!("Client {} is listening on port {}", self.id, listener.local_addr()?.port());

        tokio::try_join!(
            self.listen(listener),
            self.request("Operation X + Y".to_string(), 25)
        )?;

        Ok(())

        
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

    async fn handle_connection(&self, socket: TcpStream) {
        // Handle incoming connections
        println!("Handling connection for Client {}", self.id);
    }

    //Function to send <REQUEST, o, t, c> to the Primary
    pub async fn request (&self, o: String, t: u64) -> Result<()> {
        let message =  Message::new(
            "Request".to_string(),
            MessageType::Request { 
                o: "Operation X + Y".to_string(),
                t: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
            },
            self.id);

        let serialized_msg = to_vec(&message)?;
        
        let leader_address = format!("127.0.0.1:{}", self.leader_port);
        let mut stream = TcpStream::connect(leader_address).await?;
        stream.write_all(&serialized_msg).await?;

        println!("Client {} sent request to leader on port {}: {:?}", self.id, self.leader_port, message);

        Ok(())
    }
}