use tokio::net::TcpListener;
use tokio::net::TcpStream;

#[derive(Clone)]
pub struct Client {
    id: u64
}


impl Client {
    pub fn new(id: u64) -> Self {
        Self {
            id
        }
    }

    pub async fn start(&self) -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind(format!("127.0.0.1:{}", self.id + 50)).await?;
        println!("Client {} is listening", self.id);
       
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
}