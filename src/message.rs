use serde::{Serialize, Deserialize};
use sha2::{Sha256, Digest};

#[derive(Serialize, Deserialize, Debug, Clone)]

//Create custom types for all the 5 message types
pub enum MessageType {
    Request {
        o: String,
        t: u64
    },
    PrePrepare {
        v: u64,
        n: u64,
        d: String,
        m: Box<Message>
    },
    Prepare {
        v: u64,
        n: u64,
        d: String,
        i : u64  
    },
    Commit {
        v: u64,
        n: u64,
        d: String,
        i : u64 
    },
    Reply {
        v: u64,
        t: u64,
        i : u64,
        r: String
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Message {
    pub msg_type: MessageType,
    sender_id: u64,
    // pub view: u64,
    // pub sequence_number: u64,
    // pub payload: String, // This can be more complex depending on your needs
    // pub timestamp: Option<u64>,
}

impl Message {
    pub fn new(msg_type: MessageType, sender_id: u64) -> Self {
        Message {
            msg_type,
            // payload,
            sender_id,
            // timestamp,
        }
    }

    // Function to compute the digest of the message
    pub fn compute_digest(&self) -> String {
        let serialized_msg = serde_json::to_string(self).expect("Failed to serialize message");
        
        let mut hasher = Sha256::new();
        hasher.update(serialized_msg);

        // Get the final hash as a hexadecimal string
        let result = hasher.finalize();
        format!("{:x}", result)
    }
}