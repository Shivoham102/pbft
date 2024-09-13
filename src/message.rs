use serde::{Serialize, Deserialize};
use sha2::{Sha256, Digest};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]

//Create custom types for all the 5 message types
pub enum MessageType {
    Request {
        o: (u64, u64),
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
        r: u64
    },
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Message {
    pub msg_type: String,
    pub msg_content: MessageType,
    pub sender_id: u64,
}

impl Message {
    pub fn new(msg_type: String, msg_content: MessageType, sender_id: u64) -> Self {
        Message {
            msg_type,
            msg_content,        
            sender_id,           
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
