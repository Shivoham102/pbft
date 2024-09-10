use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum MessageType {
    PrePrepare,
    Prepare,
    Commit,
    Reply,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Message {
    pub msg_type: MessageType,
    pub view: u64,
    pub sequence_number: u64,
    pub payload: String, // This can be more complex depending on your needs
    pub sender_id: u64,
}
