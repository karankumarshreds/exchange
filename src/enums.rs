#[allow(unused)]
pub enum MessageType {
    Send,
    Recv,
    Conn,
}

pub fn msg_type(msg_type: MessageType) -> &'static str {
    match msg_type {
        MessageType::Send => "s",
        MessageType::Recv => "r",
        MessageType::Conn => "c",
    }
}

pub const Q_NAME: &'static str = "queue-test";
