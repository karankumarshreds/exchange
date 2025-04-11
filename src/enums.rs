type XName = String;
type QName = String;

#[derive(Debug,Default)]
pub struct ConnPayload {
    pub x_name: XName,
    pub q_name: QName,
}

#[derive(Debug)]
pub struct SendPayload {
    pub x_name: XName,
    pub data: String,
    pub x_type: char, // 'f': fanout, 'd': direct
}

#[derive(Debug)]
pub struct RecvPaylod {
    pub q_name: QName,
}

pub enum RequestType {
    Connection(ConnPayload),
    Producer(SendPayload),
    Consumer(RecvPaylod),
}


pub fn msg_type(msg_type: RequestType) -> &'static str {
    match msg_type {
        RequestType::Producer(_) => "s",
        RequestType::Consumer(_) => "r",
        RequestType::Connection(_) => "c",
    }
}

pub const Q_NAME: &'static str = "queue-test";
pub const X_NAME: &'static str = "exchange-test";
