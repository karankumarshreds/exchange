// use hex_literal::hex;
use std::{io::Write, net::TcpStream, thread::sleep, time::Duration};

use crate::enums::Q_NAME;

pub struct Producer {}

impl Producer {
    pub fn new() -> Self {
        Self {}
    }

    /// Test code, ideally should be done from outside the project
    pub fn start_sending(&self) {
        println!("producer.rs -> start_sending()");
        loop {
            println!("producer.rs -> start_sending() -> using Q_NAME: {Q_NAME}");
            sleep(Duration::from_secs(5)); // Sleep for 5 seconds
            let mut header = Vec::<u8>::new();
            self.construct_payload(&mut header, "Hi from producer", &Q_NAME);
            let mut stream =
                TcpStream::connect("localhost:8080").expect("Should connect to exchange");
            stream.write_all(&header).expect("Should write to exchange");
        }
    }

    fn construct_payload(&self, buf: &mut Vec<u8>, payload: &str, queue_name: &str) {
        let msg_type: u8 = b's'; /* sender */
        buf.push(msg_type);
        buf.extend_from_slice(&(queue_name.len() as i32).to_be_bytes()); /* add queue_name length */
        buf.extend_from_slice(queue_name.as_bytes()); /* add queue_name */
        buf.extend_from_slice(&((payload.len() as i32).to_be_bytes()));
        buf.extend_from_slice(payload.as_bytes()); /* add queue_name */
    }
}
