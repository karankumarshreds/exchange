use std::{io::Write, net::TcpStream, thread::sleep, time::Duration};

pub struct Producer {}

impl Producer {
    pub fn new() -> Self {
        Self {}
    }

    /// Test code, ideally should be done from outside the project
    pub fn start_sending(&self) {
        loop {
            // sleep for 5 seconds
            sleep(Duration::from_secs(5));
            let payload = "s 5 hello".as_bytes();
            let mut stream = TcpStream::connect("localhost:8080").expect("Should connect to exchange");
            println!("From producer, sending message to exchange");
            stream.write_all(payload).expect("Should write to exchange");
        }
    }
}