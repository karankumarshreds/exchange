use core::time;
use std::{thread, time::Duration};
use tokio::io::AsyncWriteExt;

use crate::enums;

pub struct Consumer {
    identifier: &'static str,
    q_name: &'static str,
}

const POLL_INTERVAL_IN_SECONDS: u64 = 6;

impl Consumer {
    pub fn new(identifier: &'static str, q_name: &'static str) -> Self {
        Self { identifier, q_name }
    }

    /// The reason why we return the tokio::net::TcpStream is because it gives us
    /// the leverage of splitting the stream into the read and write half (both owned values)
    /// as we need one to be shared with a separate reader task (thread) and another for sending
    /// read request (polling) on the main thread
    async fn connect(&self) -> tokio::net::TcpStream {
        println!("consumer.rs -> connect()");
        let mut stream = tokio::net::TcpStream::connect("localhost:8080")
            .await
            .expect("Should connect");
        let mut header = Vec::new();
        let msg_type = enums::msg_type(enums::MessageType::Conn).to_string(); // add the "c" char for connection
        header.extend_from_slice(msg_type.as_bytes()); // msg_type as a single char -> 1 byte
        header.extend_from_slice(&(self.identifier.len() as i32).to_be_bytes()); // add the q_name length
        header.extend_from_slice(self.identifier.as_bytes()); // add the indentifier
        header.extend_from_slice(&(self.q_name.len() as i32).to_be_bytes()); // add the q_name length
        header.extend_from_slice(self.q_name.as_bytes()); // add the queue_name
        stream
            .write(&header)
            .await
            .expect("Should write to the exchange to connect");
        stream
    }

    pub async fn poll(self) {
        thread::sleep(Duration::from_secs(6));
        println!("consumer.rs -> poll() -> after 6s initial delay");
        let stream = self.connect().await; // connect to exchange
        let (mut reader, mut writer) = stream.into_split();
        /* We don't need the below way of splitting the read and write halves */
        /* because: */
        /* 1. BufWriter doesn't let you have the owned halves which we need across multiple threads */
        /* 2. Tokio TcpStream allows you to have that */
        /* 3. Otherwise we would have to use the ArcMutex */
        // let reader = BufReader::new(stream.try_clone().expect("shouldclone"));
        // let  writer = BufWriter::new(stream);

        tokio::task::spawn(async move {
            loop {
                let mut buf = [0u8; 10];
                let n_bytes = reader.peek(&mut buf).await.expect("");
                if n_bytes > 0 {
                    println!("Received {n_bytes} bytes");
                }
            }
        });

        // Iterate and send the poll request with the queue name
        loop {
            thread::sleep(time::Duration::from_secs(POLL_INTERVAL_IN_SECONDS));
            let msg_type = b'c';
            writer
                .write(msg_type.to_string().as_bytes())
                .await
                .expect("should ");
        }
    }
}
