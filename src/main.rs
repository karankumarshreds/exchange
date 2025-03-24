// TODO: Use the msg_type enums everywhere
// TODO: parse it in case "c" is received
use anyhow::{Context, Result};
use enums::Q_NAME;
use queue::Queue;
use std::{
    io::{BufReader, Read, Write},
    net::{TcpListener, TcpStream},
    sync::{Arc, Mutex},
    thread,
};
mod consumer;
mod enums;
mod producer;
mod queue;

const BATCH_SIZE: usize = 5;

struct Exchange {}

struct Payload {
    msg_type: String,
    q_name: String,
    payload: String,
}

trait Readable<T: Read> {
    fn read_from(reader: &mut T) -> Self;
}

trait ReadableSized<T: Read>: Sized {
    fn read_of_len(reader: &mut T, len: usize) -> Self;
}

impl<T: Read> ReadableSized<T> for String {
    fn read_of_len(reader: &mut T, len: usize) -> Self {
        let mut buf = vec![0u8; len];
        println!("For string reading {len} bytes");
        reader.read_exact(&mut buf).expect("Should read char");
        let res = std::str::from_utf8(&buf).expect("").to_string();
        println!("result: {res}");
        res
    }
}

impl<T: Read> Readable<T> for u32 {
    fn read_from(reader: &mut T) -> Self {
        let mut buf = [0u8; 4];
        reader.read_exact(&mut buf).expect("Should read u32");
        u32::from_be_bytes(buf)
    }
}

impl<T: Read> Readable<T> for char {
    fn read_from(reader: &mut T) -> Self {
        let mut buf = [0u8; 1]; // char is 1 byte
        reader.read_exact(&mut buf).expect("Should read char");
        std::str::from_utf8(&buf)
            .expect("")
            .chars()
            .next()
            .expect("Should be there")
    }
}

impl Exchange {
    pub fn new() -> Self {
        Exchange {}
    }

    pub fn parse_payload(&self, stream: &TcpStream) -> Payload {
        let mut reader = BufReader::new(stream);
        let msg_type = char::read_from(&mut reader);
        println!("main.rs -> parse_payload() -> msg_type: {msg_type}");
        if msg_type == 'c' {
            println!("main.rs -> parse_payload() -> Connection request received");
            let id_len = u32::read_from(&mut reader);
            let id_name = String::read_of_len(&mut reader, id_len as usize); // although we are not using len specifically to parse, lets see if it works
            let q_name_len = u32::read_from(&mut reader);
            let q_name = String::read_of_len(&mut reader, q_name_len as usize);
            println!(
                "msg_type: {msg_type}, q_name_len: {q_name_len}, q_name: {q_name}, id_len: {id_len}, id_name: {id_name}"
            );

            return Payload {
                msg_type: msg_type.to_string(),
                payload: "".to_string(), // no payload for the connection request
                q_name,
            };
        }

        let q_len = u32::read_from(&mut reader);
        let q_name = String::read_of_len(&mut reader, q_len as usize);

        let payload_len = u32::read_from(&mut reader);
        let payload = String::read_of_len(&mut reader, payload_len as usize);

        println!("main.rs -> parse_payload() -> q_name: {q_name} & payload: {payload}");

        Payload {
            q_name: q_name.to_string(),
            payload: payload.to_string(),
            msg_type: msg_type.to_string(),
        }
    }

    /// Should only focus on the send (producer) messages
    /// "c" connection, "r" receive requests must be handled immediately
    pub fn process_batch(&self, batch: &mut Vec<Payload>, queue: Arc<Mutex<Queue>>) -> Result<()> {
        // We do this so that we don't have to use Arc Mutex on batch to read, push and clear each time
        // This way we create a clone of the batch and send it to the child process, reset the old one and keep updating it
        let batch_to_process = std::mem::take(batch); // creates a new batch and setting old batch with default empty
        let queue = Arc::clone(&queue);

        thread::spawn(move || {
            let mut queue = queue.lock().expect("Should attain lock on queue");
            for payload in batch_to_process {
                match payload.msg_type.as_str() {
                    "s" => {
                        // Re-assuring the type is send only
                        queue
                            .push(payload.q_name, payload.payload)
                            .expect("Should push to queue");
                    }
                    _ => {
                        unimplemented!()
                    }
                }
            }
        });
        return Ok(());
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // client connects
    // create a tcp listener for the producer to connect to

    let producer = producer::Producer::new();
    let consumer = consumer::Consumer::new("consumer-abc", Q_NAME);
    thread::spawn(move || producer.start_sending());
    tokio::task::spawn(async move { consumer.poll().await });

    let listener = TcpListener::bind("127.0.0.1:8080").context("Unable to bind address")?;
    let queue = Arc::new(Mutex::new(Queue::new()));

    // let mut batch: Vec<TcpStream> = Vec::with_capacity(BATCH_SIZE);
    let mut batch: Vec<Payload> = Vec::with_capacity(BATCH_SIZE);

    println!("main() -> listening on stream");
    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                let exchange = Exchange::new();
                let payload = exchange.parse_payload(&stream);
                match payload.msg_type.as_str() {
                    "c" => {
                        println!("Parse the payload for connection request");
                        continue;
                    }
                    "r" => {
                        println!("Got receive request");
                        // Immediately pop and reply back. We don't want a batch or halt
                        // here as the receivers would already be polling with an interval
                        let mut queue = queue.lock().expect("Should get locked");
                        match queue.pop(&payload.q_name) {
                            Some(data) => {
                                // write back to the stream
                                stream
                                    .write(data.as_bytes())
                                    .expect("Should write back to the stream");
                            }
                            None => {
                                println!("No data or queue with name {}", payload.q_name);
                            }
                        }
                    }
                    _ => {
                        if batch.len() < BATCH_SIZE {
                            println!("Pushing to batch!");
                            batch.push(payload);
                            continue;
                        } else {
                            exchange
                                .process_batch(&mut batch, queue.clone())
                                .expect("Should process batch");
                        }
                    }
                };
            }
            Err(err) => {
                println!("{err}");
                todo!("Figure out a way to handle the error");
            }
        }
    }
    Ok(())
}
