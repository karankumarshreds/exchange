// TODO: Use the msg_type enums everywhere
// TODO: We do not need the identifiers
// TODO: parse it in case "c" is received
// TODO: Fanout w/ potential direct exchange feature in future
// TODO: Bindings with a exchange
// TODO: ACK and retries
// TODO: Dead Letter Queue

/*
NOTE: the queue_name is the identifier
If two or more entities are consuming the same queues, they will compete for the queue (round robin)
It you want all the entities to consume the messages on an exchange, create their own queues with different name
The client connects with the "c" msg_type && :
    - declares the exchange name along with the exchange type (direct or fanout)
    - binds the queue with the exchange
    - consumers with same queue_name will be competing as consumers (regardless consumers)
    - direct exchange will work with exchange_name as well as routing_key

    s -> queue_name -> payload (X -> queue_name.push)
    c -> identifier && q_name -> TcpConn save based on the identifier (queues)
    r -> queue_name && identifer -> ^^ state update

    s -> exchange name: x_name && x_type: Fanout | Direct(routing_key)
    c -> x_name bind w/ q_name && i(x_change server)
    Lets say there are 3 services:
    1. service_a -> polling on q_a -> bound to exchange "x_a"
    2. service_b -> polling on q_a -> bound to exchange "x_a"


    As per the rabbitmq fanout exchange unless the queue name is different, we do the round robin
    to competing consumers.

    Lets assume service_a polls every 3 seconds. And service_b polls every 10 seconds, does that mean
    we wait send the first message to service_a and despite of us having next message available, we ignore the next
    request (on 6th second) by service_a just to achieve round robin? What should happen in this case?

    // queue_a -> "b", "c"
    // exchange: error: q_a <- service_a // A // receive request 3th second
    // exchange: error: q_a <- service_b // A // receive request 10th second
    // exchange: error: q_b <- service_c // B
    r -> x_name && q_name -> if the q_name is the same, that means i need to RR
 */
use anyhow::{Context, Result};
use enums::{ConnPayload, RecvPaylod, RequestType, SendPayload, Q_NAME, X_NAME};
use queue::QState as Queue;
use std::{
    collections::HashMap,
    default::Default,
    io::{BufReader, Read, Write},
    net::{TcpListener, TcpStream},
    sync::{Arc, Mutex},
    thread, time::Duration,
};
use producer::{Connection, ExchangeOption};
mod consumer;
mod enums;
mod producer;
mod queue;

const BATCH_SIZE: usize = 5;

type XName = String;
type QName = String;

struct Exchange {
    bindings: HashMap<XName, QName>,
}

pub enum PayloadType {
    XName(String),
    QName(String),
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
        reader.read_exact(&mut buf).expect("Should read char");
        let res = std::str::from_utf8(&buf).expect("").to_string();
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
        let mut buf = [0u8; 1];
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
        Exchange {
            bindings: Default::default(),
        }
    }

    pub fn parse_payload(&self, stream: &TcpStream) -> RequestType {
        let mut reader = BufReader::new(stream);
        let msg_type = char::read_from(&mut reader);
        println!("main.rs -> parse_payload() -> msg_type: {msg_type}");

        let x_type = char::read_from(&mut reader); // reserved 8 bits for x_type
        println!("main.rs -> parse_payload() -> x_type: {x_type}");

        let x_name_len = u32::read_from(&mut reader); // reserved 32 bits for x_name_len
        println!("main.rs -> parse_payload() -> x_name_len: {x_name_len}");

        let x_name = String::read_of_len(&mut reader, x_name_len as usize);
        println!("main.rs -> parse_payload() -> x_name: {x_name}");

        let q_name_len = u32::read_from(&mut reader); // reserved 32 bits for q_name_len
        let q_name = String::read_of_len(&mut reader, q_name_len as usize);
        let data_len = u32::read_from(&mut reader); // reserved 32 bits for data_len
        let data = String::read_of_len(&mut reader, data_len as usize);

        match msg_type {
            's' => RequestType::Producer(SendPayload { x_name, data, x_type }),
            'r' => RequestType::Consumer(RecvPaylod { q_name }),
            'c' => RequestType::Connection(ConnPayload { q_name, x_name }),
            _ => todo!(),
        }
    }

    /// Should only focus on the send (producer) messages
    /// "c" connection, "r" receive requests must be handled immediately
    pub fn process_batch(
        &self,
        batch: &mut Vec<SendPayload>,
        queue: Arc<Mutex<Queue>>,
    ) -> Result<()> {
        // We do this so that we don't have to use Arc Mutex on batch to read, push and clear each time
        // This way we create a clone of the batch and send it to the child process, reset the old one and keep updating it
        let batch_to_process = std::mem::take(batch); // creates a new batch and setting old batch with default empty
        let queue = Arc::clone(&queue);

        thread::spawn(move || {
            let mut _queue = queue.lock().expect("Should attain lock on queue");
            for b in batch_to_process {
                // let x_name = b.x_name;
                // let q_name = b.q_name;
                // let data = b.data;
                // queue
                //     .push(b.q_name, b.data)
                //     .expect("Should push to queue");
                todo!(
                    "Push to the bound queue with the exchange name: {0}",
                    b.x_name
                );
            }
        });
        return Ok(());
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:8080").context("Unable to bind address")?;
    let queue = Arc::new(Mutex::new(Queue::new()));

    let mut ex = Connection::connect("localhost:8080").expect("Should connect");
    ex.declare_exchange(X_NAME.into(), ExchangeOption::Fanout);
    // ex.publish("This is the test message".into()).expect("Should send payload");
    let consumer = consumer::Consumer::new("consumer-abc", Q_NAME);
    thread::spawn(move || {
        loop {
            thread::sleep(Duration::from_secs(5)); // Sleep for 5 seconds
            ex.publish("test data").expect("Should send test data");
        }
    });
    tokio::task::spawn(async move { consumer.poll().await });

    // This is only for the receive send requests
    let mut batch: Vec<SendPayload> = Vec::with_capacity(BATCH_SIZE);

    println!("main() -> listening on stream");

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                let exchange = Exchange::new();
                let request_type = exchange.parse_payload(&stream);
                match request_type {
                    RequestType::Connection(payload) => {
                        println!("CONN REQUEST:\t{:?}", payload);
                    }
                    RequestType::Producer(payload) => {
                        println!("RECEIVED SEND REQUEST:\t{:?}", payload);
                        // Take the exchange name, push the message to all the queues associated with it
                        // Do this via the batch approach
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
                    RequestType::Consumer(payload) => {
                        println!("RECV REQUEST:\t{:?}", payload);
                        let mut queue = queue.lock().expect("Should get locked");
                        let q_name = payload.q_name;
                        match queue.pop(&q_name) {
                            Some(data) => {
                                // Write back to the stream
                                stream
                                    .write(data.as_bytes())
                                    .expect("Should write back to the stream");
                            }
                            None => {
                                println!("No data or queue with name {}", q_name);
                            }
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
