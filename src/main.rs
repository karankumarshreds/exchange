use std::{io::Read, net::TcpListener, thread};
use anyhow::{Context, Result};
mod producer;
mod consumer;
mod queue;

fn main() -> Result<()> {
    // client connects
    // create a tcp listener for the producer to connect to

    let producer = producer::Producer::new();
    thread::spawn(move || producer.start_sending());

    let listener = TcpListener::bind("127.0.0.1:8080").context("Unable to bind address")?;

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                let mut buf = [0u8; 5];
                stream.read_exact(&mut buf[..]).context("Should read to buffer")?;
                let a = std::str::from_utf8(&buf[..1]).context("Should convert to utf8")?;
                let l = (buf[2] as char).to_digit(10).expect("Should ");
                println!("Exchange: received a: {a} & l: {l}");
                match a {
                    "s" => println!("Producer sent"),
                    "r" => println!("Consumer requested"),
                    _ => unimplemented!("unrecognised pattern")
                }
            },
            Err(err) => {
                println!("{err}");
                todo!("Figure out a way to handle the error");
            }
        }
    };
    // client sends queue name and data
    // we send the data to the queue
    Ok(())
}


// golang: subsystem in telesat, build one, network
// satcom
// telecom
// connect to satellite and 
// similar to 5g