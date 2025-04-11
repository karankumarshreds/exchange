// use hex_literal::hex;
use std::{io::Write, net::TcpStream};

#[derive(Debug)]
pub enum ExchangeOption {
    Fanout,
}

pub struct Connection {
    something: &'static str
}

pub struct Exchange {
    tcp_stream: TcpStream,
    exchange_name: Option<String>,
    exchange_option: Option<ExchangeOption>,
}

impl Exchange {
    pub fn declare_exchange(&mut self, exchange_name: String, exchange_option: ExchangeOption) {
        self.exchange_name = Some(exchange_name);
        self.exchange_option = Some(exchange_option);
    }

    pub fn publish(&mut self, data: &str) -> anyhow::Result<()> {
        let mut buf = Vec::<u8>::new();
        self.construct_payload(&mut buf, data);
        self.tcp_stream.write_all(&buf).expect("Should write all");
        Ok(())
    }


    fn construct_payload(&self, buf: &mut Vec<u8>, data: &str) {
        let x_name = self.exchange_name.as_ref().expect("Should be there");
        let msg_type: u8 = b's'; /* sender */
        buf.push(msg_type);
        buf.extend_from_slice(&(x_name.len() as i32).to_be_bytes()); /* add x_name */
        buf.extend_from_slice(x_name.as_bytes()); /* add x_name */
        buf.extend_from_slice(&((data.len() as i32).to_be_bytes()));
        buf.extend_from_slice(data.as_bytes());
    }
}

impl Connection {
    pub fn connect(addr: &'static str) -> anyhow::Result<Exchange> {
        let tcp_stream =
            TcpStream::connect(addr).expect("Should connect to exchange");
        Ok(Exchange {
            tcp_stream,
            exchange_name: None,
            exchange_option: None,
        })
    }
}
