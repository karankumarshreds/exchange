use anyhow::{Context, Result};
use std::{
    collections::{HashMap, VecDeque},
    default::Default,
};

// client -> (queue_name) -> (collect client)

pub struct QState {
    queues: HashMap<String, VecDeque<String>>,
}

impl QState {
    pub fn new() -> Self {
        return Self {
            queues: HashMap::new(),
        };
    }

    /// adds a new queue with default queue
    fn create(&mut self, q_name: String) -> Result<()> {
        self.queues.entry(q_name).or_insert(Default::default());
        Ok(())
    }

    pub fn push(&mut self, q_name: String, payload: String) -> Result<()> {
        // straight forward way
        self.queues
            .entry(q_name.clone())
            .or_default()
            .push_back(payload);
        println!("Pushed to queue: {:#?}", self.queues.get(&q_name));
        Ok(())
    }

    pub fn pop(&mut self, q_name: &str) -> Option<String> {
        self.queues
            .entry(q_name.to_string())
            .or_default()
            .pop_back()
    }
}
