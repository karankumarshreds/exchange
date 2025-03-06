use std::{collections::{HashMap, VecDeque}, default::Default};
use anyhow::{Result, Context};

pub struct Queue {
    queues: HashMap<String, VecDeque<String>>,
}

impl Queue {
    pub fn new() -> Queue {
        return Queue { queues: HashMap::new() }
    }

    pub fn add(&mut self, q_name: String) -> Result<()> {
        self.queues.entry(q_name).or_insert(Default::default());
        Ok(())
    }

    pub fn push(&mut self, q_name: String, value: String) ->Result<()> {
        self.queues.get_mut(&q_name).context("Invalid queue name, queue not found")?.push_back(value);
        Ok(())
    }

    pub fn pop(&mut self, q_name: String) -> Result<()> {
        self.queues.remove(&q_name).context("Invalid queue name, queue not found")?;
        Ok(())
    }
}
