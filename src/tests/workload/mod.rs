use crate::tests::workload::Error::TokioJoinError;
use crate::{
    err::Error,
    helpers,
    storage::DataStore,
    types::{Key, Value},
};
use futures::future::join_all;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;

type WriteWorkloadMap = HashMap<Key, Value>;

type ReadWorkloadMap = HashMap<Key, Value>;

type ReadWorkloadVec = Vec<Entry>;

type WriteWorkloadVec = Vec<Entry>;

#[derive(Clone, Debug)]
pub struct Workload {
    pub size: usize,
    pub key_len: usize,
    pub val_len: usize,
    pub write_read_ratio: f64,
}

#[derive(Clone, Debug)]
pub struct Entry {
    pub key: Vec<u8>,
    pub val: Vec<u8>,
}

impl Workload {
    pub fn new(size: usize, key_len: usize, val_len: usize, write_read_ratio: f64) -> Self {
        Self {
            size,
            key_len,
            val_len,
            write_read_ratio,
        }
    }

    pub fn generate_workload_data_as_map(&self) -> (ReadWorkloadMap, WriteWorkloadMap) {
        let mut write_workload = HashMap::with_capacity(self.size);
        let mut read_workload = HashMap::with_capacity((self.size as f64 * self.write_read_ratio) as usize);
        for _ in 0..self.size {
            let key = helpers::generate_random_id(self.key_len);
            let val = helpers::generate_random_id(self.val_len);
            write_workload.insert(key.as_bytes().to_vec(), val.as_bytes().to_vec());
        }

        let read_workload_size = (self.size as f64 * self.write_read_ratio) as usize;
        read_workload.extend(
            write_workload
                .iter()
                .take(read_workload_size)
                .map(|(key, value)| (key.to_vec(), value.to_vec())),
        );
        (read_workload, write_workload)
    }

    pub fn generate_workload_data_as_vec(&self) -> (ReadWorkloadVec, WriteWorkloadVec) {
        let mut write_workload = Vec::with_capacity(self.size);
        let mut read_workload = Vec::with_capacity((self.size as f64 * self.write_read_ratio) as usize);
        for _ in 0..self.size {
            let key = helpers::generate_random_id(self.key_len);
            let val = helpers::generate_random_id(self.val_len);
            let entry = Entry {
                key: key.as_bytes().to_vec(),
                val: val.as_bytes().to_vec(),
            };
            write_workload.push(entry);
        }
        let read_workload_size = (self.size as f64 * self.write_read_ratio) as usize;
        read_workload.extend(write_workload.iter().take(read_workload_size).map(|e| Entry {
            key: e.key.to_owned(),
            val: e.val.to_owned(),
        }));

        (read_workload, write_workload)
    }

    pub async fn insert_parallel(
        &self,
        entries: &Vec<Entry>,
        store: Arc<RwLock<DataStore<'static, Vec<u8>>>>,
    ) -> Result<(), Error> {
        let tasks = entries.iter().map(|e| {
            let s_engine = Arc::clone(&store);
            let key = e.key.clone();
            let val = e.val.clone();
            tokio::spawn(async move {
                let key_str = std::str::from_utf8(&key).unwrap();
                let val_str = std::str::from_utf8(&val).unwrap();
                let mut value = s_engine.write().await;
                value.put(key_str, val_str).await
            })
        });

        let all_results = join_all(tasks).await;
        for tokio_response in all_results {
            match tokio_response {
                Ok(entry) => {
                    if let Err(err) = entry {
                        return Err(err);
                    }
                }
                Err(_) => return Err(TokioJoinError),
            }
        }
        return Ok(());
    }
}
