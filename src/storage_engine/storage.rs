use crate::{
    background::{BackgroundJob, BackgroundResponse, FlushData},
    bloom_filter::BloomFilter,
    cfg::Config,
    compaction::{Bucket, BucketMap, Compactor},
    consts::{
        BUCKETS_DIRECTORY_NAME, CHANNEL_BUFFER_SIZE, HEAD_ENTRY_KEY, HEAD_ENTRY_LENGTH,
        META_DIRECTORY_NAME, SIZE_OF_U32, SIZE_OF_U64, SIZE_OF_U8, TAIL_ENTRY_KEY,
        TOMB_STONE_MARKER, VALUE_LOG_DIRECTORY_NAME, WRITE_BUFFER_SIZE,
    },
    err::StorageEngineError,
    key_offseter::TableBiggestKeys,
    memtable::{Entry, InMemoryTable},
    meta::Meta,
    sparse_index::SparseIndex,
    sstable::{SSTable, SSTablePath},
    value_log::ValueLog,
};
use chrono::Utc;
use futures::io::Empty;
use log::{error, info};
use tokio::sync::{
    mpsc::{self, error::TryRecvError, Receiver, Sender},
    RwLock,
};

use crate::err::StorageEngineError::*;
use std::{collections::HashMap, fs, path::PathBuf};
use std::{hash::Hash, sync::Arc};

#[derive(Debug)]
pub struct StorageEngine<K: Hash + PartialOrd + std::cmp::Ord> {
    pub dir: DirPath,
    pub active_memtable: InMemoryTable<K>,
    pub bloom_filters: Vec<BloomFilter>,
    pub val_log: ValueLog,
    pub buckets: BucketMap,
    pub biggest_key_index: TableBiggestKeys,
    pub compactor: Compactor,
    pub meta: Meta,
    pub config: Config,
    pub read_only_memtables: HashMap<K, Arc<RwLock<InMemoryTable<K>>>>,
    pub background_channel_rcv: Receiver<Result<BackgroundResponse, StorageEngineError>>,
    pub background_channel_snd: Arc<RwLock<Sender<Result<BackgroundResponse, StorageEngineError>>>>,
}

#[derive(Clone, Debug)]
pub struct DirPath {
    pub root: PathBuf,
    pub val_log: PathBuf,
    pub buckets: PathBuf,
    pub meta: PathBuf,
}

#[derive(Clone, Copy, Debug)]
pub enum SizeUnit {
    Bytes,
    Kilobytes,
    Megabytes,
    Gigabytes,
}

impl StorageEngine<Vec<u8>> {
    pub async fn new(dir: PathBuf) -> Result<Self, StorageEngineError> {
        let dir = DirPath::build(dir);
        let default_config = Config::default();
        StorageEngine::with_default_capacity_and_config(
            dir.clone(),
            SizeUnit::Bytes,
            WRITE_BUFFER_SIZE,
            &default_config,
        )
        .await
    }

    pub async fn new_with_custom_config(
        dir: PathBuf,
        config: &Config,
    ) -> Result<Self, StorageEngineError> {
        let dir = DirPath::build(dir);
        StorageEngine::with_default_capacity_and_config(
            dir.clone(),
            SizeUnit::Bytes,
            WRITE_BUFFER_SIZE,
            config,
        )
        .await
    }

    /// A Result indicating success or an `StorageEngineError` if an error occurred.
    pub async fn put(&mut self, key: &str, value: &str) -> Result<bool, StorageEngineError> {
        // Convert the key and value into Vec<u8> from given &str.
        let key = &key.as_bytes().to_vec();
        let value = &value.as_bytes().to_vec();
        let created_at = Utc::now().timestamp_millis() as u64;
        let is_tombstone = false;

        // check channel for queued updates
        self.check_queued_updates().await;

        // Write to value log first which returns the offset
        let v_offset = self
            .val_log
            .append(key, value, created_at, is_tombstone)
            .await?;

        if self.active_memtable.is_full(HEAD_ENTRY_KEY.len()) {
            let capacity = self.active_memtable.capacity();
            let size_unit = self.active_memtable.size_unit();
            let false_positive_rate = self.active_memtable.false_positive_rate();
            let head_offset = self
                .active_memtable
                .index
                .iter()
                .max_by_key(|e| e.value().0);

            // reset head in vLog
            self.val_log
                .set_head(head_offset.clone().unwrap().value().0);
            let head_entry = Entry::new(
                HEAD_ENTRY_KEY.to_vec(),
                head_offset.unwrap().value().0,
                Utc::now().timestamp_millis() as u64,
                false,
            );

            let _ = self.active_memtable.insert(&head_entry);
            self.active_memtable.read_only = true;
            self.read_only_memtables.insert(
                InMemoryTable::generate_table_id(),
                Arc::new(RwLock::new(self.active_memtable.to_owned())),
            );

            if self.read_only_memtables.len() >= self.config.max_buffer_write_number {
                let (table_id, table_to_flush) = self.read_only_memtables.iter().next().unwrap();
                let mut flush_job = BackgroundJob::Flush(FlushData::new(
                    Arc::clone(table_to_flush),
                    table_id.to_owned(),
                    self.buckets.to_owned(),
                    self.bloom_filters.to_owned(),
                    self.biggest_key_index.to_owned(),
                ));
                let sender_clone = Arc::clone(&self.background_channel_snd);
                // Trigger flush in background
                tokio::spawn(async move {
                    let job_res = flush_job.run().await;
                    if let Err(err) = sender_clone.write().await.send(job_res).await {
                        println!("Send to channel error {:?}", err);
                    }
                });
            }

            self.active_memtable = InMemoryTable::with_specified_capacity_and_rate(
                size_unit,
                capacity,
                false_positive_rate,
            );
        }
        let entry = Entry::new(
            key.to_vec(),
            v_offset.try_into().unwrap(),
            created_at,
            is_tombstone,
        );

        self.active_memtable.insert(&entry)?;
        Ok(true)
    }

    // A Result indicating success or an `io::Error` if an error occurred.
    pub async fn get(&self, key: &str) -> Result<(Vec<u8>, u64), StorageEngineError> {
        let key = key.as_bytes().to_vec();
        let mut offset = 0;
        let mut most_recent_insert_time = 0;

        //Step 1 > Check the active memtable
        if let Ok(Some((value_offset, creation_date, is_tombstone))) =
            self.active_memtable.get(&key)
        {
            offset = value_offset;
            most_recent_insert_time = creation_date;
            if is_tombstone {
                return Err(KeyFoundAsTombstoneInMemtableError);
            }
        } else {
            //Step 2 > Check the read only memtable
            let mut is_deleted = false;
            for (_, m_table) in self.read_only_memtables.iter() {
                if let Ok(Some((value_offset, creation_date, is_tombstone))) =
                    m_table.read().await.get(&key)
                {
                    if creation_date > most_recent_insert_time {
                        offset = value_offset;
                        most_recent_insert_time = creation_date;
                        is_deleted = is_tombstone;
                    }
                }
            }
            if most_recent_insert_time > 0 && is_deleted {
                return Err(KeyFoundAsTombstoneInMemtableError);
            } else if most_recent_insert_time == 0 {
                //Step 3 > Check the sstables
                let sstables_within_key_range =
                    &self.biggest_key_index.filter_sstables_by_biggest_key(&key);
                if sstables_within_key_range.is_empty() {
                    return Err(KeyNotFoundInAnySSTableError);
                }

                let bloom_filters_within_key_range = BloomFilter::bloom_filters_within_key_range(
                    &self.bloom_filters,
                    sstables_within_key_range.to_vec(),
                );
                if bloom_filters_within_key_range.is_empty() {
                    return Err(KeyNotFoundByAnyBloomFilterError);
                }

                let sstable_paths =
                    BloomFilter::sstables_within_key_range(bloom_filters_within_key_range, &key);
                match sstable_paths {
                    Some(sstables_within_key_range) => {
                        for sstable in sstables_within_key_range.iter() {
                            let sparse_index =
                                SparseIndex::new(sstable.index_file_path.clone()).await;

                            match sparse_index.get(&key).await {
                                Ok(None) => continue,
                                Ok(result) => {
                                    if let Some(block_offset) = result {
                                        let sst = SSTable::new_with_exisiting_file_path(
                                            sstable.dir.clone(),
                                            sstable.data_file_path.clone(),
                                            sstable.index_file_path.clone(),
                                        );
                                        match sst.get(block_offset, &key).await {
                                            Ok(None) => continue,
                                            Ok(result) => {
                                                if let Some((
                                                    value_offset,
                                                    created_at,
                                                    is_tombstone,
                                                )) = result
                                                {
                                                    if created_at > most_recent_insert_time {
                                                        offset = value_offset;
                                                        most_recent_insert_time = created_at;
                                                        is_deleted = is_tombstone;
                                                    }
                                                }
                                            }
                                            Err(err) => error!("{}", err),
                                        }
                                    }
                                }
                                Err(err) => error!("{}", err),
                            }
                        }

                        if most_recent_insert_time > 0 && is_deleted {
                            return Err(KeyFoundAsTombstoneInSSTableError);
                        }
                    }
                    None => {
                        return Err(KeyNotFoundInAnySSTableError);
                    }
                }
            }
        }

        // most_recent_insert_time cannot be zero unless did not find this key in any sstable
        if most_recent_insert_time > 0 {
            // Step 5: Read value from value log based on offset
            let value: Option<(Vec<u8>, bool)> = self.val_log.get(offset).await?;
            match value {
                Some((v, is_tombstone)) => {
                    if is_tombstone {
                        return Err(KeyFoundAsTombstoneInValueLogError);
                    }
                    return Ok((v, most_recent_insert_time));
                }
                None => return Err(KeyNotFoundInValueLogError),
            };
        }
        Err(NotFoundInDB)
    }

    pub async fn check_queued_updates(&mut self) {
        // Try getting response from previous flush operations
        for _ in 0..CHANNEL_BUFFER_SIZE {
            let response = self.background_channel_rcv.try_recv();
            match response {
                Ok(channel_response) => match channel_response {
                    Ok(response_variants) => match response_variants {
                        BackgroundResponse::FlushSuccessResponse {
                            table_id,
                            updated_bucket_map,
                            updated_bloom_filters,
                            updated_biggest_key_index,
                        } => {
                            self.bloom_filters = updated_bloom_filters;
                            self.buckets = updated_bucket_map;
                            self.biggest_key_index = updated_biggest_key_index;
                            self.read_only_memtables.remove(&table_id);
                        }
                    },
                    Err(err) => println!("Receiever error {:?}", err),
                },
                Err(err) => match err {
                    TryRecvError::Empty => {
                        break;
                    }
                    TryRecvError::Disconnected => {
                        println!("Sender disconnected {:?}", err);
                        break;
                    }
                },
            }
        }
    }
    /// A Result indicating success or an `io::Error` if an error occurred.
    pub async fn delete(&mut self, key: &str) -> Result<bool, StorageEngineError> {
        // check for any update in the channel
        self.check_queued_updates().await;
        // Return error if not
        self.get(key).await?;

        // Convert the key and value into Vec<u8> from given &str.
        let key = &key.as_bytes().to_vec();
        let value = &TOMB_STONE_MARKER.to_le_bytes().to_vec();
        let created_at = Utc::now().timestamp_millis() as u64;
        let is_tombstone = true;

        let v_offset = self
            .val_log
            .append(key, value, created_at, is_tombstone)
            .await?;

        // then check if memtable is full
        if self.active_memtable.is_full(HEAD_ENTRY_KEY.len()) {
            let capacity = self.active_memtable.capacity();
            let size_unit = self.active_memtable.size_unit();
            let false_positive_rate = self.active_memtable.false_positive_rate();
            let head_offset = self
                .active_memtable
                .index
                .iter()
                .max_by_key(|e| e.value().0);
            let head_entry = Entry::new(
                HEAD_ENTRY_KEY.to_vec(),
                head_offset.unwrap().value().0,
                Utc::now().timestamp_millis() as u64,
                is_tombstone,
            );
            let _ = self.active_memtable.insert(&head_entry);
            self.active_memtable.read_only = true;
            self.read_only_memtables.insert(
                InMemoryTable::generate_table_id(),
                Arc::new(RwLock::new(self.active_memtable.to_owned())),
            );

            if self.read_only_memtables.len() >= self.config.max_buffer_write_number {
                let (table_id, table_to_flush) = self.read_only_memtables.iter().next().unwrap();
                let mut flush_job = BackgroundJob::Flush(FlushData::new(
                    Arc::clone(table_to_flush),
                    table_id.to_owned(),
                    self.buckets.to_owned(),
                    self.bloom_filters.to_owned(),
                    self.biggest_key_index.to_owned(),
                ));
                let sender_clone = Arc::clone(&self.background_channel_snd);
                // Trigger flush in background
                tokio::spawn(async move {
                    let job_res = flush_job.run().await;
                    if let Err(err) = sender_clone.write().await.send(job_res).await {
                        println!("Send to channel error {:?}", err);
                    }
                });

                self.active_memtable = InMemoryTable::with_specified_capacity_and_rate(
                    size_unit,
                    capacity,
                    false_positive_rate,
                );
            }
        }

        let entry = Entry::new(
            key.to_vec(),
            v_offset.try_into().unwrap(),
            created_at,
            is_tombstone,
        );
        self.active_memtable.insert(&entry)?;
        Ok(true)
    }

    pub async fn update(&mut self, key: &str, value: &str) -> Result<bool, StorageEngineError> {
        // Call set method defined in StorageEngine.
        self.put(key, value).await
    }

    pub async fn clear(&mut self) -> Result<Self, StorageEngineError> {
        let capacity = self.active_memtable.capacity();

        let size_unit = self.active_memtable.size_unit();

        self.active_memtable.clear();

        let config = self.config.clone();

        self.buckets.clear_all().await;

        self.val_log.clear_all().await;

        StorageEngine::with_capacity_and_rate(self.dir.clone(), size_unit, capacity, &config).await
    }

    async fn with_default_capacity_and_config(
        dir: DirPath,
        size_unit: SizeUnit,
        capacity: usize,
        config: &Config,
    ) -> Result<Self, StorageEngineError> {
        Self::with_capacity_and_rate(dir, size_unit, capacity, &config).await
    }

    async fn with_capacity_and_rate(
        dir: DirPath,
        size_unit: SizeUnit,
        capacity: usize,
        config: &Config,
    ) -> Result<Self, StorageEngineError> {
        let vlog_path = &dir.clone().val_log;
        let buckets_path = dir.buckets.clone();
        let vlog_exit = vlog_path.exists();
        let vlog_empty =
            !vlog_exit || fs::metadata(vlog_path).map_err(GetFileMetaDataError)?.len() == 0;

        let biggest_key_index = TableBiggestKeys::new();
        let mut vlog = ValueLog::new(vlog_path).await?;
        let meta = Meta::new(&dir.meta);
        if vlog_empty {
            let mut active_memtable = InMemoryTable::with_specified_capacity_and_rate(
                size_unit,
                capacity,
                config.false_positive_rate,
            );

            // if ValueLog is empty then we want to insert both tail and head
            let created_at = Utc::now().timestamp_millis() as u64;

            let tail_offset = vlog
                .append(&TAIL_ENTRY_KEY.to_vec(), &vec![], created_at, false)
                .await?;
            let tail_entry = Entry::new(TAIL_ENTRY_KEY.to_vec(), tail_offset, created_at, false);

            let head_offset = vlog
                .append(&HEAD_ENTRY_KEY.to_vec(), &vec![], created_at, false)
                .await?;
            let head_entry = Entry::new(HEAD_ENTRY_KEY.to_vec(), head_offset, created_at, false);

            vlog.set_head(head_offset);
            vlog.set_tail(tail_offset);

            // insert tail and head to memtable
            active_memtable.insert(&tail_entry.to_owned())?;
            active_memtable.insert(&head_entry.to_owned())?;
            let (sender, receiver) = mpsc::channel(CHANNEL_BUFFER_SIZE);
            let read_only_memtables = HashMap::new();
            return Ok(Self {
                active_memtable,
                val_log: vlog,
                bloom_filters: Vec::new(),
                buckets: BucketMap::new(buckets_path),
                dir,
                biggest_key_index: biggest_key_index,
                compactor: Compactor::new(config.enable_ttl, config.entry_ttl_millis),
                config: config.clone(),
                meta,
                read_only_memtables,
                background_channel_rcv: receiver,
                background_channel_snd: Arc::new(RwLock::new(sender)),
            });
        }

        let mut recovered_buckets: HashMap<uuid::Uuid, Bucket> = HashMap::new();
        let mut bloom_filters: Vec<BloomFilter> = Vec::new();
        let mut most_recent_head_timestamp = 0;
        let mut most_recent_head_offset = 0;

        let mut most_recent_tail_timestamp = 0;
        let mut most_recent_tail_offset = 0;

        // engine_root/buckets/bucket{id}
        for buckets_directories in
            fs::read_dir(buckets_path.clone()).map_err(|err| BucketDirectoryOpenError {
                path: buckets_path.clone(),
                error: err,
            })?
        {
            //  engine_root/buckets/bucket{id}/sstable_{timestamp}
            for sstable_dir in
                fs::read_dir(buckets_directories.as_ref().unwrap().path()).map_err(|err| {
                    BucketDirectoryOpenError {
                        path: buckets_directories.as_ref().unwrap().path(),
                        error: err,
                    }
                })?
            {
                // engine_root/buckets/bucket{id}/sstable_{timestamp}/index_{timestamp}_.db
                // engine_root/buckets/bucket{id}/sstable_{timestamp}/sstable_{timestamp}_.db
                let mut sst_files: Vec<PathBuf> = Vec::new();
                for files in fs::read_dir(sstable_dir.as_ref().unwrap().path()).map_err(|err| {
                    BucketDirectoryOpenError {
                        path: sstable_dir.as_ref().unwrap().path(),
                        error: err,
                    }
                })? {
                    if let Ok(entry) = files {
                        let file_path = entry.path();
                        // Check if the entry is a file
                        if file_path.is_file() {
                            sst_files.push(file_path)
                        }
                    }
                }
                // Can't guarantee order that the files are retrived so sort for order
                sst_files.sort();
                // Extract bucket id
                let bucket_id = Self::get_bucket_id_from_full_bucket_path(
                    sstable_dir.as_ref().unwrap().path().clone(),
                );

                // We expect two files, data file and index file
                if sst_files.len() < 2 {
                    return Err(InvalidSSTableDirectoryError {
                        input_string: sstable_dir
                            .as_ref()
                            .unwrap()
                            .path()
                            .to_string_lossy()
                            .to_string(),
                    });
                }
                let data_file_path = sst_files[1].to_owned();
                let index_file_path = sst_files[0].to_owned();
                let sst_path = SSTablePath::new(
                    sstable_dir.as_ref().unwrap().path(),
                    data_file_path.clone(),
                    index_file_path.clone(),
                );

                let bucket_uuid =
                    uuid::Uuid::parse_str(&bucket_id).map_err(|err| InvaidUUIDParseString {
                        input_string: bucket_id,
                        error: err,
                    })?;
                // If bucket already exist in recovered bucket then just append sstable to its sstables vector
                if let Some(b) = recovered_buckets.get(&bucket_uuid) {
                    let mut temp_sstables = b.sstables.clone();
                    temp_sstables.push(sst_path.clone());
                    let updated_bucket = Bucket::new_with_id_dir_average_and_sstables(
                        buckets_directories.as_ref().unwrap().path(),
                        bucket_uuid,
                        temp_sstables.to_owned(),
                        0,
                    )
                    .await?;
                    recovered_buckets.insert(bucket_uuid, updated_bucket);
                } else {
                    // Create new bucket
                    let updated_bucket = Bucket::new_with_id_dir_average_and_sstables(
                        buckets_directories.as_ref().unwrap().path(),
                        bucket_uuid,
                        vec![sst_path.clone()],
                        0,
                    )
                    .await?;
                    recovered_buckets.insert(bucket_uuid, updated_bucket);
                }

                let sstable_from_file = SSTable::from_file(
                    sstable_dir.unwrap().path(),
                    data_file_path,
                    index_file_path,
                )
                .await?;
                let sstable = sstable_from_file.unwrap();
                // Fetch the most recent write offset so it can
                // use it to recover entries not written into sstables from value log
                let head_entry = sstable.get_value_from_index(HEAD_ENTRY_KEY);

                let tail_entry = sstable.get_value_from_index(TAIL_ENTRY_KEY);

                // update head
                if let Some((head_offset, date_created, _)) = head_entry {
                    if date_created > most_recent_head_timestamp {
                        most_recent_head_offset = head_offset;
                        most_recent_head_timestamp = date_created;
                    }
                }

                // update tail
                if let Some((tail_offset, date_created, _)) = tail_entry {
                    if date_created > most_recent_tail_timestamp {
                        most_recent_tail_offset = tail_offset;
                        most_recent_tail_timestamp = date_created;
                    }
                }

                let mut bf = SSTable::build_bloomfilter_from_sstable(&sstable.index);
                bf.set_sstable_path(sst_path.clone());
                // update bloom filters
                bloom_filters.push(bf)
            }
        }
        let mut buckets_map = BucketMap::new(buckets_path.clone());
        buckets_map.set_buckets(recovered_buckets);

        // store vLog head and tail in memory
        vlog.set_head(most_recent_head_offset);
        vlog.set_tail(most_recent_tail_offset);

        // recover memtable
        let recover_result = StorageEngine::recover_memtable(
            size_unit,
            capacity,
            config.false_positive_rate,
            &dir.val_log,
            most_recent_head_offset,
        )
        .await;
        let (sender, receiver) = mpsc::channel(CHANNEL_BUFFER_SIZE);
        match recover_result {
            Ok((active_memtable, read_only_memtables)) => Ok(Self {
                active_memtable,
                val_log: vlog,
                dir,
                buckets: buckets_map,
                bloom_filters,
                biggest_key_index,
                meta,
                compactor: Compactor::new(config.enable_ttl, config.entry_ttl_millis),
                config: config.clone(),
                read_only_memtables,
                background_channel_rcv: receiver,
                background_channel_snd: Arc::new(RwLock::new(sender)),
            }),
            Err(err) => Err(MemTableRecoveryError(Box::new(err))),
        }
    }
    async fn recover_memtable(
        size_unit: SizeUnit,
        capacity: usize,
        false_positive_rate: f64,
        vlog_path: &PathBuf,
        head_offset: usize,
    ) -> Result<
        (
            InMemoryTable<Vec<u8>>,
            HashMap<Vec<u8>, Arc<RwLock<InMemoryTable<Vec<u8>>>>>,
        ),
        StorageEngineError,
    > {
        let mut read_only_memtables: HashMap<Vec<u8>, Arc<RwLock<InMemoryTable<Vec<u8>>>>> =
            HashMap::new();
        let mut active_memtable = InMemoryTable::with_specified_capacity_and_rate(
            size_unit,
            capacity,
            false_positive_rate,
        );

        let mut vlog = ValueLog::new(&vlog_path.clone()).await?;
        let mut most_recent_offset = head_offset;
        let entries = vlog.recover(head_offset).await?;

        for e in entries {
            let entry = Entry::new(
                e.key.to_owned(),
                most_recent_offset,
                e.created_at,
                e.is_tombstone,
            );
            // Since the most recent offset is the offset we start reading entries from in value log
            // and we retrieved this from the sstable, therefore should not re-write the initial entry in
            // memtable since it's already in the sstable
            if most_recent_offset != head_offset {
                if active_memtable.is_full(e.key.len()) {
                    // Make memtable read only
                    active_memtable.read_only = true;
                    read_only_memtables.insert(
                        InMemoryTable::generate_table_id(),
                        Arc::new(RwLock::new(active_memtable.to_owned())),
                    );
                    active_memtable = InMemoryTable::with_specified_capacity_and_rate(
                        size_unit,
                        capacity,
                        false_positive_rate,
                    );
                }
                active_memtable.insert(&entry)?;
            }
            most_recent_offset += SIZE_OF_U32// Key Size -> for fetching key length
                        +SIZE_OF_U32// Value Length -> for fetching value length
                        + SIZE_OF_U64 // Date Length
                        + SIZE_OF_U8 // tombstone marker
                        + e.key.len() // Key Length
                        + e.value.len(); // Value Length
        }

        Ok((active_memtable, read_only_memtables))
    }
    // Flush all memtables
    async fn flush_all_memtables(&mut self) -> Result<(), StorageEngineError> {
        // Flush active memtable
        let hotness = 1;
        self.flush_memtable(
            &Arc::new(RwLock::new(self.active_memtable.to_owned())),
            hotness,
        )
        .await?;

        // Flush all read-only memtables
        let memtable_iterator = self.read_only_memtables.iter();
        let mut read_only_memtables = Vec::new();
        for (_, mem) in memtable_iterator {
            read_only_memtables.push(Arc::clone(&mem))
        }

        for memtable in read_only_memtables {
            self.flush_memtable(&memtable, hotness).await?;
        }

        // Sort bloom filter by hotness after flushing read-only memtables
        self.bloom_filters.sort_by(|a, b| {
            b.get_sstable_path()
                .get_hotness()
                .cmp(&a.get_sstable_path().get_hotness())
        });

        // clear the memtables
        self.active_memtable.clear();
        self.read_only_memtables = HashMap::new();
        Ok(())
    }

    async fn flush_memtable(
        &mut self,
        memtable: &Arc<RwLock<InMemoryTable<Vec<u8>>>>,
        hotness: u64,
    ) -> Result<(), StorageEngineError> {
        let sstable_path = self
            .buckets
            .insert_to_appropriate_bucket(&memtable.read().await.to_owned(), hotness)
            .await?;

        // Write the memtable to disk as SSTables
        // Insert to bloom filter
        let mut bf = memtable.read().await.get_bloom_filter();
        bf.set_sstable_path(sstable_path.clone());
        self.bloom_filters.push(bf);

        let biggest_key = memtable.read().await.find_biggest_key()?;
        self.biggest_key_index
            .set(sstable_path.get_data_file_path(), biggest_key);

        Ok(())
    }

    pub async fn run_compaction(&mut self) -> Result<bool, StorageEngineError> {
        self.check_queued_updates().await;
        self.compactor
            .run_compaction(
                &mut self.buckets,
                &mut self.bloom_filters.clone(),
                &mut self.biggest_key_index.clone(),
            )
            .await
    }

    fn get_bucket_id_from_full_bucket_path(full_path: PathBuf) -> String {
        let full_path_as_str = full_path.to_string_lossy().to_string();
        let mut bucket_id = String::new();
        // Find the last occurrence of "bucket" in the file path
        if let Some(idx) = full_path_as_str.rfind("bucket") {
            // Extract the substring starting from the index after the last occurrence of "bucket"
            let uuid_part = &full_path_as_str[idx + "bucket".len()..];
            if let Some(end_idx) = uuid_part.find('/') {
                // Extract the UUID
                let uuid = &uuid_part[..end_idx];
                bucket_id = uuid.to_string();
            }
        }
        bucket_id
    }
}

impl DirPath {
    pub(crate) fn build(root_path: PathBuf) -> Self {
        let root = root_path;
        let val_log = root.join(VALUE_LOG_DIRECTORY_NAME);
        let buckets = root.join(BUCKETS_DIRECTORY_NAME);
        let meta = root.join(META_DIRECTORY_NAME);
        Self {
            root,
            val_log,
            buckets,
            meta,
        }
    }
}
impl SizeUnit {
    pub(crate) const fn to_bytes(&self, value: usize) -> usize {
        match self {
            SizeUnit::Bytes => value,
            SizeUnit::Kilobytes => value * 1024,
            SizeUnit::Megabytes => value * 1024 * 1024,
            SizeUnit::Gigabytes => value * 1024 * 1024 * 1024,
        }
    }
}

#[cfg(test)]
mod tests {

    use log::info;
    use rand::distributions::Alphanumeric;
    use rand::{thread_rng, Rng};
    use std::sync::Arc;

    use super::*;

    use tokio::fs;
    use tokio::sync::RwLock;
    fn generate_random_string(length: usize) -> String {
        let rng = thread_rng();
        rng.sample_iter(&Alphanumeric)
            .take(length)
            .map(|c| c as char)
            .collect()
    }
    // Generate test to find keys after compaction
    #[tokio::test]
    async fn storage_engine_create_asynchronous() {
        let path = PathBuf::new().join("bump1");
        let s_engine = StorageEngine::new(path.clone()).await.unwrap();
        // Specify the number of random strings to generate
        let num_strings = 50000;

        // Specify the length of each random string
        let string_length = 10;
        // Generate random strings and store them in a vector
        let mut random_strings: Vec<String> = Vec::new();
        for _ in 0..num_strings {
            let random_string = generate_random_string(string_length);
            random_strings.push(random_string);
        }
        // for k in random_strings.clone() {
        //     s_engine.put(&k, "boyode").await.unwrap();
        // }
        let sg = Arc::new(RwLock::new(s_engine));
        let tasks = random_strings.iter().map(|k| {
            let s_engine = Arc::clone(&sg);
            let k = k.clone();
            tokio::spawn(async move {
                let mut value = s_engine.write().await;
                let resp = value.put(&k, "boy").await;
                match resp {
                    Ok(v) => {
                        assert_eq!(v, true)
                    }
                    Err(_) => {
                        assert!(false, "No err should be found")
                    }
                }
            })
        });

        for task in tasks {
            tokio::select! {
                    result = task => {
                        match result{Ok(v_opt)=>{}
                            Err(_) => todo!(), }
                }
            }
        }

        let s_engine = Arc::clone(&sg);
        let compaction_opt = s_engine.write().await.run_compaction().await;
        match compaction_opt {
            Ok(_) => {
                println!("Compaction is successful");
                println!(
                    "Length of bucket after compaction {:?}",
                    s_engine.read().await.buckets.buckets.len()
                );
                println!(
                    "Length of bloom filters after compaction {:?}",
                    s_engine.read().await.bloom_filters.len()
                );
            }
            Err(err) => {
                println!("Error during compaction {}", err)
            }
        }

        random_strings.sort();
        println!("About to start reading");
        let tasks = random_strings.iter().map(|k| {
            let s_engine = Arc::clone(&sg);
            let k = k.clone();
            tokio::spawn(async move {
                let value = s_engine.read().await;
                value.get(&k).await
            })
        });

        for task in tasks {
            tokio::select! {
                result = task => {
                    match result {
                        Ok(v) => {
                            assert_eq!(v.unwrap().0, b"boy");
                        }
                        Err(_) => {
                            assert!(false, "No error should be found");
                        }
                    }
                }
            }
        }

        let _ = fs::remove_dir_all(path.clone()).await;
    }

    #[tokio::test]
    async fn storage_engine_create_synchronous() {
        let path = PathBuf::new().join("bump2");
        let mut s_engine = StorageEngine::new(path.clone()).await.unwrap();

        // Specify the number of random strings to generate
        let num_strings = 50000;

        // Specify the length of each random string
        let string_length = 10;
        // Generate random strings and store them in a vector
        let mut random_strings: Vec<String> = Vec::new();
        for _ in 0..num_strings {
            let random_string = generate_random_string(string_length);
            random_strings.push(random_string);
        }

        // Insert the generated random strings
        for (_, s) in random_strings.iter().enumerate() {
            s_engine.put(s, "boyode").await.unwrap();
        }
        // let compactor = Compactor::new();

        let compaction_opt = s_engine.run_compaction().await;
        match compaction_opt {
            Ok(_) => {
                println!("Compaction is successful");
                println!(
                    "Length of bucket after compaction {:?}",
                    s_engine.buckets.buckets.len()
                );
                println!(
                    "Length of bloom filters after compaction {:?}",
                    s_engine.bloom_filters.len()
                );
            }
            Err(err) => {
                println!("Error during compaction {}", err)
            }
        }

        // random_strings.sort();
        for k in random_strings {
            let result = s_engine.get(&k).await;
            match result {
                Ok((value, _)) => {
                    assert_eq!(value, b"boyode");
                }
                Err(_) => {
                    assert!(false, "No err should be found");
                }
            }
        }

        let _ = fs::remove_dir_all(path.clone()).await;
        // sort to make fetch random
    }

    #[tokio::test]
    async fn storage_engine_compaction_asynchronous() {
        let path = PathBuf::new().join("bump3");
        let s_engine = StorageEngine::new(path.clone()).await.unwrap();

        // Specify the number of random strings to generate
        let num_strings = 50000;

        // Specify the length of each random string
        let string_length = 10;
        // Generate random strings and store them in a vector
        let mut random_strings: Vec<String> = Vec::new();
        for _ in 0..num_strings {
            let random_string = generate_random_string(string_length);
            random_strings.push(random_string);
        }
        // for k in random_strings.clone() {
        //     s_engine.put(&k, "boyode").await.unwrap();
        // }
        let sg = Arc::new(RwLock::new(s_engine));
        let binding = random_strings.clone();
        let tasks = binding.iter().map(|k| {
            let s_engine = Arc::clone(&sg);
            let k = k.clone();
            tokio::spawn(async move {
                let mut value = s_engine.write().await;
                value.put(&k, "boyode").await
            })
        });

        //Collect the results from the spawned tasks
        for task in tasks {
            tokio::select! {
                result = task => {
                    match result{
                        Ok(v_opt)=>{
                            match v_opt{
                                Ok(v) => {
                                    assert_eq!(v, true)
                                },
                                Err(_) => { assert!(false, "No err should be found")},
                            }
                             }
                        Err(_) =>  assert!(false, "No err should be found") }
                    //println!("{:?}",result);
                }
            }
        }

        // sort to make fetch random
        random_strings.sort();
        let key = &random_strings[0];

        let get_res1 = sg.read().await.get(key).await;
        let get_res2 = sg.read().await.get(key).await;
        let get_res3 = sg.read().await.get(key).await;
        let get_res4 = sg.read().await.get(key).await;
        match get_res1 {
            Ok(v) => {
                assert_eq!(v.0, b"boyode");
            }
            Err(_) => {
                assert!(false, "No error should be found");
            }
        }

        match get_res2 {
            Ok(v) => {
                assert_eq!(v.0, b"boyode");
            }
            Err(_) => {
                assert!(false, "No error should be found");
            }
        }

        match get_res3 {
            Ok(v) => {
                assert_eq!(v.0, b"boyode");
            }
            Err(_) => {
                assert!(false, "No error should be found");
            }
        }
        match get_res4 {
            Ok(v) => {
                assert_eq!(v.0, b"boyode");
            }
            Err(_) => {
                assert!(false, "No error should be found");
            }
        }

        let del_res = sg.write().await.delete(key).await;
        match del_res {
            Ok(v) => {
                assert_eq!(v, true)
            }
            Err(_) => {
                assert!(false, "No error should be found");
            }
        }

        let get_res2 = sg.read().await.get(key).await;
        match get_res2 {
            Ok(_) => {
                assert!(false, "Should not be found after compaction")
            }
            Err(err) => {
                assert_eq!(
                    StorageEngineError::KeyFoundAsTombstoneInMemtableError.to_string(),
                    err.to_string()
                )
            }
        }

        let _ = sg.write().await.flush_all_memtables().await;
        sg.write().await.active_memtable.clear();

        // We expect tombstone to be flushed to an sstable at this point
        let get_res2 = sg.read().await.get(key).await;
        match get_res2 {
            Ok(_) => {
                assert!(false, "Should not be found after compaction")
            }
            Err(err) => {
                assert_eq!(
                    StorageEngineError::KeyFoundAsTombstoneInSSTableError.to_string(),
                    err.to_string()
                )
            }
        }

        let compaction_opt = sg.write().await.run_compaction().await;
        // Insert the generated random strings
        // let compactor = Compactor::new();
        // let compaction_opt = sg.write().await.run_compaction().await;
        match compaction_opt {
            Ok(_) => {
                println!("Compaction is successful");
                println!(
                    "Length of bucket after compaction {:?}",
                    sg.read().await.buckets.buckets.len()
                );
                println!(
                    "Length of bloom filters after compaction {:?}",
                    sg.read().await.bloom_filters.len()
                );
            }
            Err(err) => {
                info!("Error during compaction {}", err)
            }
        }

        // Insert the generated random strings
        let get_res3 = sg.read().await.get(key).await;
        match get_res3 {
            Ok(_) => {
                assert!(false, "Deleted key should be found as tumbstone");
            }

            Err(err) => {
                println!("{}", err);
                if err.to_string() != KeyFoundAsTombstoneInSSTableError.to_string()
                    && err.to_string() != KeyNotFoundInAnySSTableError.to_string()
                {
                    println!("{}", err);
                    assert!(
                        false,
                        "Key should be mapped to tombstone or deleted from all sstables"
                    )
                }
            }
        }
        let _ = fs::remove_dir_all(path.clone()).await;
    }

    #[tokio::test]
    async fn storage_engine_update_asynchronous() {
        let path = PathBuf::new().join("bump4");
        let mut s_engine = StorageEngine::new(path.clone()).await.unwrap();

        // Specify the number of random strings to generate
        let num_strings = 6000;

        // Specify the length of each random string
        let string_length = 10;
        // Generate random strings and store them in a vector
        let mut random_strings: Vec<String> = Vec::new();
        for _ in 0..num_strings {
            let random_string = generate_random_string(string_length);
            random_strings.push(random_string);
        }
        for k in random_strings.clone() {
            s_engine.put(&k, "boyode").await.unwrap();
        }
        let sg = Arc::new(RwLock::new(s_engine));
        let binding = random_strings.clone();
        let tasks = binding.iter().map(|k| {
            let s_engine = Arc::clone(&sg);
            let k = k.clone();
            tokio::spawn(async move {
                let mut value = s_engine.write().await;
                value.put(&k, "boyode").await
            })
        });

        // Collect the results from the spawned tasks
        for task in tasks {
            tokio::select! {
                result = task => {
                    match result{
                        Ok(v_opt)=>{
                            match v_opt{
                                Ok(v) => {
                                    assert_eq!(v, true)
                                },
                                Err(_) => { assert!(false, "No err should be found")},
                            }
                             }
                        Err(_) =>  assert!(false, "No err should be found") }
                }
            }
        }
        // // sort to make fetch random
        random_strings.sort();
        let key = &random_strings[0];
        let updated_value = "updated_key";

        let get_res = sg.read().await.get(key).await;
        match get_res {
            Ok(v) => {
                assert_eq!(v.0, b"boyode");
            }
            Err(_) => {
                assert!(false, "No error should be found");
            }
        }

        let update_res = sg.write().await.update(key, updated_value).await;
        match update_res {
            Ok(v) => {
                assert_eq!(v, true)
            }
            Err(_) => {
                assert!(false, "No error should be found");
            }
        }
        let _ = sg.write().await.flush_all_memtables().await;
        sg.write().await.active_memtable.clear();

        let get_res = sg.read().await.get(key).await;
        match get_res {
            Ok((value, _)) => {
                assert_eq!(value, updated_value.as_bytes().to_vec())
            }
            Err(_) => {
                assert!(false, "Should not run")
            }
        }

        // // Run compaction
        let compaction_opt = sg.write().await.run_compaction().await;
        match compaction_opt {
            Ok(_) => {
                println!("Compaction is successful");
                println!(
                    "Length of bucket after compaction {:?}",
                    sg.read().await.buckets.buckets.len()
                );
                println!(
                    "Length of bloom filters after compaction {:?}",
                    sg.read().await.bloom_filters.len()
                );
            }
            Err(err) => {
                info!("Error during compaction {}", err)
            }
        }

        let get_res = sg.read().await.get(key).await;
        match get_res {
            Ok((value, _)) => {
                assert_eq!(value, updated_value.as_bytes().to_vec())
            }
            Err(_) => {
                assert!(false, "Should not run")
            }
        }
        let _ = fs::remove_dir_all(path.clone()).await;
    }

    #[tokio::test]
    async fn storage_engine_deletion_asynchronous() {
        let path = PathBuf::new().join("bump5");
        let s_engine = StorageEngine::new(path.clone()).await.unwrap();

        // Specify the number of random strings to generate
        let num_strings = 60000;

        // Specify the length of each random string
        let string_length = 10;
        // Generate random strings and store them in a vector
        let mut random_strings: Vec<String> = Vec::new();
        for _ in 0..num_strings {
            let random_string = generate_random_string(string_length);
            random_strings.push(random_string);
        }
        // for k in random_strings.clone() {
        //     s_engine.put(&k, "boyode").await.unwrap();
        // }
        let sg = Arc::new(RwLock::new(s_engine));
        let binding = random_strings.clone();
        let tasks = binding.iter().map(|k| {
            let s_engine = Arc::clone(&sg);
            let k = k.clone();
            tokio::spawn(async move {
                let mut value = s_engine.write().await;
                value.put(&k, "boyode").await
            })
        });
        let key = "aunkanmi";
        let _ = sg.write().await.put(key, "boyode").await;
        // // Collect the results from the spawned tasks
        for task in tasks {
            tokio::select! {
                result = task => {
                    match result{
                        Ok(v_opt)=>{
                            match v_opt{
                                Ok(v) => {
                                    assert_eq!(v, true)
                                },
                                Err(_) => { assert!(false, "No err should be found")},
                            }
                             }
                        Err(_) =>  assert!(false, "No err should be found") }
                }
            }
        }
        // sort to make fetch random
        random_strings.sort();
        let get_res = sg.read().await.get(key).await;
        match get_res {
            Ok((value, _)) => {
                assert_eq!(value, "boyode".as_bytes().to_vec());
            }
            Err(err) => {
                assert_ne!(key.as_bytes().to_vec(), err.to_string().as_bytes().to_vec());
            }
        }

        let del_res = sg.write().await.delete(key).await;
        match del_res {
            Ok(v) => {
                assert_eq!(v, true);
            }
            Err(err) => {
                assert!(err.to_string().is_empty())
            }
        }

        let _ = sg.write().await.flush_all_memtables().await;

        let get_res = sg.read().await.get(key).await;
        match get_res {
            Ok((_, _)) => {
                assert!(false, "Should not be executed")
            }
            Err(err) => {
                assert_eq!(
                    KeyFoundAsTombstoneInSSTableError.to_string(),
                    err.to_string()
                )
            }
        }

        let compaction_opt = sg.write().await.run_compaction().await;
        match compaction_opt {
            Ok(_) => {
                println!("Compaction is successful");
                println!(
                    "Length of bucket after compaction {:?}",
                    sg.read().await.buckets.buckets.len()
                );
                println!(
                    "Length of bloom filters after compaction {:?}",
                    sg.read().await.bloom_filters.len()
                );
            }
            Err(err) => {
                info!("Error during compaction {}", err)
            }
        }

        // Insert the generated random strings
        println!("trying to get this after compaction {}", key);
        let get_res = sg.read().await.get(key).await;
        match get_res {
            Ok((_, _)) => {
                assert!(false, "Should not ne executed")
            }
            Err(err) => {
                if err.to_string() != KeyFoundAsTombstoneInSSTableError.to_string()
                    && err.to_string() != KeyNotFoundInAnySSTableError.to_string()
                {
                    assert!(
                        false,
                        "Key should be mapped to tombstone or deleted from all sstables"
                    )
                }
            }
        }
        let _ = fs::remove_dir_all(path.clone()).await;
    }
}
