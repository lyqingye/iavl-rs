use std::cell::RefCell;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::{Arc, RwLock};
use anyhow::*;
use rocksdb:: {
    ReadOptions,
    WriteOptions,
    Cache,
    Options,
    BlockBasedOptions,
};

pub trait DB<Bat: Batch>: Clone + 'static {

    fn get(&self,key: &[u8]) -> Result<&[u8]>;

    fn has(&self,key: &[u8]) -> Result<()>;

    fn set(&mut self,key: &[u8], value: &[u8]) -> Result<()>;

    fn set_sync(&mut self,key: &[u8], value: &[u8]) -> Result<()>;

    fn delete(&mut self,key: &[u8]) -> Result<()>;

    fn delete_sync(&mut self,key: &[u8]) -> Result<()>;

    fn close(&mut self) -> Result<()>;

    fn new_batch(&self) -> Bat;
}

pub trait Batch: Clone + 'static {

    fn set(&mut self,key: &[u8], value: &[u8]) -> Result<()>;

    fn delete(&mut self,key: &[u8]) -> Result<()>;

    fn write(&mut self) -> Result<()>;

    fn write_sync(&mut self) -> Result<()>;

    fn close(&self) -> Result<()>;
}


#[derive(Clone)]
pub struct RocksDB {
    inner: Arc<RwLock<RocksDBInner>>,
}

unsafe impl Sync for RocksDB { }
unsafe impl Send for RocksDB { }

struct RocksDBInner {
    db: rocksdb::DB,
    ro: rocksdb::ReadOptions,
    wo: rocksdb::WriteOptions,
    wo_sync: rocksdb::WriteOptions,
}

pub fn new_rocks_db(name: &str,dir: &Path) -> Result<RocksDB> {
    let mut bbto = BlockBasedOptions::default();
    let cache = Cache::new_lru_cache(1 << 30)
        .map_err(|_| anyhow!("panic"))?;
    bbto.set_block_cache(&cache);
    bbto.set_bloom_filter(10.0,true);

    let mut opts = Options::default();
    opts.set_block_based_table_factory(&bbto);
    opts.create_if_missing(true);
    opts.increase_parallelism(num_cpus::get() as i32);
    opts.optimize_level_style_compaction(512 * 1024 * 1024);

    let db_path = dir.join(format!("{}.db",name));
    let db = rocksdb::DB::open(&opts,db_path)
        .map_err(|_| anyhow!("open db fail"))?;

    let ro = ReadOptions::default();
    let wo = WriteOptions::default();
    let mut wo_sync = WriteOptions::default();
    wo_sync.set_sync(true);

    Ok(RocksDB{
        inner: Arc::new(RwLock::new(
            RocksDBInner {
                db,
                ro,
                wo,
                wo_sync,
            }
        ))
    })
}

impl <Bat: Batch> DB<Bat> for RocksDB {
    fn get(&self, key: &[u8]) -> Result<&[u8]> {
        let inner = self.inner.read().map_err(|_| anyhow!("faild to get read lock"))?;
    }

    fn has(&self, key: &[u8]) -> Result<()> {
        todo!()
    }

    fn set(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        todo!()
    }

    fn set_sync(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        todo!()
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        todo!()
    }

    fn delete_sync(&mut self, key: &[u8]) -> Result<()> {
        todo!()
    }

    fn close(&mut self) -> Result<()> {
        todo!()
    }

    fn new_batch(&self) -> Bat {
        todo!()
    }
}