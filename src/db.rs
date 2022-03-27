use anyhow::*;
use rocksdb::{BlockBasedOptions, Cache, Options, ReadOptions, WriteOptions};
use std::any::Any;
use std::cell::RefCell;
use std::path::Path;
use std::rc::Rc;

use crate::error::DBError;

pub trait DB {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    fn has(&self, key: &[u8]) -> Result<bool>;

    fn set(&mut self, key: &[u8], value: &[u8]) -> Result<()>;

    fn set_sync(&mut self, key: &[u8], value: &[u8]) -> Result<()>;

    fn delete(&mut self, key: &[u8]) -> Result<()>;

    fn delete_sync(&mut self, key: &[u8]) -> Result<()>;

    fn new_batch(&mut self) -> Box<dyn Batch>;

    fn write_batch(&mut self, batch: Box<dyn Batch>) -> Result<()>;

    fn write_batch_sync(&mut self, batch: Box<dyn Batch>) -> Result<()>;
}

pub trait Batch {
    fn set(&mut self, key: &[u8], value: &[u8]) -> Result<()>;

    fn delete(&mut self, key: &[u8]) -> Result<()>;

    fn as_any(&self) -> &dyn Any;
}

#[derive(Clone)]
pub struct RocksDB {
    inner: Rc<Inner>,
}

struct Inner {
    db: rocksdb::DB,
    ro: rocksdb::ReadOptions,
    wo: rocksdb::WriteOptions,
    wo_sync: rocksdb::WriteOptions,
}

pub fn new_rocks_db(name: &str, dir: &Path) -> Result<RocksDB> {
    let mut bbto = BlockBasedOptions::default();
    let cache = Cache::new_lru_cache(1 << 30).map_err(|e| DBError::WrapError(e.to_string()))?;
    bbto.set_block_cache(&cache);
    bbto.set_bloom_filter(10.0, true);

    let mut opts = Options::default();
    opts.set_block_based_table_factory(&bbto);
    opts.create_if_missing(true);
    opts.increase_parallelism(num_cpus::get() as i32);
    opts.optimize_level_style_compaction(512 * 1024 * 1024);

    let db_path = dir.join(format!("{}.db", name));
    let db = rocksdb::DB::open(&opts, db_path).map_err(|e| DBError::WrapError(e.to_string()))?;

    let ro = ReadOptions::default();
    let wo = WriteOptions::default();
    let mut wo_sync = WriteOptions::default();
    wo_sync.set_sync(true);

    Ok(RocksDB {
        inner: Rc::new(Inner {
            db,
            ro,
            wo,
            wo_sync,
        }),
    })
}

impl DB for RocksDB {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if key.is_empty() {
            return Err(DBError::EmptyKey.into());
        }
        self.inner
            .db
            .get_opt(key, &self.inner.ro)
            .map_err(|e| DBError::WrapError(e.to_string()).into())
    }

    fn has(&self, key: &[u8]) -> Result<bool> {
        if key.is_empty() {
            return Err(DBError::EmptyKey.into());
        }
        Ok(self.inner.db.key_may_exist(key))
    }

    fn set(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        if key.is_empty() {
            return Err(DBError::EmptyKey.into());
        }
        if value.is_empty() {
            return Err(DBError::EmptyValue.into());
        }
        self.inner
            .db
            .put_opt(key, value, &self.inner.wo)
            .map_err(|e| DBError::WrapError(e.to_string()).into())
    }

    fn set_sync(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        if key.is_empty() {
            return Err(DBError::EmptyKey.into());
        }
        if value.is_empty() {
            return Err(DBError::EmptyValue.into());
        }
        self.inner
            .db
            .put_opt(key, value, &self.inner.wo_sync)
            .map_err(|e| DBError::WrapError(e.to_string()).into())
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        if key.is_empty() {
            return Err(DBError::EmptyKey.into());
        }
        self.inner
            .db
            .delete_opt(key, &self.inner.wo)
            .map_err(|e| DBError::WrapError(e.to_string()).into())
    }

    fn delete_sync(&mut self, key: &[u8]) -> Result<()> {
        if key.is_empty() {
            return Err(DBError::EmptyKey.into());
        }
        self.inner
            .db
            .delete_opt(key, &self.inner.wo_sync)
            .map_err(|e| DBError::WrapError(e.to_string()).into())
    }

    fn new_batch(&mut self) -> Box<dyn Batch> {
        Box::new(RocksDBBatch {
            inner: Rc::new(RefCell::new(rocksdb::WriteBatch::default())),
        })
    }

    fn write_batch(&mut self, batch: Box<dyn Batch>) -> Result<()> {
        let b = batch
            .as_any()
            .downcast_ref::<RocksDBBatch>()
            .ok_or(DBError::DownCast)?
            .to_owned();
        self.inner
            .db
            .write(b.inner.take())
            .map_err(|e| DBError::WrapError(e.to_string()).into())
    }

    fn write_batch_sync(&mut self, batch: Box<dyn Batch>) -> Result<()> {
        let b = batch
            .as_any()
            .downcast_ref::<RocksDBBatch>()
            .ok_or(DBError::DownCast)?
            .to_owned();
        self.inner
            .db
            .write_opt(b.inner.take(), &self.inner.wo_sync)
            .map_err(|e| DBError::WrapError(e.to_string()).into())
    }
}

impl Drop for RocksDB {
    fn drop(&mut self) {
        self.inner.db.flush().unwrap();
    }
}

#[derive(Clone)]
pub struct RocksDBBatch {
    inner: Rc<RefCell<rocksdb::WriteBatch>>,
}

impl Batch for RocksDBBatch {
    fn set(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        if key.is_empty() {
            return Err(DBError::EmptyKey.into());
        }
        if value.is_empty() {
            return Err(DBError::EmptyValue.into());
        }
        self.inner.as_ref().borrow_mut().put(key, value);
        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        if key.is_empty() {
            return Err(DBError::EmptyKey.into());
        }
        self.inner.as_ref().borrow_mut().delete(key);
        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    pub fn test_crud() {
        let mut db = new_rocks_db("test_crud", &std::env::temp_dir()).unwrap();
        db.set(b"key", b"value").unwrap();
        assert_eq!(true, db.has(b"key").unwrap());
        assert_eq!(Some(b"value".to_vec()), db.get(b"key").unwrap());
        db.delete(b"key").unwrap();
        assert_eq!(false, db.has(b"key").unwrap());
        assert_eq!(None, db.get(b"key").unwrap());
        drop(db);
        std::fs::remove_dir_all(std::env::temp_dir().join("test_crud.db")).unwrap();
    }

    #[test]
    pub fn test_batch() {
        let mut db = new_rocks_db("test_batch", &std::env::temp_dir()).unwrap();
        let mut batch = db.new_batch();

        for i in 0u32..100u32 {
            batch.set(&i.to_le_bytes(), &i.to_le_bytes()).unwrap();
        }
        db.write_batch_sync(batch).unwrap();
        for i in 0u32..100u32 {
            assert_eq!(true, db.has(&i.to_le_bytes()).unwrap());
        }
        drop(db);
        std::fs::remove_dir_all(std::env::temp_dir().join("test_batch.db")).unwrap();
    }
}
