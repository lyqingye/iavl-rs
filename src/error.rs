use thiserror::Error;

#[derive(Error, Debug)]
pub enum AvlTreeError {
    #[error("root hash not found")]
    RootHashNotFound,

    #[error("key and value non exsistence in tree")]
    ValueNonExsistence,
}
