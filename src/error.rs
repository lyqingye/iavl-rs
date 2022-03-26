use thiserror::Error;

#[derive(Error, Debug)]
pub enum AvlTreeError {
    #[error("root hash not found")]
    RootHashNotFound,

    #[error("key and value non existence in tree")]
    ValueNonExistence,
}

#[derive(Error, Debug)]
pub enum DBError {
    #[error("DownCast Type Fail!")]
    DownCast,

    #[error("{0}")]
    WrapError(String),

    #[error("Empty key")]
    EmptyKey,

    #[error("Empty value")]
    EmptyValue,
}
