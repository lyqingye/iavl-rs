use sha2::{Digest, Sha256};

pub type Hash = Vec<u8>;

pub fn hash_value(bytes: &[u8]) -> Hash {
    let mut sha = Sha256::new();
    sha.update(bytes);
    let hash = sha.finalize();
    hash.to_vec()
}

pub fn hash_array(bytes_array: &[&[u8]]) -> Hash {
    let mut sha = Sha256::new();
    for bytes in bytes_array {
        sha.update(*bytes);
    }
    let hash = sha.finalize();
    hash.to_vec()
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_hash() {
        let result = hash_value(b"hello");
        assert_eq!(
            result,
            hex::decode("2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824")
                .unwrap()
        );
        assert_eq!(Sha256::digest(b"hello").to_vec(), result);
    }

    #[test]
    fn test_hash_array() {
        let result = hash_array(&[b"h", b"e", b"l", b"l", b"o"]);
        assert_eq!(Sha256::digest(b"hello").to_vec(), result);
    }
}
