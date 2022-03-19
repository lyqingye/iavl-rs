use crate::hash::{hash_array, Hash};
pub struct ProofPathNode {
    pub prefix: Vec<u8>,
    pub suffix: Vec<u8>,
}

pub struct Proof {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub path: Vec<ProofPathNode>,
}

impl Proof {
    pub fn calc_exsistence_root(&self) -> Hash {
        let mut hash = hash_array(&[self.key.as_ref(), self.value.as_ref()]);
        for node in &self.path {
            hash = hash_array(&[node.prefix.as_ref(), hash.as_ref(), node.suffix.as_ref()])
        }
        hash
    }
}
