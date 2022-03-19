use crate::hash::{hash_array, hash_value, Hash};

pub type NodeRef = Option<Box<AvlNode>>;

#[derive(Eq, PartialEq, Debug, Clone)]
pub struct AvlNode {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub hash: Hash,
    pub merkle_hash: Hash,
    pub height: u32,
    pub left: NodeRef,
    pub right: NodeRef,
}

#[allow(clippy::unnecessary_wraps)]
pub fn as_node_ref(key: Vec<u8>, value: Vec<u8>) -> NodeRef {
    Some(Box::new(AvlNode::new(key, value)))
}

impl AvlNode {
    fn new(key: Vec<u8>, value: Vec<u8>) -> Self {
        let hash = hash_array(&[key.as_ref(), value.as_ref()]);
        let merkle_hash = hash_value(hash.as_ref());
        AvlNode {
            key,
            value,
            hash,
            merkle_hash,
            height: 0,
            left: None,
            right: None,
        }
    }

    fn left_height(&self) -> Option<u32> {
        self.left.as_ref().map(|left| left.height)
    }

    fn right_height(&self) -> Option<u32> {
        self.right.as_ref().map(|right| right.height)
    }

    pub fn left_hash(&self) -> Option<&[u8]> {
        Some(self.left.as_ref()?.merkle_hash.as_ref())
    }

    pub fn right_hash(&self) -> Option<&[u8]> {
        Some(self.right.as_ref()?.merkle_hash.as_ref())
    }

    fn update_height(&mut self) {
        match &self.right {
            None => match &self.left {
                None => self.height = 0,
                Some(left) => self.height = left.height + 1,
            },
            Some(right) => match &self.left {
                None => self.height = right.height + 1,
                Some(left) => self.height = std::cmp::max(left.height, right.height) + 1,
            },
        }
    }

    fn update_hashes(&mut self) {
        let mut array: Vec<&[u8]> = Vec::new();
        if let Some(left) = &self.left {
            array.push(left.merkle_hash.as_ref());
        }
        array.push(self.hash.as_ref());
        if let Some(right) = &self.right {
            array.push(right.merkle_hash.as_ref());
        }
        self.merkle_hash = hash_array(array.as_ref());
    }

    pub fn update_value(&mut self, value: &[u8]) -> Vec<u8> {
        let hash = hash_array(&[self.key.as_ref(), value]);
        self.hash = hash;
        std::mem::replace(&mut self.value, value.to_vec())
    }

    pub fn update(&mut self) {
        self.update_hashes();
        self.update_height();
    }

    pub fn balance_factor(&self) -> i32 {
        match (self.left_height(), self.right_height()) {
            (None, None) => 0,
            (None, Some(h)) => -(h as i32),
            (Some(h), None) => h as i32,
            (Some(h_l), Some(h_r)) => (h_l as i32) - (h_r as i32),
        }
    }

    pub fn is_leaf(&self) -> bool {
        self.right.is_none() && self.left.is_none()
    }
}
