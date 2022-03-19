use crate::error::AvlTreeError;
use crate::hash::*;
use crate::node::*;
use crate::proof::*;
use anyhow::*;
use std::cmp::Ordering;

#[derive(PartialEq, Eq, Debug, Clone)]
pub struct Tree {
    pub root: NodeRef,
}

impl Tree {
    pub fn new() -> Self {
        Tree { root: None }
    }

    pub fn root_hash(&self) -> Option<&Hash> {
        Some(&self.root.as_ref()?.merkle_hash)
    }

    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        let mut node_ref = &self.root;
        while let Some(ref node) = node_ref {
            let node_key: &[u8] = node.key.as_ref();
            match node_key.cmp(key) {
                Ordering::Greater => node_ref = &node.left,
                Ordering::Less => node_ref = &node.right,
                Ordering::Equal => return Some(node.value.as_ref()),
            }
        }
        None
    }

    #[cfg(test)]
    pub fn get_node_ref(&self, key: &[u8]) -> Option<&Box<Node>> {
        let mut node_ref = &self.root;
        while let Some(ref node) = node_ref {
            let node_key: &[u8] = node.key.as_ref();
            match node_key.cmp(key) {
                Ordering::Greater => node_ref = &node.left,
                Ordering::Less => node_ref = &node.right,
                Ordering::Equal => return Some(node),
            }
        }
        None
    }

    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Option<Vec<u8>> {
        let node_ref = &mut self.root;
        let mut old_value = None;
        Self::insert_recursive(node_ref, key, value, &mut old_value);
        old_value
    }

    fn insert_recursive(
        node_ref: &mut NodeRef,
        key: &[u8],
        value: &[u8],
        old_value: &mut Option<Vec<u8>>,
    ) {
        if let Some(node) = node_ref {
            let node_key: &[u8] = node.key.as_ref();
            match node_key.cmp(key) {
                Ordering::Greater => Self::insert_recursive(&mut node.left, key, value, old_value),
                Ordering::Less => Self::insert_recursive(&mut node.right, key, value, old_value),
                Ordering::Equal => return *old_value = Some(node.update_value(value)),
            }
            node.update();
            Self::balance_node(node_ref);
        } else {
            *node_ref = as_node_ref(key.to_vec(), value.to_vec());
        }
    }

    /// Rebalance the AVL tree by performing rotations, if needed.
    fn balance_node(node_ref: &mut NodeRef) {
        let node = node_ref
            .as_mut()
            .expect("[AVL]: Empty node in node balance");
        let balance_factor = node.balance_factor();
        if balance_factor >= 2 {
            let left = node
                .left
                .as_mut()
                .expect("[AVL]: Unexpected empty left node");
            if left.balance_factor() < 1 {
                Tree::rotate_left(&mut node.left);
            }
            Tree::rotate_right(node_ref);
        } else if balance_factor <= -2 {
            let right = node
                .right
                .as_mut()
                .expect("[AVL]: Unexpected empty right node");
            if right.balance_factor() > -1 {
                Tree::rotate_right(&mut node.right);
            }
            Tree::rotate_left(node_ref);
        }
    }

    pub fn rotate_right(root: &mut NodeRef) {
        let mut node = root.take().expect("[AVL]: Empty root in right rotation");
        let mut left = node.left.take().expect("[AVL]: Unexpected right rotation");
        let mut left_right = left.right.take();
        std::mem::swap(&mut node.left, &mut left_right);
        node.update();
        std::mem::swap(&mut left.right, &mut Some(node));
        left.update();
        std::mem::swap(root, &mut Some(left));
    }

    pub fn rotate_left(root: &mut NodeRef) {
        let mut node = root.take().expect("[AVL]: Empty root in left rotation");
        let mut right = node.right.take().expect("[AVL]: Unexpected left rotation");
        let mut right_left = right.left.take();
        std::mem::swap(&mut node.right, &mut right_left);
        node.update();
        std::mem::swap(&mut right.left, &mut Some(node));
        right.update();
        std::mem::swap(root, &mut Some(right))
    }

    #[cfg(test)]
    pub fn validate(&self) -> bool {
        Self::validate_recursive(self.root.as_ref().unwrap())
    }

    #[cfg(test)]
    pub fn validate_recursive(node: &Node) -> bool {
        if node.is_leaf() {
            assert_eq!(0, node.height, "Leaf node height must be 0");
            return true;
        }

        let mut chidren_height = 0;
        if let Some(left) = &node.left {
            chidren_height = left.height;
        }

        if let Some(right) = &node.right {
            chidren_height = std::cmp::max(chidren_height, right.height);
        }

        if node.height != chidren_height + 1 {
            return false;
        }

        if node.balance_factor() >= 2 {
            return false;
        }
        if let Some(left) = &node.left {
            if !Self::validate_recursive(left) {
                return false;
            }
        }
        if let Some(right) = &node.right {
            if !Self::validate_recursive(right) {
                return false;
            }
        }
        true
    }

    pub fn get_proof(&self, key: &[u8]) -> Option<Proof> {
        self.get_proof_recursive(key, &self.root)
    }

    fn get_proof_recursive(&self, key: &[u8], node: &NodeRef) -> Option<Proof> {
        if let Some(node) = node {
            let empty_hash = [];
            let node_key: &[u8] = node.key.as_ref();
            let (mut proof, prefix, suffix) = match node_key.cmp(key) {
                Ordering::Greater => {
                    let proof = self.get_proof_recursive(key, &node.left)?;
                    let prefix = vec![];
                    let mut suffix: Vec<u8> = Vec::with_capacity(64);
                    suffix.extend(node.hash.iter());
                    suffix.extend(node.right_hash().unwrap_or(&empty_hash));
                    (proof, prefix, suffix)
                }
                Ordering::Less => {
                    let proof = self.get_proof_recursive(key, &node.right)?;
                    let suffix = vec![];
                    let mut prefix: Vec<u8> = Vec::with_capacity(64);
                    prefix.extend(node.left_hash().unwrap_or(&empty_hash));
                    prefix.extend(node.hash.iter());
                    (proof, prefix, suffix)
                }
                Ordering::Equal => {
                    let proof = Proof {
                        key: node.key.clone(),
                        value: node.value.clone(),
                        path: vec![],
                    };
                    let prefix = node.left_hash().unwrap_or(&empty_hash).to_vec();
                    let suffix = node.right_hash().unwrap_or(&empty_hash).to_vec();
                    (proof, prefix, suffix)
                }
            };

            let path_node = ProofPathNode { prefix, suffix };
            proof.path.push(path_node);
            Some(proof)
        } else {
            None
        }
    }

    pub fn verify_existence(&self, key: &[u8], value: &[u8], proof: &Proof) -> Result<()> {
        assert!(proof.key.eq(key));
        assert!(proof.value.eq(value));
        let root = self.root_hash().ok_or(AvlTreeError::RootHashNotFound)?;
        if proof.calc_root_hash().eq(root) {
            Ok(())
        } else {
            Err(AvlTreeError::ValueNonExistence.into())
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_simple_tree() {
        let mut tree = Tree::new();
        let now = std::time::Instant::now();
        for i in 0u32..10000u32 {
            let bytes = i.to_le_bytes();
            tree.insert(&bytes, &bytes);
            assert!(tree.validate());
        }
        for i in 0u32..10000u32 {
            let bytes = i.to_le_bytes();
            tree.get(&bytes).unwrap();
        }
        println!("{}", now.elapsed().as_secs());
    }

    #[test]
    fn test_root_hash() {
        let mut tree = Tree::new();
        let nodes: [u32; 9] = [100, 50, 150, 25, 75, 125, 175, 65, 85];
        let mut hashs = vec![];
        for node in nodes {
            tree.insert(&node.to_le_bytes(), &node.to_le_bytes());
            hashs.push(hash_array(&[&node.to_le_bytes(), &node.to_le_bytes()]));
        }
        assert_eq!(3, tree.root.as_ref().unwrap().height);
        assert_eq!(
            100u32.to_le_bytes().to_vec(),
            tree.root.as_ref().unwrap().value
        );
        let hash_75 = hash_array(&[
            hash_value(hashs[7].as_ref()).as_ref(),
            hashs[4].as_ref(),
            hash_value(hashs[8].as_ref()).as_ref(),
        ]);
        let hash_150 = hash_array(&[
            hash_value(hashs[5].as_ref()).as_ref(),
            hashs[2].as_ref(),
            hash_value(hashs[6].as_ref()).as_ref(),
        ]);
        let hash_50 = hash_array(&[
            hash_value(hashs[3].as_ref()).as_ref(),
            hashs[1].as_ref(),
            hash_75.as_ref(),
        ]);
        let root = hash_array(&[hash_50.as_ref(), hashs[0].as_ref(), hash_150.as_ref()]);
        assert!(root.eq(tree.root_hash().unwrap()))
    }

    #[test]
    fn test_proof() {
        let mut tree = Tree::new();
        for i in 0u32..10000u32 {
            let bytes = i.to_le_bytes();
            tree.insert(&bytes, &bytes);
        }

        for i in 0u32..10000u32 {
            let bytes = i.to_le_bytes();
            let proof = tree.get_proof(&bytes).unwrap();
            assert!(tree.verify_existence(&bytes, &bytes, &proof).is_ok());
        }
    }
}
