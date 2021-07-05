use std::borrow::Borrow;
use std::hash::Hash;
use std::str::FromStr;

use anyhow::Result;
use ton_types::UInt256;

use crate::utils::*;

#[derive(Debug, Hash, Eq, PartialEq)]
pub enum PackageEntryId<I> {
    Block(I),
    Proof(I),
    ProofLink(I),
}

impl PackageEntryId<ton_block::BlockIdExt> {
    pub fn from_filename(filename: &str) -> Result<Self> {
        let block_id_pos = match filename.find('(') {
            Some(pos) => pos,
            None => return Err(PackageEntryIdError::InvalidFileName.into()),
        };

        let (prefix, block_id) = filename.split_at(block_id_pos);

        Ok(match prefix {
            PACKAGE_ENTRY_BLOCK => Self::Block(parse_block_id(block_id)?),
            PACKAGE_ENTRY_PROOF => Self::Proof(parse_block_id(block_id)?),
            PACKAGE_ENTRY_PROOF_LINK => Self::ProofLink(parse_block_id(block_id)?),
            _ => return Err(PackageEntryIdError::InvalidFileName.into()),
        })
    }
}

impl<I> PackageEntryId<I>
where
    I: Borrow<ton_block::BlockIdExt> + Hash,
{
    fn filename_prefix(&self) -> &'static str {
        match self {
            PackageEntryId::Block(_) => PACKAGE_ENTRY_BLOCK,
            PackageEntryId::Proof(_) => PACKAGE_ENTRY_PROOF,
            PackageEntryId::ProofLink(_) => PACKAGE_ENTRY_PROOF_LINK,
        }
    }
}

pub trait GetFileName {
    fn filename(&self) -> String;
}

impl GetFileName for ton_block::BlockIdExt {
    fn filename(&self) -> String {
        format!(
            "({},{:016x},{}):{}:{}",
            self.shard_id.workchain_id(),
            self.shard_id.shard_prefix_with_tag(),
            self.seq_no,
            hex::encode_upper(self.root_hash.as_slice()),
            hex::encode_upper(self.file_hash.as_slice())
        )
    }
}

impl<I> GetFileName for PackageEntryId<I>
where
    I: Borrow<ton_block::BlockIdExt> + Hash,
{
    fn filename(&self) -> String {
        match self {
            Self::Block(block_id) | Self::Proof(block_id) | Self::ProofLink(block_id) => {
                format!("{}{}", self.filename_prefix(), block_id.borrow().filename())
            }
        }
    }
}

impl<I> std::fmt::Display for PackageEntryId<I>
where
    I: Borrow<ton_block::BlockIdExt> + Hash,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.filename().as_str())
    }
}

fn parse_block_id(filename: &str) -> Result<ton_block::BlockIdExt> {
    let mut parts = filename.split(':');

    let shard_id = match parts.next() {
        Some(part) => part,
        None => return Err(PackageEntryIdError::ShardIdNotFound.into()),
    };

    let mut shard_id_parts = shard_id.split(',');
    let workchain_id = match shard_id_parts
        .next()
        .and_then(|part| part.strip_prefix("("))
    {
        Some(part) => i32::from_str(part)?,
        None => return Err(PackageEntryIdError::WorkchainIdNotFound.into()),
    };

    let shard_prefix_tagged = match shard_id_parts.next() {
        Some(part) => u64::from_str_radix(part, 16)?,
        None => return Err(PackageEntryIdError::ShardPrefixNotFound.into()),
    };

    let seq_no = match shard_id_parts
        .next()
        .and_then(|part| part.strip_suffix(')'))
    {
        Some(part) => u32::from_str(part)?,
        None => return Err(PackageEntryIdError::SeqnoNotFound.into()),
    };

    let shard_id =
        ton_block::ShardIdent::with_tagged_prefix(workchain_id, shard_prefix_tagged).convert()?;

    let root_hash = match parts.next() {
        Some(part) => UInt256::from_str(part).convert()?,
        None => return Err(PackageEntryIdError::RootHashNotFound.into()),
    };

    let file_hash = match parts.next() {
        Some(part) => UInt256::from_str(part).convert()?,
        None => return Err(PackageEntryIdError::FileHashNotFound.into()),
    };

    Ok(ton_block::BlockIdExt {
        shard_id,
        seq_no,
        root_hash,
        file_hash,
    })
}

const PACKAGE_ENTRY_BLOCK: &str = "block";
const PACKAGE_ENTRY_PROOF: &str = "proof";
const PACKAGE_ENTRY_PROOF_LINK: &str = "prooflink";

#[derive(thiserror::Error, Debug)]
enum PackageEntryIdError {
    #[error("Invalid filename")]
    InvalidFileName,
    #[error("Shard id not found")]
    ShardIdNotFound,
    #[error("Workchain id not found")]
    WorkchainIdNotFound,
    #[error("Shard prefix not found")]
    ShardPrefixNotFound,
    #[error("Seqno not found")]
    SeqnoNotFound,
    #[error("Root hash not found")]
    RootHashNotFound,
    #[error("File hash not found")]
    FileHashNotFound,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_store_load() {
        fn check_package_id(package_id: PackageEntryId<ton_block::BlockIdExt>) {
            assert_eq!(
                PackageEntryId::from_filename(&package_id.filename()).unwrap(),
                package_id
            );
        }

        let block_id = ton_block::BlockIdExt {
            shard_id: ton_block::ShardIdent::with_tagged_prefix(-1, ton_block::SHARD_FULL).unwrap(),
            seq_no: rand::random(),
            root_hash: UInt256::rand(),
            file_hash: UInt256::rand(),
        };

        check_package_id(PackageEntryId::Block(block_id.clone()));
        check_package_id(PackageEntryId::Proof(block_id.clone()));
        check_package_id(PackageEntryId::ProofLink(block_id.clone()));
    }
}