use std::convert::TryInto;
use std::io::{Read, Write};

use anyhow::{Context, Result};
use crc::{crc32, Hasher32};
use ton_types::{ByteOrderRead, UInt256};

#[derive(Debug)]
pub struct BocHeader {
    pub root_index: Option<usize>,
    pub index_included: bool,
    pub has_crc: bool,
    pub ref_size: usize,
    pub offset_size: usize,
    pub cell_count: usize,
}

impl BocHeader {
    pub fn from_reader<R>(src: &mut R) -> Result<Self>
    where
        R: Read,
    {
        const BOC_INDEXED_TAG: u32 = 0x68ff65f3;
        const BOC_INDEXED_CRC32_TAG: u32 = 0xacc3a728;
        const BOC_GENERIC_TAG: u32 = 0xb5ee9c72;

        let magic = src.read_be_u32()?;
        let first_byte = src.read_byte()?;

        let index_included;
        let mut has_crc = false;
        let ref_size;

        match magic {
            BOC_INDEXED_TAG => {
                ref_size = first_byte as usize;
                index_included = true;
            }
            BOC_INDEXED_CRC32_TAG => {
                ref_size = first_byte as usize;
                index_included = true;
                has_crc = true;
            }
            BOC_GENERIC_TAG => {
                index_included = first_byte & 0b1000_0000 != 0;
                has_crc = first_byte & 0b0100_0000 != 0;
                ref_size = (first_byte & 0b0000_0111) as usize;
            }
            _ => {
                return Err(ShardStateParserError::InvalidShardStateHeader).context("Invalid flags")
            }
        }

        if ref_size == 0 || ref_size > 4 {
            return Err(ShardStateParserError::InvalidShardStateHeader)
                .context("Ref size must be in range [1;4]");
        }

        let offset_size = src.read_byte()? as usize;
        if offset_size == 0 || offset_size > 8 {
            return Err(ShardStateParserError::InvalidShardStateHeader)
                .context("Offset size must be in range [1;8]");
        }

        let cell_count = src.read_be_uint(ref_size)?;
        let root_count = src.read_be_uint(ref_size)?;
        src.read_be_uint(ref_size)?; // skip absent

        if root_count != 1 {
            return Err(ShardStateParserError::InvalidShardStateHeader)
                .context("Expected one root cell");
        }
        if root_count > cell_count {
            return Err(ShardStateParserError::InvalidShardStateHeader)
                .context("Root count is greater then cell count");
        }

        src.read_be_uint(offset_size)?; // skip total cells size

        let root_index = if magic == BOC_GENERIC_TAG {
            Some(src.read_be_uint(ref_size)?)
        } else {
            None
        };

        Ok(Self {
            root_index,
            index_included,
            has_crc,
            ref_size,
            offset_size,
            cell_count,
        })
    }
}

macro_rules! try_read {
    ($expr:expr) => {
        match $expr {
            Ok(data) => data,
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e.into()),
        }
    };
}

pub struct PacketCrc {
    pub value: u32,
}

impl PacketCrc {
    pub fn from_reader<R>(src: &mut R) -> Result<Option<Self>>
    where
        R: Read,
    {
        Ok(Some(Self {
            value: try_read!(src.read_le_u32()),
        }))
    }
}

pub struct RawCell {
    pub cell_type: ton_types::CellType,
    pub level: u8,
    pub data: Vec<u8>,
    pub reference_indices: Vec<u32>,
    pub reference_hashes: Vec<UInt256>,
    pub hashes: Option<[[u8; 32]; 4]>,
    pub depths: Option<[u16; 4]>,
}

impl RawCell {
    pub fn from_reader<R>(
        src: &mut R,
        ref_size: usize,
        cell_count: usize,
        cell_index: usize,
    ) -> Result<Option<Self>>
    where
        R: Read,
    {
        let d1 = try_read!(src.read_byte());
        let l = d1 >> 5;
        let h = (d1 & 0b0001_0000) != 0;
        let s = (d1 & 0b0000_1000) != 0;
        let r = (d1 & 0b0000_0111) as usize;
        let absent = r == 0b111 && h;

        if absent {
            log::info!("ABSENT: {}, {}, {}, {}", l, h, s, r);

            let data_size = 32 * ((ton_types::LevelMask::with_level(l).level() + 1) as usize);
            let mut cell_data = vec![0; data_size + 1];
            try_read!(src.read_exact(&mut cell_data[..data_size]));
            cell_data[data_size] = 0x80;

            return Ok(Some(RawCell {
                cell_type: ton_types::CellType::Ordinary,
                level: l,
                data: cell_data,
                reference_indices: Vec::new(),
                reference_hashes: Vec::new(),
                hashes: None,
                depths: None,
            }));
        }

        if r > 4 {
            log::info!("cell_index={},r={}", cell_index, r);
            return Err(ShardStateParserError::InvalidShardStateCell)
                .context("Cell must contain at most 4 references");
        }

        let d2 = try_read!(src.read_byte());
        let data_size = ((d2 >> 1) + if d2 & 1 != 0 { 1 } else { 0 }) as usize;
        let no_completion_tag = d2 & 1 == 0;

        let (hashes, depths) = if h {
            let mut hashes = [[0u8; 32]; 4];
            let mut depths = [0; 4];

            let level = ton_types::LevelMask::with_mask(l).level() as usize;
            for hash in hashes.iter_mut().take(level + 1) {
                try_read!(src.read_exact(hash));
            }
            for depth in depths.iter_mut().take(level + 1) {
                *depth = try_read!(src.read_be_uint(2)) as u16;
            }
            (Some(hashes), Some(depths))
        } else {
            (None, None)
        };

        let mut cell_data = vec![0; data_size + if no_completion_tag { 1 } else { 0 }];
        try_read!(src.read_exact(&mut cell_data[..data_size]));

        if no_completion_tag {
            cell_data[data_size] = 0x80;
        }

        let cell_type = if !s {
            ton_types::CellType::Ordinary
        } else {
            ton_types::CellType::from(cell_data[0])
        };

        let mut reference_indices = Vec::with_capacity(r);
        if r > 0 {
            for _ in 0..r {
                let index = try_read!(src.read_be_uint(ref_size));
                if index > cell_count || index <= cell_index {
                    return Err(ShardStateParserError::InvalidShardStateCell)
                        .context("Reference index out of range");
                } else {
                    reference_indices.push(index as u32);
                }
            }
        }

        Ok(Some(RawCell {
            cell_type,
            level: l,
            data: cell_data,
            reference_indices,
            reference_hashes: Vec::with_capacity(r),
            hashes,
            depths,
        }))
    }
}

#[derive(Copy, Clone, Eq, PartialEq)]
pub enum ReaderAction {
    Incomplete,
    Complete,
}

pub struct ShardStatePacketReader {
    hasher: crc32::Digest,
    offset: usize,
    current_packet: Vec<u8>,
    next_packet: Vec<u8>,
    bytes_to_skip: usize,
}

impl ShardStatePacketReader {
    pub fn new() -> Self {
        Self {
            hasher: crc32::Digest::new(crc32::CASTAGNOLI),
            offset: 0,
            current_packet: Default::default(),
            next_packet: Default::default(),
            bytes_to_skip: 0,
        }
    }

    pub fn crc32(&self) -> u32 {
        self.hasher.sum32()
    }

    pub fn begin(&'_ mut self) -> ShardStatePacketReaderTransaction<'_> {
        let offset = self.offset;
        ShardStatePacketReaderTransaction {
            reader: self,
            reading_next_packet: false,
            offset,
        }
    }

    pub fn set_skip(&mut self, n: usize) {
        self.bytes_to_skip = n;
    }

    pub fn set_next_packet(&mut self, packet: Vec<u8>) {
        self.next_packet = packet;
    }

    pub fn process_skip(&mut self) -> ReaderAction {
        let mut n = std::mem::take(&mut self.bytes_to_skip);

        let remaining = self.current_packet.len() - self.offset;
        log::info!("TO SKIP: {}, REMAINING: {}", n, remaining);
        if n > remaining {
            log::info!("SWITCHING TO NEXT PACKET");
            n -= remaining;
            self.offset = 0;
            self.current_packet = std::mem::take(&mut self.next_packet);
        } else {
            log::info!("COMPLETE 1");
            self.offset += n;
            return ReaderAction::Complete;
        }

        if n > self.current_packet.len() {
            log::info!("INCOMPLETE");
            n -= self.current_packet.len();
            self.current_packet = Vec::new();
            self.bytes_to_skip = n;
            ReaderAction::Incomplete
        } else {
            log::info!("COMPLETE 2");
            self.offset += n;
            ReaderAction::Complete
        }
    }
}

pub struct ShardStatePacketReaderTransaction<'a> {
    reader: &'a mut ShardStatePacketReader,
    reading_next_packet: bool,
    offset: usize,
}

impl<'a> ShardStatePacketReaderTransaction<'a> {
    pub fn end(self) {
        if self.reading_next_packet {
            // Write to the hasher until the end of current packet
            self.reader
                .hasher
                .write(&self.reader.current_packet[self.reader.offset..]);

            // Write to the hasher current bytes
            self.reader
                .hasher
                .write(&self.reader.next_packet[..self.offset]);

            // Replace current packet
            self.reader.current_packet = std::mem::take(&mut self.reader.next_packet);
        } else {
            // Write to the hasher current bytes
            self.reader
                .hasher
                .write(&self.reader.current_packet[..self.offset]);
        }

        // Bump offset
        self.reader.offset = self.offset;
    }
}

impl<'a> Read for ShardStatePacketReaderTransaction<'a> {
    fn read(&mut self, mut buf: &mut [u8]) -> std::io::Result<usize> {
        let mut result = 0;

        loop {
            let current_packet = match self.reading_next_packet {
                // Reading non-empty current packet
                false if self.offset < self.reader.current_packet.len() => {
                    &self.reader.current_packet
                }

                // Current packet is empty - retry and switch to next
                false => {
                    self.reading_next_packet = true;
                    self.offset = 0;
                    continue;
                }

                // Reading non-empty next packet
                true if self.offset < self.reader.next_packet.len() => &self.reader.next_packet,

                // Reading next packet which is empty
                true => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "packet buffer underflow",
                    ))
                }
            };

            let n = std::cmp::min(current_packet.len() - self.offset, buf.len());
            unsafe {
                std::ptr::copy_nonoverlapping(
                    current_packet.as_ptr().offset(self.offset as isize),
                    buf.as_mut_ptr(),
                    n,
                )
            };

            result += n;
            self.offset += n;

            let tmp = buf;
            buf = &mut tmp[n..];

            if buf.is_empty() {
                return Ok(result);
            }
        }
    }
}

#[derive(thiserror::Error, Debug)]
enum ShardStateParserError {
    #[error("Invalid shard state header")]
    InvalidShardStateHeader,
    #[error("Invalid shard state cell")]
    InvalidShardStateCell,
}
