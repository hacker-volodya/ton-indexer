use std::io::Read;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};

use anyhow::Result;
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use parking_lot::RwLock;
use rustc_hash::FxHashSet;
use smallvec::SmallVec;
use ton_types::{ByteOrderRead, CellImpl, FxDashMap, UInt256};

use super::marker::Marker;
use crate::db::*;

pub struct CellStorage {
    db: Arc<Db>,
    cells_cache: Arc<FxDashMap<UInt256, Weak<StorageCell>>>,
}

impl CellStorage {
    pub fn new(db: Arc<Db>) -> Result<Arc<Self>> {
        let cache = Arc::new(FxDashMap::default());
        Ok(Arc::new(Self {
            db,
            cells_cache: cache,
        }))
    }

    pub fn store_cell(
        &self,
        batch: &mut rocksdb::WriteBatch,
        marker: Marker,
        root: ton_types::Cell,
    ) -> Result<usize, CellStorageError> {
        // Prepare handles
        let cells = &self.db.cells;
        let raw = cells.db();
        let cf = cells.cf();
        let read_options = cells.read_config();

        let mut transaction = FxHashSet::default();

        let mut buffer = Vec::with_capacity(512);

        // Check root cell
        let cell_id = root.repr_hash();
        match raw.get_pinned_cf_opt(&cf, cell_id.as_array(), read_options) {
            Ok(Some(value)) => {
                // NOTE: dereference value only once to prevent multiple ffi calls
                let value = value.as_ref();
                // Overwrite value if it has different marker
                if value.is_empty() {
                    // Empty cell is invalid
                    return Err(CellStorageError::InvalidCell);
                } else if value[0] > 0 && value[0] != marker {
                    // Proceed if cell was updated
                    buffer.clear();
                    buffer.extend_from_slice(value);
                    buffer[0] = marker.0;
                    batch.put_cf(&cf, cell_id.as_slice(), &buffer);
                    transaction.insert(cell_id);
                } else {
                    // Cell already exists. Do nothing
                    return Ok(0);
                }
            }
            // Insert cell value if it doesn't exist
            Ok(None) => {
                if StorageCell::serialize_to(marker, &*root, &mut buffer).is_err() {
                    return Err(CellStorageError::InvalidCell);
                }
                batch.put_cf(&cf, cell_id.as_slice(), &buffer);
                transaction.insert(cell_id);
            }
            Err(e) => return Err(CellStorageError::Internal(e)),
        }

        let mut stack = Vec::with_capacity(16);
        stack.push(root);

        while let Some(current) = stack.pop() {
            for i in 0..current.references_count() {
                let cell = match current.reference(i) {
                    Ok(cell) => cell,
                    Err(_) => return Err(CellStorageError::InvalidCell),
                };
                let cell_id = cell.repr_hash();

                match raw.get_pinned_cf_opt(&cf, cell_id.as_array(), read_options) {
                    Ok(Some(value)) => {
                        // NOTE: dereference value only once to prevent multiple ffi calls
                        let value = value.as_ref();
                        if value.is_empty() {
                            // Empty cell is invalid
                            return Err(CellStorageError::InvalidCell);
                        } else if value[0] > 0 && value[0] != marker {
                            buffer.clear();
                            buffer.extend_from_slice(value);
                            // SAFETY: bounds are checked above. Definitely removes bounds check
                            unsafe { *buffer.get_unchecked_mut(0) = marker.0 };
                            batch.put_cf(&cf, cell_id.as_slice(), &buffer);
                            transaction.insert(cell_id);
                        } else {
                            // Cell already exists
                            continue;
                        }
                    }
                    // Already inserting this cell
                    Ok(None) if transaction.contains(&cell_id) => continue,
                    Ok(None) => {
                        if StorageCell::serialize_to(marker, &*cell, &mut buffer).is_err() {
                            return Err(CellStorageError::InvalidCell);
                        }
                        batch.put_cf(&cf, cell_id.as_slice(), &buffer);
                        transaction.insert(cell_id);
                    }
                    Err(e) => return Err(CellStorageError::Internal(e)),
                }

                stack.push(cell);
            }
        }

        Ok(transaction.len())
    }

    pub fn load_cell(
        self: &Arc<Self>,
        hash: UInt256,
    ) -> Result<Arc<StorageCell>, CellStorageError> {
        if let Some(cell) = self.cells_cache.get(&hash) {
            if let Some(cell) = cell.upgrade() {
                return Ok(cell);
            }
        }

        let cell = match self.db.cells.get(hash.as_slice()) {
            Ok(Some(value)) => match StorageCell::deserialize(self.clone(), &value) {
                Ok(cell) => Arc::new(cell),
                Err(_) => return Err(CellStorageError::InvalidCell),
            },
            Ok(None) => return Err(CellStorageError::CellNotFound),
            Err(e) => return Err(CellStorageError::Internal(e)),
        };
        self.cells_cache.insert(hash, Arc::downgrade(&cell));

        Ok(cell)
    }

    pub async fn sweep_cells(&self, target_marker: Marker) -> Result<usize> {
        let total = Arc::new(AtomicUsize::new(0));
        let mut tasks = FuturesUnordered::new();

        for i in 0..8 {
            // iii00000, 00000000, ...
            let lower_bound = if i > 0 {
                let mut lower_bound = [0; 32];
                lower_bound[0] = i << 5;
                Some(lower_bound)
            } else {
                None
            };

            // jjj00000, 00000000, ... (where jjj = i + 1)
            let upper_bound = if i < 7 {
                let mut upper_bound = [0; 32];
                upper_bound[0] = (i + 1) << 5;
                Some(upper_bound)
            } else {
                None
            };

            // Prepare cells read options
            let raw = self.db.raw().clone();
            let cells = &self.db.cells;
            let cells_cf = cells.get_unbounded_cf();

            let mut read_options = cells.new_read_config();
            if let Some(upper_bound) = upper_bound {
                read_options.set_iterate_upper_bound(upper_bound);
            }

            let write_options = cells.new_write_config();

            let total = total.clone();
            tasks.push(tokio::task::spawn_blocking(move || {
                let cells_cf = cells_cf.bound();

                // Prepare iterator
                let mut iter = raw.raw_iterator_cf_opt(&cells_cf, read_options);
                if let Some(lower_bound) = &lower_bound {
                    iter.seek(lower_bound);
                } else {
                    iter.seek_to_first();
                }

                // Iterate all cells in range
                let mut subtotal = 0;
                loop {
                    let (key, value) = match iter.item() {
                        Some(item) => item,
                        None => break iter.status()?,
                    };

                    if !value.is_empty() {
                        let marker = value[0];

                        if marker > 0 && marker != target_marker {
                            raw.delete_cf_opt(&cells_cf, key, &write_options)?;
                            subtotal += 1;
                        }
                    }

                    iter.next();
                }

                total.fetch_add(subtotal, Ordering::Relaxed);
                Ok::<_, rocksdb::Error>(())
            }));
        }

        // Wait for all tasks to complete
        while let Some(result) = tasks.next().await {
            result??
        }

        // Load counter
        Ok(total.load(Ordering::Relaxed))
    }

    pub fn mark_cells_tree(
        &self,
        root_cell: UInt256,
        target_marker: MarkerStrategy,
    ) -> Result<usize, CellStorageError> {
        let (target_marker, force, alter_persistent_edge) = match target_marker {
            MarkerStrategy::WhileDifferent { marker, force } => (marker, force, false),
            // Marker::PersistentStateTransition => (PS_TEMP_MARKER, true, true),
        };

        // Prepare handles
        let raw = self.db.raw();
        let cells = &self.db.cells;

        let cf = cells.cf();
        let read_config = cells.read_config();
        let write_config = cells.write_config();

        // Start from the root cell
        let mut stack = SmallVec::<[_; 256]>::with_capacity(256);
        stack.push(root_cell);

        let mut total = 0;

        let mut batch = rocksdb::WriteBatch::default();

        // While some cells left
        while let Some(cell_id) = stack.pop() {
            // Load cell marker and references from the top of the stack
            let (persistent_cell, marker_changed, references) =
                match raw.get_pinned_cf_opt(&cf, cell_id.as_slice(), read_config) {
                    Ok(Some(value)) => {
                        let (marker, references) =
                            match StorageCell::deserialize_marker_and_references(value.as_ref()) {
                                Ok(cell) => cell,
                                Err(_) => return Err(CellStorageError::InvalidCell),
                            };

                        let persistent_cell = marker.is_persistent();
                        let marker_changed =
                            (!persistent_cell || alter_persistent_edge) && marker != target_marker;

                        // Update cell data if marker changed
                        if marker_changed {
                            batch.merge_cf(&cf, cell_id.as_slice(), [target_marker.0]);
                        }

                        (persistent_cell, marker_changed, references)
                    }
                    Ok(None) => return Err(CellStorageError::CellNotFound),
                    Err(e) => return Err(CellStorageError::Internal(e)),
                };

            total += marker_changed as usize;

            // Add all children
            if !persistent_cell && (marker_changed || force) {
                for cell_id in references {
                    stack.push(cell_id.hash());
                }
            }
        }

        if let Err(e) = raw.write_opt(batch, write_config) {
            return Err(CellStorageError::Internal(e));
        }

        Ok(total)
    }

    pub fn drop_cell(&self, hash: &UInt256) {
        self.cells_cache.remove(hash);
    }
}

#[derive(Debug, Copy, Clone)]
pub enum MarkerStrategy {
    WhileDifferent { marker: Marker, force: bool },
    // PersistentStateTransition,
}

#[derive(thiserror::Error, Debug)]
pub enum CellStorageError {
    #[error("Cell not found in cell db")]
    CellNotFound,
    #[error("Invalid cell")]
    InvalidCell,
    #[error("Internal rocksdb error")]
    Internal(#[source] rocksdb::Error),
}

pub struct StorageCell {
    _c: countme::Count<Self>,
    cell_storage: Arc<CellStorage>,
    cell_data: ton_types::CellData,
    references: RwLock<SmallVec<[StorageCellReference; 4]>>,
    tree_bits_count: u64,
    tree_cell_count: u64,
}

impl StorageCell {
    pub fn repr_hash(&self) -> UInt256 {
        self.hash(ton_types::MAX_LEVEL)
    }

    pub fn deserialize(boc_db: Arc<CellStorage>, mut data: &[u8]) -> Result<Self> {
        // skip marker
        if data.is_empty() {
            return Err(StorageCellError::InvalidData.into());
        }
        data = &(*data)[1..];

        // deserialize cell
        let cell_data = ton_types::CellData::deserialize(&mut data)?;
        let references_count = data.read_byte()?;
        let mut references = SmallVec::with_capacity(references_count as usize);

        for _ in 0..references_count {
            let hash = UInt256::from(data.read_u256()?);
            references.push(StorageCellReference::Unloaded(hash));
        }

        let (tree_bits_count, tree_cell_count) = match data.read_le_u64() {
            Ok(tree_bits_count) => match data.read_le_u64() {
                Ok(tree_cell_count) => (tree_bits_count, tree_cell_count),
                Err(_) => (0, 0),
            },
            Err(_) => (0, 0),
        };

        Ok(Self {
            _c: Default::default(),
            cell_storage: boc_db,
            cell_data,
            references: RwLock::new(references),
            tree_bits_count,
            tree_cell_count,
        })
    }

    pub fn deserialize_marker_and_references(
        mut data: &[u8],
    ) -> Result<(Marker, SmallVec<[StorageCellReference; 4]>)> {
        let reader = &mut data;

        // skip marker
        let mut marker = 0u8;
        reader.read_exact(std::slice::from_mut(&mut marker))?;

        // deserialize cell
        let _cell_data = ton_types::CellData::deserialize(reader)?;
        let references_count = reader.read_byte()?;
        let mut references = SmallVec::with_capacity(references_count as usize);

        for _ in 0..references_count {
            let hash = UInt256::from(reader.read_u256()?);
            references.push(StorageCellReference::Unloaded(hash));
        }
        Ok((Marker(marker), references))
    }

    pub fn serialize_to(marker: Marker, cell: &dyn CellImpl, target: &mut Vec<u8>) -> Result<()> {
        target.clear();

        // write marker
        target.push(marker.0);

        // serialize cell
        let references_count = cell.references_count() as u8;
        cell.cell_data().serialize(target)?;
        target.push(references_count);

        for i in 0..references_count {
            target.extend_from_slice(cell.reference(i as usize)?.repr_hash().as_slice());
        }

        target.extend_from_slice(&cell.tree_bits_count().to_le_bytes());
        target.extend_from_slice(&cell.tree_cell_count().to_le_bytes());

        Ok(())
    }

    pub fn reference(&self, index: usize) -> Result<Arc<StorageCell>> {
        let hash = match &self.references.read().get(index) {
            Some(StorageCellReference::Unloaded(hash)) => *hash,
            Some(StorageCellReference::Loaded(cell)) => return Ok(cell.clone()),
            None => return Err(StorageCellError::AccessingInvalidReference.into()),
        };

        let storage_cell = self.cell_storage.load_cell(hash)?;
        self.references.write()[index] = StorageCellReference::Loaded(storage_cell.clone());

        Ok(storage_cell)
    }
}

impl CellImpl for StorageCell {
    fn data(&self) -> &[u8] {
        self.cell_data.data()
    }

    fn cell_data(&self) -> &ton_types::CellData {
        &self.cell_data
    }

    fn bit_length(&self) -> usize {
        self.cell_data.bit_length() as usize
    }

    fn references_count(&self) -> usize {
        self.references.read().len()
    }

    fn reference(&self, index: usize) -> Result<ton_types::Cell> {
        Ok(ton_types::Cell::with_cell_impl_arc(self.reference(index)?))
    }

    fn cell_type(&self) -> ton_types::CellType {
        self.cell_data.cell_type()
    }

    fn level_mask(&self) -> ton_types::LevelMask {
        self.cell_data.level_mask()
    }

    fn hash(&self, index: usize) -> UInt256 {
        self.cell_data.hash(index)
    }

    fn depth(&self, index: usize) -> u16 {
        self.cell_data.depth(index)
    }

    fn store_hashes(&self) -> bool {
        self.cell_data.store_hashes()
    }

    fn tree_bits_count(&self) -> u64 {
        self.tree_bits_count
    }

    fn tree_cell_count(&self) -> u64 {
        self.tree_cell_count
    }
}

impl Drop for StorageCell {
    fn drop(&mut self) {
        self.cell_storage.drop_cell(&self.repr_hash())
    }
}

#[derive(Clone)]
pub enum StorageCellReference {
    Loaded(Arc<StorageCell>),
    Unloaded(UInt256),
}

impl StorageCellReference {
    pub fn hash(&self) -> UInt256 {
        match self {
            Self::Loaded(cell) => cell.repr_hash(),
            Self::Unloaded(hash) => *hash,
        }
    }
}

#[derive(thiserror::Error, Debug)]
enum StorageCellError {
    #[error("Invalid data")]
    InvalidData,
    #[error("Accessing invalid cell reference")]
    AccessingInvalidReference,
}
