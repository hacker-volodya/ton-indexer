use std::sync::atomic::{self, AtomicBool, AtomicI32, AtomicI64, AtomicU32, AtomicU64};
use std::sync::Arc;
use std::time::{Duration, Instant};

use adnl::common::{KeyId, KeyOption, Query, Wait};
use adnl::node::AddressCache;
use anyhow::{anyhow, Result};
use dashmap::{DashMap, DashSet};
use dht::DhtNode;
use overlay::{OverlayNode, OverlayShortId};
use rand::Rng;
use ton_api::ton::rpc;
use ton_api::ton::{ton_node::Capabilities, TLObject};

#[derive(Debug)]
pub struct Neighbour {
    id: Arc<KeyId>,
    last_ping: AtomicU64,
    proto_version: AtomicI32,
    capabilities: AtomicI64,
    roundtrip_adnl: AtomicU64,
    roundtrip_rldp: AtomicU64,
    all_attempts: AtomicU64,
    fail_attempts: AtomicU64,
    fines_points: AtomicU32,
    active_check: AtomicBool,
    unreliability: AtomicU32,
}

impl Neighbour {
    pub fn new(id: Arc<KeyId>) -> Self {
        Self {
            id,
            last_ping: Default::default(),
            proto_version: Default::default(),
            capabilities: Default::default(),
            roundtrip_adnl: Default::default(),
            roundtrip_rldp: Default::default(),
            all_attempts: Default::default(),
            fail_attempts: Default::default(),
            fines_points: Default::default(),
            active_check: Default::default(),
            unreliability: Default::default(),
        }
    }

    pub fn update_proto_version(&self, data: &Capabilities) {
        self.proto_version
            .store(*data.version(), atomic::Ordering::Release);
        self.capabilities
            .store(*data.capabilities(), atomic::Ordering::Release);
    }

    pub fn id(&self) -> &Arc<KeyId> {
        &self.id
    }

    pub fn query_success(&self, roundtrip: u64, is_rldp: bool) {
        loop {
            let old_unreliability = self.unreliability.load(atomic::Ordering::Acquire);
            if old_unreliability > 0 {
                let new_unreliability = old_unreliability - 1;
                if self
                    .unreliability
                    .compare_exchange(
                        old_unreliability,
                        new_unreliability,
                        atomic::Ordering::Release,
                        atomic::Ordering::Relaxed,
                    )
                    .is_err()
                {
                    continue;
                }
            }
            break;
        }
        if is_rldp {
            self.update_roundtrip_rldp(roundtrip)
        } else {
            self.update_roundtrip_adnl(roundtrip)
        }
    }

    pub fn query_failed(&self, roundtrip: u64, is_rldp: bool) {
        self.unreliability.fetch_add(1, atomic::Ordering::Release);
        if is_rldp {
            self.update_roundtrip_rldp(roundtrip)
        } else {
            self.update_roundtrip_adnl(roundtrip)
        }
    }

    pub fn roundtrip_adnl(&self) -> Option<u64> {
        Self::roundtrip(&self.roundtrip_adnl)
    }

    pub fn roundtrip_rldp(&self) -> Option<u64> {
        Self::roundtrip(&self.roundtrip_rldp)
    }

    pub fn update_roundtrip_adnl(&self, roundtrip: u64) {
        Self::set_roundtrip(&self.roundtrip_adnl, roundtrip)
    }

    pub fn update_roundtrip_rldp(&self, roundtrip: u64) {
        Self::set_roundtrip(&self.roundtrip_rldp, roundtrip)
    }

    fn last_ping(&self) -> u64 {
        self.last_ping.load(atomic::Ordering::Acquire)
    }

    fn set_last_ping(&self, elapsed: u64) {
        self.last_ping.store(elapsed, atomic::Ordering::Release)
    }

    fn roundtrip(storage: &AtomicU64) -> Option<u64> {
        let roundtrip = storage.load(atomic::Ordering::Acquire);
        if roundtrip == 0 {
            None
        } else {
            Some(roundtrip)
        }
    }
    fn set_roundtrip(storage: &AtomicU64, roundtrip: u64) {
        let roundtrip_old = storage.load(atomic::Ordering::Acquire);
        let roundtrip = if roundtrip_old > 0 {
            (roundtrip_old + roundtrip) / 2
        } else {
            roundtrip
        };
        storage.store(roundtrip, atomic::Ordering::Release);
    }
}

pub const MAX_NEIGHBOURS: usize = 16;

pub const PROTO_VERSION: i32 = 2;
pub const PROTO_CAPABILITIES: i64 = 1;
pub const STOP_UNRELIABILITY: u32 = 5;
pub const FAIL_UNRELIABILITY: u32 = 10;

const FINES_POINTS_COUNT: u32 = 100;

pub struct Neighbours {
    peers: NeighboursCache,
    all_peers: DashSet<Arc<KeyId>>,
    overlay_id: Arc<OverlayShortId>,
    overlay: Arc<OverlayNode>,
    dht: Arc<DhtNode>,
    fail_attempts: AtomicU64,
    all_attempts: AtomicU64,
    start: Instant,
}

impl Neighbours {
    const TIMEOUT_PING_MAX: u64 = 1000; // Milliseconds
    const TIMEOUT_PING_MIN: u64 = 10; // Milliseconds

    pub fn new(
        start_peers: &[Arc<KeyId>],
        dht: &Arc<DhtNode>,
        overlay: &Arc<OverlayNode>,
        overlay_id: Arc<OverlayShortId>,
    ) -> Result<Self> {
        Ok(Self {
            peers: NeighboursCache::new(start_peers)?,
            all_peers: Default::default(),
            overlay_id,
            overlay: overlay.clone(),
            dht: dht.clone(),
            fail_attempts: Default::default(),
            all_attempts: Default::default(),
            start: Instant::now(),
        })
    }

    pub fn count(&self) -> usize {
        self.peers.count()
    }

    pub fn add(&self, peer: Arc<KeyId>) -> Result<bool> {
        if self.count() >= MAX_NEIGHBOURS {
            return Ok(false);
        }
        self.peers.insert(peer)
    }

    pub fn contains(&self, peer: &Arc<KeyId>) -> bool {
        self.peers.contains(peer)
    }

    pub fn contains_overlay_peer(&self, id: &Arc<KeyId>) -> bool {
        self.all_peers.contains(id)
    }

    pub fn add_overlay_peer(&self, id: Arc<KeyId>) {
        self.all_peers.insert(id);
    }

    pub fn remove_overlay_peer(&self, id: &Arc<KeyId>) {
        self.all_peers.remove(id);
    }

    pub fn got_neighbours(&self, peers: AddressCache) -> Result<()> {
        log::trace!("got_neighbours");
        let mut ex = false;
        let mut rng = rand::thread_rng();
        let mut is_delete_peer = false;

        let (mut iter, mut current) = peers.first();
        while let Some(elem) = current {
            if self.contains(&elem) {
                current = peers.next(&mut iter);
                continue;
            }
            let count = self.peers.count();

            if count == MAX_NEIGHBOURS {
                let mut a: Option<Arc<KeyId>> = None;
                let mut b: Option<Arc<KeyId>> = None;
                let mut u: u32 = 0;

                for (cnt, current) in self.peers.get_iter().enumerate() {
                    let un = current.unreliability.load(atomic::Ordering::Acquire);
                    if un > u {
                        u = un;
                        a = Some(current.id.clone());
                    }
                    if cnt == 0 || rng.gen_range(0, cnt) == 0 {
                        b = Some(current.id.clone());
                    }
                }

                let mut deleted_peer = b;

                if u > STOP_UNRELIABILITY {
                    deleted_peer = a;
                    is_delete_peer = true;
                } else {
                    ex = true;
                }
                let deleted_peer = deleted_peer
                    .ok_or_else(|| anyhow!("Internal error: deleted peer is not set!"))?;
                self.peers.replace(&deleted_peer, elem.clone())?;

                if is_delete_peer {
                    self.overlay
                        .delete_public_peer(&deleted_peer, &self.overlay_id)
                        .map_err(|e| anyhow!("Failed to delete public peer: {}", e))?;
                    self.remove_overlay_peer(&deleted_peer);
                    is_delete_peer = false;
                }
            } else {
                self.peers.insert(elem.clone())?;
            }

            if ex {
                break;
            }
            current = peers.next(&mut iter);
        }

        log::trace!("/got_neighbours");
        Ok(())
    }

    pub fn start_reload(self: Arc<Self>) {
        tokio::spawn(async move {
            loop {
                let sleep_time = rand::thread_rng().gen_range(10, 30);
                tokio::time::sleep(Duration::from_secs(sleep_time)).await;
                if let Err(e) = self.reload_neighbours(&self.overlay_id).await {
                    log::warn!("Failed to reload neighbours: {}", e);
                }
            }
        });
    }

    pub fn start_ping(self: Arc<Self>) {
        tokio::spawn(async move {
            loop {
                if let Err(e) = self.ping_neighbours().await {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    log::warn!("Failed to ping neighbours: {}", e);
                }
            }
        });
    }

    pub async fn reload_neighbours(&self, overlay_id: &Arc<OverlayShortId>) -> Result<()> {
        log::trace!("start reload_neighbours (overlay: {})", overlay_id);
        let neighbours_cache = AddressCache::with_limit((MAX_NEIGHBOURS * 2 + 1) as u32);
        self.overlay
            .get_cached_random_peers(&neighbours_cache, overlay_id, (MAX_NEIGHBOURS * 2) as u32)
            .map_err(|e| anyhow!("Failed to get cached random peers: {}", e))?;
        self.got_neighbours(neighbours_cache)?;
        log::trace!("finish reload_neighbours (overlay: {})", overlay_id);
        Ok(())
    }

    pub fn start_rnd_peers_process(self: Arc<Self>) {
        let _handler = tokio::spawn(async move {
            let receiver = self.overlay.clone();
            let id = self.overlay_id.clone();
            log::trace!("wait random peers...");
            loop {
                let this = self.clone();
                tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
                for peer in this.peers.get_iter() {
                    match receiver.get_random_peers(&peer.id(), &id, None).await {
                        Ok(Some(peers)) => {
                            let mut new_peers = Vec::new();

                            for peer in peers.iter() {
                                match KeyOption::from_tl_public_key(&peer.id) {
                                    Ok(peer_key) => {
                                        if !this.contains_overlay_peer(peer_key.id()) {
                                            new_peers.push(peer_key.id().clone());
                                        }
                                    }
                                    Err(e) => log::warn!("{}", e),
                                }
                            }
                            if !new_peers.is_empty() {
                                this.clone().add_new_peers(new_peers);
                            }
                        }
                        Err(e) => {
                            log::warn!("call get_random_peers is error: {}", e);
                        }
                        _ => {}
                    }
                }
            }
        });
    }

    fn add_new_peers(self: Arc<Self>, peers: Vec<Arc<KeyId>>) {
        let this = self;
        tokio::spawn(async move {
            for peer in peers.iter() {
                log::trace!("add_new_peers: start find address: peer {}", peer);
                match DhtNode::find_address(&this.dht, peer).await {
                    Ok((ip, _)) => {
                        log::info!("add_new_peers: addr peer {}", ip);
                        this.add_overlay_peer(peer.clone());
                    }
                    Err(e) => {
                        log::warn!("add_new_peers: find address error - {}", e);
                        continue;
                    }
                }
            }
        });
    }

    pub fn choose_neighbour(&self) -> Result<Option<Arc<Neighbour>>> {
        let count = self.peers.count();
        if count == 0 {
            return Ok(None);
        }

        let mut rng = rand::thread_rng();
        let mut best: Option<Arc<Neighbour>> = None;
        let mut sum = 0;
        let node_stat = self.fail_attempts.load(atomic::Ordering::Acquire) as f64
            / self.all_attempts.load(atomic::Ordering::Acquire) as f64;

        log::trace!("Select neighbour for overlay {}", self.overlay_id);
        for neighbour in self.peers.get_iter() {
            let mut unr = neighbour.unreliability.load(atomic::Ordering::Acquire);
            let proto_version = neighbour.proto_version.load(atomic::Ordering::Acquire);
            let capabilities = neighbour.capabilities.load(atomic::Ordering::Acquire);
            let roundtrip_rldp = neighbour.roundtrip_rldp.load(atomic::Ordering::Acquire);
            let roundtrip_adnl = neighbour.roundtrip_adnl.load(atomic::Ordering::Acquire);
            let peer_stat = neighbour.fail_attempts.load(atomic::Ordering::Acquire) as f64
                / neighbour.all_attempts.load(atomic::Ordering::Acquire) as f64;
            let fines_points = neighbour.fines_points.load(atomic::Ordering::Acquire);

            if count == 1 {
                return Ok(Some(neighbour));
            }
            if proto_version < PROTO_VERSION {
                unr += 4;
            } else if proto_version == PROTO_VERSION && capabilities < PROTO_CAPABILITIES {
                unr += 2;
            }

            log::trace!(
                "Neighbour {}, unr {}, rt ADNL {}, rt RLDP {} (all stat: {:.4}, peer stat: {:.4}/{}))",
                neighbour.id(), unr,
                roundtrip_adnl,
                roundtrip_rldp,
                node_stat,
                peer_stat,
                fines_points
            );
            if unr <= FAIL_UNRELIABILITY {
                if node_stat + (node_stat * 0.2f64) < peer_stat {
                    if fines_points > 0 {
                        let _ = neighbour.fines_points.fetch_update(
                            atomic::Ordering::Release,
                            atomic::Ordering::Relaxed,
                            |x| if x > 0 { Some(x - 1) } else { None },
                        );
                        continue;
                    }
                    neighbour
                        .active_check
                        .store(true, atomic::Ordering::Release);
                }

                let w = (1 << (FAIL_UNRELIABILITY - unr)) as i64;
                sum += w;

                if rng.gen_range(0, sum) < w {
                    best = Some(neighbour.clone());
                }
            }
        }

        if let Some(best) = &best {
            log::trace!("Selected neighbour {}", best.id);
        } else {
            log::trace!("Selected neighbour None");
        }
        Ok(best)
    }
    pub fn update_neighbour_stats(
        &self,
        peer: &Arc<KeyId>,
        roundtrip: u64,
        success: bool,
        is_rldp: bool,
        is_register: bool,
    ) -> Result<()> {
        log::trace!("update_neighbour_stats");
        let it = &self.peers.get(peer);
        if let Some(neighbour) = it {
            if success {
                neighbour.query_success(roundtrip, is_rldp);
            } else {
                neighbour.query_failed(roundtrip, is_rldp);
            }
            if is_register {
                neighbour
                    .all_attempts
                    .fetch_add(1, atomic::Ordering::Release);
                self.all_attempts.fetch_add(1, atomic::Ordering::Release);
                if !success {
                    neighbour
                        .fail_attempts
                        .fetch_add(1, atomic::Ordering::Release);
                    self.fail_attempts.fetch_add(1, atomic::Ordering::Release);
                }
                if neighbour.active_check.load(atomic::Ordering::Acquire) {
                    if !success {
                        neighbour
                            .fines_points
                            .fetch_add(FINES_POINTS_COUNT, atomic::Ordering::Release);
                    }
                    neighbour
                        .active_check
                        .store(false, atomic::Ordering::Release);
                }
            };
        }
        log::trace!("/update_neighbour_stats");
        Ok(())
    }

    pub fn got_neighbour_capabilities(
        &self,
        peer: &Arc<KeyId>,
        _roundtrip: u64,
        capabilities: &Capabilities,
    ) -> Result<()> {
        if let Some(it) = &self.peers.get(peer) {
            it.update_proto_version(capabilities);
        }
        Ok(())
    }

    pub async fn ping_neighbours(self: &Arc<Self>) -> Result<()> {
        let count = self.peers.count();
        if count == 0 {
            anyhow::bail!("No peers in overlay {}", self.overlay_id)
        } else {
            log::trace!("neighbours: overlay {} count {}", self.overlay_id, count);
        }

        let max_count = if count < 6 { count } else { 6 };
        let (wait, mut queue_reader) = Wait::new();
        loop {
            let peer = if let Some(peer) = self.peers.next_for_ping(&self.start)? {
                peer
            } else {
                tokio::time::sleep(Duration::from_millis(Self::TIMEOUT_PING_MIN)).await;
                log::trace!("next_for_ping return None");
                continue;
            };

            let last = self.start.elapsed().as_millis() as u64 - peer.last_ping();
            if last < Self::TIMEOUT_PING_MAX {
                tokio::time::sleep(Duration::from_millis(Self::TIMEOUT_PING_MAX - last)).await;
            } else {
                tokio::time::sleep(Duration::from_millis(Self::TIMEOUT_PING_MIN)).await;
            }

            let self_cloned = self.clone();
            let wait_cloned = wait.clone();
            let mut count = wait.request();

            tokio::spawn(async move {
                if let Err(e) = self_cloned.update_capabilities(peer).await {
                    log::warn!("ERROR: {}", e)
                }
                wait_cloned.respond(Some(()));
            });

            while count >= max_count {
                wait.wait(&mut queue_reader, false).await;
                count -= 1;
            }
        }
    }

    async fn update_capabilities(self: Arc<Self>, peer: Arc<Neighbour>) -> Result<()> {
        let now = Instant::now();
        peer.set_last_ping(self.start.elapsed().as_millis() as u64);
        let query = TLObject::new(rpc::ton_node::GetCapabilities);
        log::trace!("Query capabilities from {} {}", peer.id, self.overlay_id);
        let timeout = Some(adnl::node::AdnlNode::calc_timeout(peer.roundtrip_adnl()));

        match self
            .overlay
            .query(&peer.id, &query, &self.overlay_id, timeout)
            .await
        {
            Ok(Some(answer)) => {
                let caps: Capabilities = Query::parse(answer, &query)
                    .map_err(|e| anyhow!("Failed to parse query: {}", e))?;
                log::trace!(
                    "Got capabilities from {} {}: {:?}",
                    peer.id,
                    self.overlay_id,
                    caps
                );

                let roundtrip = now.elapsed().as_millis() as u64;
                self.update_neighbour_stats(&peer.id, roundtrip, true, false, false)?;
                log::trace!("Good caps {}: {}", peer.id, self.overlay_id);
                self.got_neighbour_capabilities(&peer.id, roundtrip, &caps)?;
                Ok(())
            }
            _ => {
                log::trace!("Bad caps {}: {}", peer.id, self.overlay_id);
                anyhow::bail!("Capabilities were not received from {}", peer.id)
            }
        }
    }
}

#[derive(Clone)]
pub struct NeighboursCache {
    cache: Arc<NeighboursCacheCore>,
}

impl NeighboursCache {
    pub fn new(start_peers: &[Arc<KeyId>]) -> Result<Self> {
        Ok(Self {
            cache: Arc::new(NeighboursCacheCore::new(start_peers)?),
        })
    }

    pub fn contains(&self, peer: &Arc<KeyId>) -> bool {
        self.cache.contains(peer)
    }

    pub fn insert(&self, peer: Arc<KeyId>) -> Result<bool> {
        self.cache.insert(peer)
    }

    pub fn count(&self) -> usize {
        self.cache.count()
    }

    pub fn get(&self, peer: &Arc<KeyId>) -> Option<Arc<Neighbour>> {
        self.cache.get(peer)
    }

    pub fn next_for_ping(&self, start: &Instant) -> Result<Option<Arc<Neighbour>>> {
        self.cache.next_for_ping(start)
    }

    pub fn replace(&self, old: &Arc<KeyId>, new: Arc<KeyId>) -> Result<bool> {
        self.cache.replace(old, new)
    }

    pub fn get_iter(&self) -> NeighboursCacheIterator {
        NeighboursCacheIterator::new(self.cache.clone())
    }
}

struct NeighboursCacheCore {
    count: AtomicU32,
    next: AtomicU32,
    indices: DashMap<u32, Arc<KeyId>>,
    values: DashMap<Arc<KeyId>, Arc<Neighbour>>,
}

impl NeighboursCacheCore {
    pub fn new(start_peers: &[Arc<KeyId>]) -> Result<Self> {
        let instance = Self {
            count: Default::default(),
            next: Default::default(),
            indices: Default::default(),
            values: Default::default(),
        };

        let mut index = 0;
        for peer in start_peers.iter() {
            if index < MAX_NEIGHBOURS {
                instance.insert(peer.clone())?;
                index += 1;
            }
        }

        Ok(instance)
    }

    pub fn contains(&self, peer: &Arc<KeyId>) -> bool {
        self.values.contains_key(peer)
    }

    pub fn insert(&self, peer: Arc<KeyId>) -> Result<bool> {
        self.insert_ex(peer, false)
    }

    pub fn count(&self) -> usize {
        self.count.load(atomic::Ordering::Acquire) as usize
    }

    pub fn get(&self, peer: &Arc<KeyId>) -> Option<Arc<Neighbour>> {
        self.values.get(peer).map(|result| result.value().clone())
    }

    pub fn next_for_ping(&self, start: &Instant) -> Result<Option<Arc<Neighbour>>> {
        let mut next = self.next.load(atomic::Ordering::Acquire);
        let count = self.count.load(atomic::Ordering::Acquire);
        let started_from = next;

        let mut result: Option<Arc<Neighbour>> = None;
        loop {
            let key_id = match self.indices.get(&next) {
                Some(key_id) => key_id,
                None => anyhow::bail!("Neighbour index not found"),
            };

            match self.values.get(key_id.value()) {
                Some(neighbour) => {
                    next = (next + 1) % count;
                    self.next.store(next, atomic::Ordering::Release);
                    let neighbour = neighbour.value();
                    if start.elapsed().as_millis() as u64 - neighbour.last_ping() < 1000 {
                        if next == started_from {
                            break;
                        } else if let Some(result) = &result {
                            if neighbour.last_ping() >= result.last_ping() {
                                continue;
                            }
                        }
                    }

                    result.replace(neighbour.clone());
                    break;
                }
                None => continue,
            }
        }

        Ok(result)
    }

    fn insert_ex(&self, peer: Arc<KeyId>, silent_insert: bool) -> Result<bool> {
        use dashmap::mapref::entry::Entry;

        let count = self.count.load(atomic::Ordering::Acquire);
        if !silent_insert && (count >= MAX_NEIGHBOURS as u32) {
            anyhow::bail!("NeighboursCache overflow");
        }

        match self.values.entry(peer.clone()) {
            Entry::Vacant(entry) => {
                let mut index = 0;
                if !silent_insert {
                    index = self.count.fetch_add(1, atomic::Ordering::Acquire);
                    if index >= MAX_NEIGHBOURS as u32 {
                        self.count.fetch_sub(1, atomic::Ordering::Release);
                        anyhow::bail!("NeighboursCache overflow")
                    }
                }

                entry.insert(Arc::new(Neighbour::new(peer.clone())));

                if !silent_insert {
                    self.indices.insert(index, peer);
                }
                Ok(true)
            }
            Entry::Occupied(_) => Ok(false),
        }
    }

    pub fn replace(&self, old: &Arc<KeyId>, new: Arc<KeyId>) -> Result<bool> {
        log::info!("started replace (old: {}, new: {})", &old, &new);
        let index = match self.get_index(old) {
            Some(index) => index,
            None => anyhow::bail!("replaced neighbour not found!"),
        };

        log::info!(
            "replace func use index: {} (old: {}, new: {})",
            &index,
            &old,
            &new
        );
        let status_insert = self.insert_ex(new.clone(), true)?;

        if status_insert {
            self.indices.insert(index, new);
            self.values.remove(old);
        }
        log::info!("finish replace (old: {})", &old);
        Ok(status_insert)
    }

    fn get_index(&self, peer: &Arc<KeyId>) -> Option<u32> {
        for index in self.indices.iter() {
            if index.value().cmp(peer) == std::cmp::Ordering::Equal {
                return Some(*index.key());
            }
        }
        None
    }
}

pub struct NeighboursCacheIterator {
    current: i32,
    parent: Arc<NeighboursCacheCore>,
}

impl NeighboursCacheIterator {
    fn new(parent: Arc<NeighboursCacheCore>) -> Self {
        NeighboursCacheIterator {
            current: -1,
            parent,
        }
    }
}

impl Iterator for NeighboursCacheIterator {
    type Item = Arc<Neighbour>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut result = None;

        let current = self.current + 1;
        for _ in 0..5 {
            let key_id = if let Some(key_id) = &self.parent.indices.get(&(current as u32)) {
                key_id.value().clone()
            } else {
                return None;
            };

            if let Some(neighbour) = &self.parent.values.get(&key_id) {
                self.current = current;
                result = Some(neighbour.value().clone());
                break;
            } else {
                // Value has been updated. Repeat step
                continue;
            }
        }

        result
    }
}
