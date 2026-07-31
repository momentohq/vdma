//! Asynchronous server-side DMA: `DMA.GET` and `DMA.SET` both run on the pipelined fabric worker.
//!
//! The valkey thread only blocks the client, builds the request and submits. The worker
//! ([`dma_libfabric::FabricServer`]) keeps many transfers in flight off the GIL and returns
//! completions in batches; [`complete_batch`] finishes a whole batch under one GIL acquisition.
//!
//! - GET sources valkey's own value buffer zero-copy via `CreateStringReferenceFromKey`, a retained
//!   reference that is defrag-safe, falling back to a copy when the server lacks the API.
//! - SET lands in an off-keyspace `create_uninitialized` buffer and commits into the key with a
//!   no-copy `StringSet` only once the optional CRC checks out, so the keyspace never sees a
//!   half-written or corrupt value.

use std::collections::HashMap;
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, Mutex, OnceLock, PoisonError};

use dma_libfabric::{
    Completion, Configuration as FabricConfiguration, Direction, FabricServer, Outcome, Pool,
    Provider, TransferBuffer, TransferDone, TransferRequest,
};
use dma_traits::{Advertisement, DmaError, encode_hex};
use valkey_module::{
    BlockedClient, Context, ThreadSafeContext, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue,
};

use linkme::distributed_slice;
use valkey_module::server_events::{CLIENT_CHANGED_SERVER_EVENTS_LIST, ClientChangeSubevent};

use crate::valkey_error::command_error;
use crate::{static_state, valkey_logger};

/// The per-op token carried to the worker and back, holding the valkey handles needed to finish the
/// transfer. They are dereferenced only under the GIL, in [`finish_under_gil`].
enum OpToken {
    /// `DMA.GET`: reply, then release the retained source value.
    Get {
        blocked: BlockedClient,
        held: HeldValue,
        /// Held open until `finish_under_gil`, so the lifecycle tree closes when the API call does.
        span: tracing::Span,
    },
    /// `DMA.SET`: verify CRC, commit the DMA'd destination into the key or error, reply.
    Set {
        blocked: BlockedClient,
        /// The DMA landing buffer, allocated off the GIL by [`DestinationAllocator::allocate`]
        /// before the `fi_read` is posted. `None` until then, and if the worker was gone.
        destination: Option<ValkeyString>,
        key: ValkeyString,
        expected_crc: Option<u32>,
        span: tracing::Span,
    },
}

// SAFETY: the token crosses to the worker thread only to be carried back. Its valkey handles are
// dereferenced under the GIL in `finish_under_gil`; no valkey API is called on them off the GIL.
unsafe impl Send for OpToken {}

impl dma_libfabric::DestinationAllocator for OpToken {
    /// Allocate the SET landing buffer on the fabric worker rather than the valkey command thread,
    /// returning its pointer for the `fi_read`. GET supplies its source buffer in the request.
    fn allocate(&mut self, length: usize) -> *mut u8 {
        match self {
            OpToken::Set {
                destination, span, ..
            } => {
                let _alloc =
                    tracing::info_span!(parent: span.id(), "alloc_destination", length).entered();
                let mut value = ValkeyString::create_uninitialized(length);
                let pointer = value.as_mut_slice().as_mut_ptr();
                *destination = Some(value);
                pointer
            }
            OpToken::Get { .. } => ptr::null_mut(),
        }
    }
}

/// The value kept alive for a GET transfer.
enum HeldValue {
    /// A retained reference to valkey's own value buffer: shares the sds, pins the address.
    /// Released under the GIL once the transfer completes.
    Retained(ValkeyString),
    /// An owned copy, when the server lacks `CreateStringReferenceFromKey`.
    Copied(Vec<u8>),
}

impl HeldValue {
    fn as_bytes(&self) -> &[u8] {
        match self {
            HeldValue::Retained(value) => value.as_slice(),
            HeldValue::Copied(bytes) => bytes,
        }
    }
}

/// One worker per local EFA device, or a single worker for other providers, opened on first use.
/// Only valkey's single command thread starts these, so the lazy init cannot race.
static FABRIC_SERVERS: OnceLock<Vec<FabricServer<OpToken>>> = OnceLock::new();

/// Sticky client-to-device assignment, pinning a connection to one worker for its life. efa-direct
/// requires it: the client inserts that worker's address into its address vector via `dma.hello`, so
/// every command from it must reach the same device. New clients round-robin via `NEXT`.
static ASSIGNMENTS: LazyLock<Mutex<HashMap<u64, usize>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
static NEXT: AtomicUsize = AtomicUsize::new(0);

/// Start one worker per discovered EFA device for round-robin balancing, or a single worker for
/// other providers and when discovery finds nothing.
fn start_servers(
    context: &Context,
    fabric: &FabricConfiguration,
) -> Result<Vec<FabricServer<OpToken>>, DmaError> {
    let domains = if matches!(fabric.providers.first(), Some(Provider::EfaDirect)) {
        dma_libfabric::discover_domains(fabric).unwrap_or_default()
    } else {
        Vec::new()
    };
    // One pool shared across every fabric worker, which checksummed completions round-robin across.
    // Two threads per device by default, so CRC capacity scales with the fabric.
    let worker_count = domains.len().max(1);
    let pool_threads = fabric.crc_pool_threads.unwrap_or(2 * worker_count);
    let pool = Arc::new(
        Pool::new(pool_threads)
            .map_err(|error| DmaError::Fabric(format!("failed to start crc pool: {error}")))?,
    );
    if domains.is_empty() {
        valkey_logger::lifecycle(
            context,
            "valkey-dma: starting 1 fabric worker (single device)",
        );
        return Ok(vec![FabricServer::start(
            fabric,
            Box::new(complete_batch),
            pool,
        )?]);
    }
    valkey_logger::lifecycle(
        context,
        &format!(
            "valkey-dma: discovered {} EFA device(s) {domains:?}; starting one worker per device, \
             round-robining new sessions across them",
            domains.len()
        ),
    );
    let mut servers = Vec::with_capacity(domains.len());
    for domain in &domains {
        let mut per_device = fabric.clone();
        per_device.interfaces = vec![domain.clone()];
        servers.push(FabricServer::start(
            &per_device,
            Box::new(complete_batch),
            Arc::clone(&pool),
        )?);
    }
    Ok(servers)
}

fn fabric_servers(context: &Context) -> Result<&'static [FabricServer<OpToken>], DmaError> {
    if let Some(servers) = FABRIC_SERVERS.get() {
        return Ok(servers);
    }
    let configuration = static_state::configuration();
    let servers = start_servers(context, &configuration.dma_libfabric)?;
    let _ = FABRIC_SERVERS.set(servers);
    FABRIC_SERVERS
        .get()
        .map(Vec::as_slice)
        .ok_or_else(|| DmaError::Fabric("fabric servers unavailable".into()))
}

/// The worker assigned to `client_id`, assigning round-robin on first sight.
fn fabric_server_for(
    context: &Context,
    client_id: u64,
) -> Result<&'static FabricServer<OpToken>, DmaError> {
    let servers = fabric_servers(context)?;
    let mut assignments = ASSIGNMENTS.lock().unwrap_or_else(PoisonError::into_inner);
    let index = *assignments
        .entry(client_id)
        .or_insert_with(|| NEXT.fetch_add(1, Ordering::Relaxed) % servers.len());
    Ok(&servers[index])
}

/// Release a disconnecting client's fabric state, so its address-vector entry doesn't linger for the
/// endpoint's life.
#[distributed_slice(CLIENT_CHANGED_SERVER_EVENTS_LIST)]
static ON_CLIENT_CHANGE: fn(&Context, ClientChangeSubevent) = on_client_change;

fn on_client_change(context: &Context, subevent: ClientChangeSubevent) {
    if matches!(subevent, ClientChangeSubevent::Disconnected) {
        release_client(context.get_client_id());
    }
}

/// Drop a client's device assignment and ask its worker to remove its address-vector entry, which
/// the worker defers until the client's in-flight ops drain. A no-op for a client that never issued
/// a transfer.
fn release_client(client_id: u64) {
    let assigned = {
        let mut assignments = ASSIGNMENTS.lock().unwrap_or_else(PoisonError::into_inner);
        assignments.remove(&client_id)
    };
    if let Some(index) = assigned
        && let Some(server) = FABRIC_SERVERS.get().and_then(|servers| servers.get(index))
    {
        server.remove_peer(client_id);
    }
}

/// `DMA.HELLO`: the hex fabric address of the worker assigned to this client. The client inserts it
/// into its own address vector before issuing a transfer, so the server can RMA against it on
/// efa-direct, where the target must already hold the initiator's address. The assignment is sticky,
/// so this is the same device the client's transfers will use. Starts the workers if they aren't up.
pub fn dma_hello(context: &Context) -> ValkeyResult {
    let server = fabric_server_for(context, context.get_client_id()).map_err(command_error)?;
    Ok(ValkeyValue::SimpleString(encode_hex(
        server.local_address(),
    )))
}

/// Finish a batch of completed transfers.
fn complete_batch(completions: std::vec::Drain<Completion<OpToken>>) {
    // A tracing-cache workflow root for the completion phase.
    let _batch =
        tracing::info_span!(parent: None, "complete_batch", ops = completions.len()).entered();
    let thread_safe = ThreadSafeContext::new();

    // Under the valkey lock, do only what needs the gil
    let pending: Vec<PendingReply> = {
        let _gil = tracing::info_span!("gil").entered();
        let guard = {
            let _acquire = tracing::info_span!("acquire").entered();
            thread_safe.lock()
        };
        let _completions = tracing::info_span!("commit").entered();
        completions
            .map(|completion| {
                finish_under_gil(&guard, completion.caller_context, completion.outcome)
            })
            .collect()
    };

    let _replies = tracing::info_span!("reply").entered();
    for reply in pending {
        send_reply(reply);
    }
}

/// A reply staged during the gil phase, to be delivered outside the global lock.
struct PendingReply {
    blocked: BlockedClient,
    reply: ValkeyResult,
    /// Alive to the end, so the span reflects the whole transfer time.
    span: tracing::Span,
}

/// The gil-bound half of finishing a transfer: release GET source values, commit SET values.
fn finish_under_gil(context: &Context, caller_context: OpToken, outcome: Outcome) -> PendingReply {
    match caller_context {
        OpToken::Get {
            blocked,
            held,
            span,
        } => {
            // Attach by id, so it doesn't adopt `complete_batch` from the stack.
            let _finish =
                tracing::info_span!(parent: span.id(), "finish", operation = "get").entered();
            let reply = get_reply(outcome);
            drop(held); // releases the retained source value under the GIL
            PendingReply {
                blocked,
                reply,
                span,
            }
        }
        OpToken::Set {
            blocked,
            destination,
            key,
            expected_crc,
            span,
        } => {
            let _finish =
                tracing::info_span!(parent: span.id(), "finish", operation = "set").entered();
            // Under the GIL: `destination` moves into the key with no copy on success, or is freed
            // on error or CRC mismatch, and `key` drops at the end of this arm.
            let reply = set_reply(context, outcome, destination, &key, expected_crc);
            PendingReply {
                blocked,
                reply,
                span,
            }
        }
    }
}

/// The off-gil half of finishing a transfer: deliver the reply through the blocked client's
/// thread-safe context, which valkey's reply APIs don't need the gil for. Dropping the context
/// unblocks the client; dropping `span` closes the transfer once the reply is on its way.
fn send_reply(pending: PendingReply) {
    let PendingReply {
        blocked,
        reply,
        span,
    } = pending;
    let _reply = tracing::info_span!(parent: span.id(), "reply").entered();
    ThreadSafeContext::with_blocked_client(blocked).reply(reply);
}

/// The reply for a completed GET: `<bytes>`, or `<bytes> <crc>` when a checksum was requested.
fn get_reply(outcome: Outcome) -> ValkeyResult {
    match outcome {
        Ok(TransferDone {
            bytes,
            checksum: Some(checksum),
        }) => Ok(ValkeyValue::Array(vec![
            ValkeyValue::Integer(bytes as i64),
            ValkeyValue::Integer(i64::from(checksum)),
        ])),
        Ok(TransferDone {
            bytes,
            checksum: None,
        }) => Ok(ValkeyValue::Integer(bytes as i64)),
        Err(error) => Err(command_error(error)),
    }
}

/// Verify the worker's CRC against the client's and, on a match, move the DMA'd destination into the
/// key with no copy. A mismatch or transfer error leaves the key untouched and frees `destination`,
/// so a corrupt value is never committed.
fn set_reply(
    context: &Context,
    outcome: Outcome,
    destination: Option<ValkeyString>,
    key: &ValkeyString,
    expected_crc: Option<u32>,
) -> ValkeyResult {
    let done = match outcome {
        Ok(done) => done,
        Err(error) => return Err(command_error(error)),
    };
    if let Some(expected) = expected_crc {
        let actual = done.checksum.unwrap_or_default();
        if actual != expected {
            return Err(ValkeyError::String(format!(
                "ERR checksum mismatch: client {expected:#010x}, server {actual:#010x} \
                 (value not stored)"
            )));
        }
    }
    // The worker allocates the landing buffer before posting the read, so a successful transfer
    // always carries one.
    let destination = destination.ok_or(ValkeyError::Str("ERR landing buffer missing"))?;
    let _commit = tracing::info_span!("commit").entered();
    context
        .open_key_writable(key)
        .set_move(destination)
        .map_err(|_| ValkeyError::Str("ERR failed to store value"))?;
    Ok(ValkeyValue::Integer(done.bytes as i64))
}

/// DMA the value at `key` to the client's exposed buffer on the worker, replying when it completes.
/// Synchronously `Null` if the key is absent, or an error if the value exceeds the client's
/// advertised capacity.
pub fn dma_get(
    context: &Context,
    advertisement: &Advertisement,
    key: &ValkeyString,
    capacity: usize,
    want_checksum: bool,
) -> ValkeyResult {
    // A tracing-cache root: contextual spans with no active span on the stack are dropped, so the
    // lifecycle's top span must be an explicit root for its children to cache under it.
    let transfer = tracing::info_span!(parent: None, "dma.get", length = tracing::field::Empty);
    // Entered only for the synchronous setup. The request and caller context keep it open, though
    // not entered, across the worker's wire and completion phases, so it times the whole API call.
    let _handle = transfer.enter();
    let read = tracing::info_span!("read_key").entered();
    let valkey_key = context.open_key(key);
    let bytes = match valkey_key.read() {
        Ok(Some(bytes)) => bytes,
        Ok(None) => return Ok(ValkeyValue::Null),
        Err(_) => return Err(ValkeyError::Str("ERR failed to read key")),
    };
    let length = bytes.len();
    if length > capacity {
        return Err(ValkeyError::String(format!(
            "ERR value of {length} bytes exceeds the client buffer of {capacity} bytes"
        )));
    }
    transfer.record("length", length);
    drop(read);

    // Retain valkey's own value buffer where supported, else copy.
    let retain = tracing::info_span!("retain_value").entered();
    let held = match valkey_key.retained_string_value() {
        Some(value) => {
            note_zero_copy(context);
            HeldValue::Retained(value)
        }
        None => {
            note_fallback(context);
            HeldValue::Copied(bytes.to_vec())
        }
    };
    let pointer = held.as_bytes().as_ptr().cast_mut();
    drop(retain);

    // Block the client, build the request, hand it to the fabric worker.
    let _submit = tracing::info_span!("submit").entered();
    let client_id = context.get_client_id();
    let server = match fabric_server_for(context, client_id) {
        Ok(server) => server,
        Err(error) => return Err(command_error(error)),
    };
    let blocked = context.block_client();
    // Stop entering the transfer span but keep it open: its `Id` rides on the request for the worker
    // to parent `wire`, and the one owning `Span` moves into the caller context, to close at
    // `finish_under_gil`. Never cloned — tracing-cache closes on first drop.
    drop(_submit);
    drop(_handle);
    let request = TransferRequest {
        client_id,
        peer_address: advertisement.address.clone(),
        remote_key: advertisement.remote_key,
        remote_address: advertisement.remote_address,
        buffer: TransferBuffer { pointer, length },
        direction: Direction::ToPeer,
        want_checksum,
        parent_id: transfer.id(),
        caller_context: OpToken::Get {
            blocked,
            held,
            span: transfer,
        },
    };
    submit(context, server, request);
    Ok(ValkeyValue::NoReply)
}

/// DMA the payload from the peer into an off-keyspace buffer on the worker, then verify the CRC and
/// commit it into `key`. Replies when it completes.
pub fn dma_set(
    context: &Context,
    advertisement: &Advertisement,
    key: ValkeyString,
    length: usize,
    expected_crc: Option<u32>,
) -> ValkeyResult {
    // A root span entered only for setup, kept open across the worker phases — see `dma_get`.
    let transfer = tracing::info_span!(parent: None, "dma.set", length);
    let _handle = transfer.enter();

    let _submit = tracing::info_span!("submit").entered();
    let client_id = context.get_client_id();
    let server = match fabric_server_for(context, client_id) {
        Ok(server) => server,
        Err(error) => return Err(command_error(error)),
    };
    let blocked = context.block_client();
    // Stop entering the transfer span, keeping it open — see `dma_get`.
    drop(_submit);
    drop(_handle);
    let request = TransferRequest {
        client_id,
        peer_address: advertisement.address.clone(),
        remote_key: advertisement.remote_key,
        remote_address: advertisement.remote_address,
        // the worker allocates this off the gil
        buffer: TransferBuffer {
            pointer: ptr::null_mut(),
            length,
        },
        direction: Direction::FromPeer,
        want_checksum: expected_crc.is_some(),
        parent_id: transfer.id(),
        caller_context: OpToken::Set {
            blocked,
            destination: None,
            key,
            expected_crc,
            span: transfer,
        },
    };
    submit(context, server, request);
    Ok(ValkeyValue::NoReply)
}

/// Submit a request, finishing it inline if the worker is gone so the blocked client isn't stranded.
fn submit(context: &Context, server: &FabricServer<OpToken>, request: TransferRequest<OpToken>) {
    if let Err(request) = server.submit(request) {
        // Command thread with the GIL held: do the keyspace half, then reply with the failure.
        let pending = finish_under_gil(
            context,
            request.caller_context,
            Err(DmaError::Fabric("fabric worker is gone".into())),
        );
        send_reply(pending);
    }
}

/// Announce, once, that `DMA.GET` is on the zero-copy retained-value path.
fn note_zero_copy(context: &Context) {
    static LOGGED: AtomicBool = AtomicBool::new(false);
    if !LOGGED.swap(true, Ordering::Relaxed) {
        valkey_logger::lifecycle(
            context,
            "DMA.GET: zero-copy path active (retained value buffer)",
        );
    }
}

/// Announce, once, that `DMA.GET` fell back to copying because the server lacks
/// `CreateStringReferenceFromKey`.
fn note_fallback(context: &Context) {
    static LOGGED: AtomicBool = AtomicBool::new(false);
    if !LOGGED.swap(true, Ordering::Relaxed) {
        valkey_logger::fatal(
            context,
            "DMA.GET: FALLBACK copy path active — server does not export \
             CreateStringReferenceFromKey; every GET copies the value. Rebuild valkey with the \
             CreateStringReferenceFromKey module API.",
        );
    }
}
