//! The valkey module entry point: the `valkey_module!` declaration, `init`, and command handlers.

use configuration::Configuration;
use dma_libfabric_protocol::Advertisement;
use valkey_module::alloc::ValkeyAlloc;
use valkey_module::configuration::ConfigurationFlags;
use valkey_module::{
    Context, Status, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue, valkey_module,
};

use crate::memory::RegionMode;
use crate::valkey_error::command_error;
use crate::{memory, observability, session, static_state, transfer, valkey_logger};

valkey_module! {
    name: "valkey-dma",
    version: 1,
    allocator: (ValkeyAlloc, ValkeyAlloc),
    data_types: [],
    init: init,
    commands: [
        ["dma.info", dma_info_command, "readonly", 0, 0, 0],
        ["dma.hello", dma_hello_command, "readonly", 0, 0, 0],
        ["dma.set", dma_set_command, "write deny-oom", 5, 5, 1],
        ["dma.get", dma_get_command, "readonly", 5, 5, 1],
    ],
    configurations: [
        i64: [],
        string: [
            ["config", unsafe { crate::static_state::module_config_file() }, "./valkey-dma.toml", ConfigurationFlags::IMMUTABLE, None],
        ],
        bool: [],
        enum: [],
        module_args_as_configuration: true,
    ]
}

fn init(context: &Context, _args: &[ValkeyString]) -> Status {
    valkey_logger::lifecycle(
        context,
        &format!(
            "valkey-dma module starting, build {}",
            env!("VDMA_BUILD_ID")
        ),
    );

    let configuration = match load_configuration() {
        Ok(configuration) => configuration,
        Err(error) => {
            valkey_logger::fatal(context, &format!("failed to load configuration: {error}"));
            return Status::Err;
        }
    };

    observability::install(&configuration.observability);

    configure_allocator(
        context,
        configuration.jemalloc.huge_arena_decay_ms,
        configuration.jemalloc.huge_arena_oversize_threshold,
    );

    static_state::set_configuration(configuration);
    let active = static_state::configuration();
    valkey_logger::lifecycle(
        context,
        &format!(
            "valkey-dma configuration loaded: dma-libfabric providers {:?}",
            active.dma_libfabric.providers
        ),
    );
    Status::Ok
}

/// Settle how RMA memory regions are reclaimed. With jemalloc, install extent hooks that deregister
/// a region as the pages it covers are reclaimed, so page decay can stay on without a registration
/// going stale, and tune the huge arena where oversize values live so freed ones recycle in place.
/// Without it, fall back to registering per transfer. See [`crate::memory::install`].
fn configure_allocator(
    context: &Context,
    huge_arena_decay_ms: i64,
    huge_arena_oversize_threshold: usize,
) {
    let report = memory::install(huge_arena_decay_ms, huge_arena_oversize_threshold);

    if RegionMode::PerOperation == report.mode {
        valkey_logger::warning(
            context,
            "valkey-dma: no jemalloc in the host, so RMA operands are registered per transfer and \
             deregistered on completion. Correct, but fi_mr_reg runs on the fabric worker for every \
             transfer — expect far lower throughput. Run against a jemalloc build for production.",
        );
        return;
    }

    let huge = report
        .huge_arena
        .map_or_else(|| "none".to_string(), |index| index.to_string());
    valkey_logger::lifecycle(
        context,
        &format!(
            "valkey-dma: tuned huge arena {huge} — decay window {}ms, oversize_threshold {} MiB; \
             arenas are hooked lazily per registration (decay stays ON — hooks deregister covered \
             pages before purge; freed values below the threshold recycle in place)",
            report.huge_arena_decay_ms,
            report.huge_arena_oversize_threshold / (1024 * 1024),
        ),
    );
}

/// Load configuration from the wired TOML path, falling back to defaults when none is configured.
fn load_configuration() -> Result<Configuration, String> {
    match static_state::config_file_path() {
        Some(path) => {
            let document = std::fs::read_to_string(&path)
                .map_err(|error| format!("reading {path}: {error}"))?;
            Configuration::from_toml(&document).map_err(|error| error.to_string())
        }
        None => Ok(Configuration::default()),
    }
}

/// `DMA.INFO`: report the selected libfabric provider's attributes, opening the server endpoint if
/// needed. For diagnosing provider selection on a new fabric.
fn dma_info_command(_context: &Context, _args: Vec<ValkeyString>) -> ValkeyResult {
    let configuration = static_state::configuration();
    match session::endpoint_info(&configuration) {
        Ok(info) => Ok(ValkeyValue::Array(
            info.lines()
                .into_iter()
                .map(ValkeyValue::SimpleString)
                .collect(),
        )),
        Err(error) => Err(command_error(error)),
    }
}

/// `DMA.HELLO`: return the server's fabric address so the client can insert it into its address
/// vector, which efa-direct requires before any RMA. See [`crate::transfer::dma_hello`].
fn dma_hello_command(context: &Context, _args: Vec<ValkeyString>) -> ValkeyResult {
    transfer::dma_hello(context)
}

/// `DMA.SET`: block the client and DMA the payload into an off-keyspace buffer on the fabric worker,
/// committing it into the key once the transfer and CRC complete. See [`crate::transfer`].
fn dma_set_command(context: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let parsed = parse_data_args(&args)?;
    let expected_crc = parsed.crc.map(parse_crc).transpose()?;
    // The commit happens after this command returns and valkey frees its argv, so the key must
    // outlive the command. Retaining the existing string object is a reference-count bump, cheaper
    // than rebuilding it from bytes per command.
    let key = parsed.key.safe_clone(context);
    transfer::dma_set(
        context,
        &parsed.advertisement,
        key,
        parsed.length,
        expected_crc,
    )
}

/// `DMA.GET`: block the client and DMA the value out on the fabric worker, so the valkey thread
/// isn't held for the transfer. See [`crate::transfer`].
fn dma_get_command(context: &Context, args: Vec<ValkeyString>) -> ValkeyResult {
    let parsed = parse_data_args(&args)?;
    transfer::dma_get(
        context,
        &parsed.advertisement,
        parsed.key,
        parsed.length,
        parsed.crc.is_some(),
    )
}

/// The parsed fields of a `DMA.SET` or `DMA.GET`, whose wire layout is
/// `<command> <address> <rkey> <remote-address> <length> <key> [<crc>]`. The advertisement says
/// where the client's buffer is. The length is payload bytes for SET and buffer capacity for GET.
/// The trailing CRC is the checksum to verify on SET, and on GET asks the server to return one.
struct DataArgs<'a> {
    advertisement: Advertisement,
    length: usize,
    key: &'a ValkeyString,
    crc: Option<&'a [u8]>,
}

fn parse_data_args(args: &[ValkeyString]) -> Result<DataArgs<'_>, ValkeyError> {
    // `base` counts the args through the key, which is also where a trailing crc would sit.
    let base = 3 + Advertisement::ARG_COUNT;
    if args.len() != base && args.len() != base + 1 {
        return Err(ValkeyError::WrongArity);
    }
    let advertisement =
        Advertisement::from_fields(&[args[1].as_slice(), args[2].as_slice(), args[3].as_slice()])
            .map_err(command_error)?;
    Ok(DataArgs {
        advertisement,
        length: parse_length(args[base - 2].as_slice())?,
        key: &args[base - 1],
        crc: args.get(base).map(ValkeyString::as_slice),
    })
}

/// Parse a decimal byte length from a command argument.
fn parse_length(raw: &[u8]) -> Result<usize, ValkeyError> {
    dma_libfabric_protocol::parse_ascii(raw).ok_or(ValkeyError::Str(
        "ERR length is not an integer or out of range",
    ))
}

/// Parse a decimal CRC32 from a command argument.
fn parse_crc(raw: &[u8]) -> Result<u32, ValkeyError> {
    dma_libfabric_protocol::parse_ascii(raw).ok_or(ValkeyError::Str(
        "ERR crc is not an integer or out of range",
    ))
}
