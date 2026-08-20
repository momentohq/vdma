//! Argument parsing the initiator examples share: which fabric to open, and the peer advertisement
//! to RMA against.
//!
//! ```text
//! <bind> [<address-hex> <rkey> <remote-addr>]     # tcp
//! --efa [<address-hex> <rkey> <remote-addr>]      # efa-direct, no bind
//! ```
//!
//! `--efa` takes no bind address: an EFA endpoint binds to the device and advertises its own fabric
//! address rather than an IP, so pinning a source is a tcp-only convenience. Without the three
//! advertisement fields an example prints its own address and waits for the target's on stdin — see
//! [`peer_from_stdin`].

use dma_libfabric::{Configuration, Provider};
use dma_libfabric_protocol::{Advertisement, DmaError};

pub struct Arguments {
    pub configuration: Configuration,
    /// `None` when the advertisement fields were not given.
    pub peer: Option<Advertisement>,
}

/// Read the three advertisement fields from a line of stdin. This is the efa-direct ordering: a
/// target must hold the initiator's address in its own address vector before it can be RMA'd
/// against, and an endpoint's address is new every run — so the initiator comes up first, prints the
/// address the target needs, and blocks here until the target prints its advertisement back.
pub fn peer_from_stdin() -> Result<Advertisement, DmaError> {
    let mut line = String::new();
    std::io::stdin()
        .read_line(&mut line)
        .map_err(|error| DmaError::Fabric(format!("reading the advertisement: {error}")))?;
    let fields: Vec<&str> = line.split_whitespace().collect();
    match advertisement(&fields) {
        Some(peer) => peer,
        None => Err(DmaError::Fabric(format!(
            "expected <address-hex> <rkey> <remote-addr>, got {} fields",
            fields.len()
        ))),
    }
}

/// The three advertisement fields, or `None` when there aren't exactly three of them.
fn advertisement(fields: &[&str]) -> Option<Result<Advertisement, DmaError>> {
    let [address, remote_key, remote_address] = fields else {
        return None;
    };
    Some(
        Advertisement::from_fields(&[
            address.as_bytes(),
            remote_key.as_bytes(),
            remote_address.as_bytes(),
        ])
        .map_err(|error| DmaError::Fabric(error.to_string())),
    )
}

pub fn parse() -> Result<Arguments, DmaError> {
    let (flags, positional): (Vec<String>, Vec<String>) = std::env::args()
        .skip(1)
        .partition(|argument| argument.starts_with("--"));
    let efa = flags.iter().any(|flag| "--efa" == flag);
    let mut positional = positional.into_iter();
    let configuration = Configuration {
        providers: vec![if efa {
            Provider::EfaDirect
        } else {
            Provider::Tcp
        }],
        bind: if efa { None } else { positional.next() },
        ..Configuration::default()
    };

    let fields: Vec<String> = positional.collect();
    let fields: Vec<&str> = fields.iter().map(String::as_str).collect();
    let peer = advertisement(&fields).transpose()?;
    Ok(Arguments {
        configuration,
        peer,
    })
}
