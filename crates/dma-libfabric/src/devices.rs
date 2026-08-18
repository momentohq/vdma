//! Runtime discovery of the local fabric domains, so a caller can open one endpoint per device and
//! spread load across every card an instance exposes. An EFA instance has 2 to 16 or more EFA
//! devices, each a distinct libfabric domain.

use std::ffi::CStr;

use crate::sys::fi_freeinfo;
use dma_libfabric_protocol::DmaError;

use crate::configuration::Configuration;
use crate::endpoint::query_info;

/// Every fabric domain the configured provider exposes, deduplicated, one per card, in the order
/// libfabric returns them. Pass a name back as `Configuration.interfaces[0]` to pin an endpoint to
/// that device.
pub fn discover_domains(configuration: &Configuration) -> Result<Vec<String>, DmaError> {
    let list = query_info(configuration, None)?;
    let mut names: Vec<String> = Vec::new();
    // SAFETY: a valid fi_info list from `fi_getinfo`, only read, then freed once.
    unsafe {
        let mut node = list;
        while !node.is_null() {
            let current = node;
            node = (*current).next;
            let name = (*(*current).domain_attr).name;
            if name.is_null() {
                continue;
            }
            let Ok(name) = CStr::from_ptr(name).to_str() else {
                continue;
            };
            if !names.iter().any(|existing| existing == name) {
                names.push(name.to_string());
            }
        }
        fi_freeinfo(list);
    }
    Ok(names)
}
