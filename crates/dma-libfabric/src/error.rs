//! Mapping libfabric return codes to [`DmaError`].

use std::ffi::CStr;
use std::os::raw::c_char;

use crate::sys::{fi_cq_err_entry, fi_cq_strerror, fi_strerror, fid_cq};
use dma_libfabric_protocol::DmaError;

/// Turn a libfabric return code, 0 for ok and negative for `-errno`, into a `Result`.
pub(crate) fn check(code: i32, what: &'static str) -> Result<(), DmaError> {
    if 0 == code {
        Ok(())
    } else {
        Err(fabric_error(code, what))
    }
}

/// Build a [`DmaError::Fabric`] from a libfabric error code.
pub(crate) fn fabric_error(code: i32, what: &'static str) -> DmaError {
    // libfabric returns negative errno values, and fi_strerror wants the positive one.
    let message = unsafe { CStr::from_ptr(fi_strerror(-code)) }.to_string_lossy();
    DmaError::Fabric(format!("{what}: {message} ({code})"))
}

/// Build a [`DmaError::Transfer`] from a completion-queue error entry, decoding both the libfabric
/// `err` via `fi_strerror` and the provider-specific `prov_errno` via `fi_cq_strerror` — EFA's
/// hardware completion-status strings name the actual failure, such as a bad address or
/// unresponsive remote, instead of an opaque number. Shared by every completion-error path.
pub(crate) fn completion_error(completion_queue: *mut fid_cq, entry: &fi_cq_err_entry) -> DmaError {
    // `cq_err_entry.err` is a positive errno, unlike return codes, so fi_strerror takes it as-is.
    let fabric = unsafe { CStr::from_ptr(fi_strerror(entry.err)) }.to_string_lossy();
    let mut buffer = [0 as c_char; 256];
    // fi_cq_strerror may return a static string or write into `buffer`, so use its return.
    let provider = unsafe {
        let decoded = fi_cq_strerror(
            completion_queue,
            entry.prov_errno,
            entry.err_data,
            buffer.as_mut_ptr(),
            buffer.len(),
        );
        CStr::from_ptr(decoded).to_string_lossy().into_owned()
    };
    DmaError::Transfer(format!(
        "completion error: {fabric} (err={}); provider: {provider} (prov_errno={})",
        entry.err, entry.prov_errno
    ))
}
