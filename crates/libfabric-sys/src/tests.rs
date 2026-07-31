//! Runtime smoke tests: prove the bindings link and libfabric actually responds.

use std::ffi::CStr;
use std::ptr;

use super::{fi_freeinfo, fi_getinfo, fi_info, fi_version};

#[test]
fn reports_a_version() {
    // `fi_version` is a real exported symbol, so a non-zero result proves libfabric linked.
    let version = unsafe { fi_version() };
    assert!(version > 0, "fi_version returned 0");
}

#[test]
fn discovers_the_tcp_provider() {
    unsafe {
        let mut info: *mut fi_info = ptr::null_mut();
        let result = fi_getinfo(
            fi_version(),
            ptr::null(),
            ptr::null(),
            0,
            ptr::null(),
            &mut info,
        );
        assert_eq!(result, 0, "fi_getinfo failed: {result}");
        assert!(!info.is_null(), "fi_getinfo returned no providers");

        let mut found_tcp = false;
        let mut node = info;
        while !node.is_null() {
            let fabric_attr = (*node).fabric_attr;
            if !fabric_attr.is_null() && !(*fabric_attr).prov_name.is_null() {
                let name = CStr::from_ptr((*fabric_attr).prov_name).to_string_lossy();
                if name.contains("tcp") {
                    found_tcp = true;
                    break;
                }
            }
            node = (*node).next;
        }

        fi_freeinfo(info);
        assert!(found_tcp, "tcp provider not discovered by libfabric");
    }
}
