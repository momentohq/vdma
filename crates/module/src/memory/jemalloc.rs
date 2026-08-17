//! valkey's jemalloc, whose symbols carry the `je_` prefix.
//!
//! Resolved with `dlsym` rather than linked, so the module still loads against another allocator —
//! a macos dev build, say — and simply reports jemalloc as absent.

use std::os::raw::{c_char, c_int, c_void};
use std::sync::OnceLock;

unsafe extern "C" {
    fn dlsym(handle: *mut c_void, symbol: *const c_char) -> *mut c_void;
}

#[cfg(target_os = "macos")]
const RTLD_DEFAULT: *mut c_void = -2isize as *mut c_void;
#[cfg(not(target_os = "macos"))]
const RTLD_DEFAULT: *mut c_void = std::ptr::null_mut();

type MallctlFn =
    unsafe extern "C" fn(*const c_char, *mut c_void, *mut usize, *mut c_void, usize) -> c_int;
type MallocFn = unsafe extern "C" fn(usize) -> *mut c_void;
type FreeFn = unsafe extern "C" fn(*mut c_void);

struct Jemalloc {
    mallctl: MallctlFn,
    malloc: MallocFn,
    free: FreeFn,
}

static JEMALLOC: OnceLock<Option<Jemalloc>> = OnceLock::new();

fn jemalloc() -> Option<&'static Jemalloc> {
    JEMALLOC
        .get_or_init(|| {
            // SAFETY: each symbol is looked up by its own name and transmuted to that function's
            // signature, taken from jemalloc's public header.
            unsafe {
                let mallctl = dlsym(RTLD_DEFAULT, c"je_mallctl".as_ptr());
                let malloc = dlsym(RTLD_DEFAULT, c"je_malloc".as_ptr());
                let free = dlsym(RTLD_DEFAULT, c"je_free".as_ptr());
                if mallctl.is_null() || malloc.is_null() || free.is_null() {
                    return None;
                }
                Some(Jemalloc {
                    mallctl: std::mem::transmute::<*mut c_void, MallctlFn>(mallctl),
                    malloc: std::mem::transmute::<*mut c_void, MallocFn>(malloc),
                    free: std::mem::transmute::<*mut c_void, FreeFn>(free),
                })
            }
        })
        .as_ref()
}

/// `je_mallctl`, or a non-zero code when the host has no jemalloc.
fn mallctl(
    name: *const c_char,
    oldp: *mut c_void,
    oldlenp: *mut usize,
    newp: *mut c_void,
    newlen: usize,
) -> c_int {
    match jemalloc() {
        Some(jemalloc) => unsafe { (jemalloc.mallctl)(name, oldp, oldlenp, newp, newlen) },
        None => -1,
    }
}

/// A `je_malloc` allocation, freed with `je_free` so it lands in jemalloc's arenas rather than the
/// host's default allocator. Used to force an arena into existence.
pub(crate) struct Allocation(*mut c_void);

impl Allocation {
    /// `None` without jemalloc, or when the allocation fails.
    pub(crate) fn new(size: usize) -> Option<Self> {
        let jemalloc = jemalloc()?;
        let pointer = unsafe { (jemalloc.malloc)(size) };
        (!pointer.is_null()).then_some(Self(pointer))
    }

    pub(crate) fn pointer(&self) -> *mut c_void {
        self.0
    }
}

impl Drop for Allocation {
    fn drop(&mut self) {
        // SAFETY: jemalloc allocated it, so jemalloc frees it; dropped once.
        if let Some(jemalloc) = jemalloc() {
            unsafe { (jemalloc.free)(self.0) };
        }
    }
}

/// The arena index owning `pointer`, via `arenas.lookup`: write the pointer, read the index.
pub(crate) fn arena_of(pointer: *mut c_void) -> Option<u32> {
    let mut arena_ind: u32 = 0;
    let mut out_size = std::mem::size_of::<u32>();
    let mut lookup = pointer;
    let code = mallctl(
        c"arenas.lookup".as_ptr(),
        (&mut arena_ind as *mut u32).cast::<c_void>(),
        &mut out_size,
        (&mut lookup as *mut *mut c_void).cast::<c_void>(),
        std::mem::size_of::<*mut c_void>(),
    );
    (0 == code).then_some(arena_ind)
}

/// Write a `mallctl` value of a fixed-size type: `isize` for the decay windows, `usize` for the
/// oversize threshold, a pointer for the extent-hook table.
pub(crate) fn write<T>(name: &std::ffi::CStr, mut value: T) -> bool {
    let code = mallctl(
        name.as_ptr(),
        std::ptr::null_mut(),
        std::ptr::null_mut(),
        (&mut value as *mut T).cast::<c_void>(),
        std::mem::size_of::<T>(),
    );
    0 == code
}

/// Read a `mallctl` value of a fixed-size type.
pub(crate) fn read<T>(name: &std::ffi::CStr) -> Option<T> {
    let mut value = std::mem::MaybeUninit::<T>::uninit();
    let mut size = std::mem::size_of::<T>();
    let code = mallctl(
        name.as_ptr(),
        value.as_mut_ptr().cast::<c_void>(),
        &mut size,
        std::ptr::null_mut(),
        0,
    );
    // SAFETY: jemalloc wrote `size_of::<T>()` bytes into it on success.
    (0 == code && std::mem::size_of::<T>() == size).then(|| unsafe { value.assume_init() })
}

/// The `arena.<index>.<field>` key the arena ctls are named by.
pub(crate) fn arena_key(index: u32, field: &str) -> std::ffi::CString {
    std::ffi::CString::new(format!("arena.{index}.{field}")).unwrap_or_default()
}
