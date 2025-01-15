#[cfg(feature = "file_watcher")]
mod file_watcher;

#[cfg(feature = "multi_threaded")]
mod file_asset;
#[cfg(not(feature = "multi_threaded"))]
mod sync_file_asset;

use async_lock::Semaphore;
#[cfg(feature = "file_watcher")]
pub use file_watcher::*;
use tracing::{debug, error, info};

use std::{
    env,
    path::{Path, PathBuf},
};

pub(crate) fn get_base_path() -> PathBuf {
    if let Ok(manifest_dir) = env::var("BEVY_ASSET_ROOT") {
        PathBuf::from(manifest_dir)
    } else if let Ok(manifest_dir) = env::var("CARGO_MANIFEST_DIR") {
        PathBuf::from(manifest_dir)
    } else {
        env::current_exe()
            .map(|path| path.parent().map(ToOwned::to_owned).unwrap())
            .unwrap()
    }
}

/// I/O implementation for the local filesystem.
///
/// This asset I/O is fully featured but it's not available on `android` and `wasm` targets.
pub struct FileAssetReader {
    root_path: PathBuf,

    ///Used to ensure the `asset_server` does not try to acquire more loaders (and thus `file_handles`) than the OS allows
    descriptor_counter: Semaphore,
}

impl FileAssetReader {
    /// Creates a new `FileAssetIo` at a path relative to the executable's directory, optionally
    /// watching for changes.
    ///
    /// See `get_base_path` below.
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        let root_path = Self::get_base_path().join(path.as_ref());

        //Normal limits are cut in half to allow for .meta files and sub 1 for headroom
        #[cfg(target_os = "ios")]
        /*
        https://forum.vizrt.com/index.php?threads/ios-too-many-open-files-with-little-number-of-sources-receivers.250906/#:~:text=The%20number%20of%20sockets%20quickly,iOS%20and%20crashes%20the%20application.
        Documentation is fairly scarce on the actual limit, there is no documentation that I've been able to find from apple
        */
        const FILE_LIMIT: usize = 256; // The normal limit is 256, cut in half for .meta files and sub 1 because 128 still throws the occasional error (3 failed files out of 1500)

        /*
        https://krypted.com/mac-os-x/maximum-files-in-mac-os-x/
        Running `ulimit -n` on a MBP M3-Max yields 2560. In empirical testing when using the exact limit
        some failures would still squeak through. This also leaves a small amount of headroom for direct
        std::fs calls by the client application
        */
        #[cfg(target_os = "macos")]
        const FILE_LIMIT: usize = 2559;

        /*
        https://docs.pingidentity.com/pingdirectory/latest/installing_the_pingdirectory_suite_of_products/pd_ds_config_file_descriptor_limits.html#:~:text=Many%20Linux%20distributions%20have%20a,large%20number%20of%20concurrent%20connections.
        Setting this as a 'sensible' default in lieu of a cross platform way to determine file descriptor limits. For OSX/Linux we could potentially run ulimit at runtime, but client applications could also chunk their calls to asset_server
        as a workaround. Apps that exceed this limit would be fairly exceptional.
        */
        #[cfg(all(not(target_os = "macos"), not(target_os = "ios")))]
        let FILE_LIMIT: usize = 1024;

        info!("FILE_LIMIT: {}", FILE_LIMIT);
        debug!(
            "Asset Server using {} as its base path.",
            root_path.display()
        );
        Self { root_path, descriptor_counter: Semaphore::new(FILE_LIMIT) }
    }

    /// Returns the base path of the assets directory, which is normally the executable's parent
    /// directory.
    ///
    /// To change this, set [`AssetPlugin.file_path`].
    pub fn get_base_path() -> PathBuf {
        get_base_path()
    }

    /// Returns the root directory where assets are loaded from.
    ///
    /// See `get_base_path`.
    pub fn root_path(&self) -> &PathBuf {
        &self.root_path
    }
}

pub struct FileAssetWriter {
    root_path: PathBuf,
}

impl FileAssetWriter {
    /// Creates a new `FileAssetIo` at a path relative to the executable's directory, optionally
    /// watching for changes.
    ///
    /// See `get_base_path` below.
    pub fn new<P: AsRef<Path> + core::fmt::Debug>(path: P, create_root: bool) -> Self {
        let root_path = get_base_path().join(path.as_ref());
        if create_root {
            if let Err(e) = std::fs::create_dir_all(&root_path) {
                error!(
                    "Failed to create root directory {} for file asset writer: {}",
                    root_path.display(),
                    e
                );
            }
        }
        Self { root_path }
    }
}
