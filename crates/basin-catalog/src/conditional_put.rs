//! Emulated create-if-absent for object stores without conditional PUTs.
//!
//! The bucket-native catalog and lease registry commit by appending a
//! monotonically versioned object with [`PutMode::Create`], which the AWS SDK
//! turns into `If-None-Match: *`. Backblaze B2 answers `501 Not Implemented` to
//! that header, so the first DDL statement on a B2 bucket fails before anything
//! is written, even though plain PUT, checksum and ACL headers all work.
//!
//! [`EmulatedConditionalPut`] wraps such a store and turns create-if-absent into
//! a HEAD, an unconditional PUT, and a read-back ETag check:
//!
//! - HEAD returns `AlreadyExists` when the object is there, which is exactly
//!   what the catalog commit loop matches on.
//! - The unconditional PUT carries the commit.
//! - Reading the ETag back catches the narrow race where another writer
//!   replaced the object between the HEAD and the PUT, and reports it as
//!   `AlreadyExists` so the caller reloads and retries.
//!
//! The check is not atomic. It only has to make a lost update visible instead
//! of silent, and that is enough for the versioned-log protocol: losers retry,
//! winners keep their version. A single writer per project, which is the normal
//! deployment shape, never races at all.

use std::fmt;
use std::sync::Arc;

use futures::stream::BoxStream;
use object_store::path::Path as OsPath;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};

/// Selects how create-if-absent is satisfied.
pub const CONDITIONAL_PUT_ENV: &str = "BASIN_STORAGE_CONDITIONAL_PUT";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConditionalPut {
    /// The store implements `If-None-Match` on PUT: Tigris, Cloudflare R2,
    /// AWS S3. This is the default.
    Required,
    /// Emulate it with HEAD + PUT + ETag read-back: Backblaze B2 and other
    /// stores that answer 501 to the precondition header.
    Emulated,
}

fn parse_mode(raw: &str) -> Result<ConditionalPut, String> {
    let value = raw.trim().to_ascii_lowercase();
    match value.as_str() {
        "" | "require" | "required" | "on" | "1" | "true" => Ok(ConditionalPut::Required),
        "emulate" | "emulated" | "off" | "0" | "false" | "disabled" => Ok(ConditionalPut::Emulated),
        other => Err(format!(
            "{CONDITIONAL_PUT_ENV} must be 'require' or 'emulate', got {other:?}"
        )),
    }
}

/// Read [`CONDITIONAL_PUT_ENV`]. Unknown values are an error: this setting
/// decides whether a safety property is enforced by the store, so a typo must
/// stop startup instead of silently changing behaviour.
pub fn conditional_put_mode() -> Result<ConditionalPut, String> {
    match std::env::var(CONDITIONAL_PUT_ENV) {
        Ok(raw) => parse_mode(&raw),
        Err(_) => Ok(ConditionalPut::Required),
    }
}

/// Wrap a raw bucket handle for catalog use, emulating create-if-absent when
/// the environment asks for it.
pub fn catalog_store(
    store: Arc<dyn ObjectStore>,
) -> Result<(Arc<dyn ObjectStore>, ConditionalPut), String> {
    let mode = conditional_put_mode()?;
    let store: Arc<dyn ObjectStore> = match mode {
        ConditionalPut::Required => store,
        ConditionalPut::Emulated => Arc::new(EmulatedConditionalPut::new(store)),
    };
    Ok((store, mode))
}

/// Store wrapper that emulates `PutMode::Create` where the backend has no
/// conditional PUT.
pub struct EmulatedConditionalPut {
    inner: Arc<dyn ObjectStore>,
}

impl EmulatedConditionalPut {
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self { inner }
    }
}

impl fmt::Debug for EmulatedConditionalPut {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "EmulatedConditionalPut({})", self.inner)
    }
}

impl fmt::Display for EmulatedConditionalPut {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "EmulatedConditionalPut({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for EmulatedConditionalPut {
    async fn put_opts(
        &self,
        location: &OsPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        if !matches!(opts.mode, object_store::PutMode::Create) {
            return self.inner.put_opts(location, payload, opts).await;
        }

        // Create means "fail if present", so probe first: without conditional
        // headers this is the only signal the store can give us.
        match self.inner.head(location).await {
            Ok(_) => {
                return Err(object_store::Error::AlreadyExists {
                    path: location.to_string(),
                    source: "object already exists (emulated create-if-absent)".into(),
                });
            }
            Err(object_store::Error::NotFound { .. }) => {}
            Err(err) => return Err(err),
        }

        let result = self
            .inner
            .put_opts(
                location,
                payload,
                PutOptions {
                    mode: object_store::PutMode::Overwrite,
                    ..opts
                },
            )
            .await?;

        // Make the narrow probe-then-write race visible: if another writer
        // landed after us, the stored ETag is no longer the one we got back.
        if let Some(our_etag) = result.e_tag.as_deref() {
            let stored = self.inner.head(location).await?;
            if stored.e_tag.as_deref() != Some(our_etag) {
                return Err(object_store::Error::AlreadyExists {
                    path: location.to_string(),
                    source: "concurrent writer replaced the object (emulated create-if-absent)"
                        .into(),
                });
            }
        }
        Ok(result)
    }

    async fn put_multipart_opts(
        &self,
        location: &OsPath,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &OsPath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<OsPath>>,
    ) -> BoxStream<'static, object_store::Result<OsPath>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&OsPath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&OsPath>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &OsPath,
        to: &OsPath,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use object_store::memory::InMemory;
    use object_store::PutMode;

    fn store() -> EmulatedConditionalPut {
        EmulatedConditionalPut::new(Arc::new(InMemory::new()))
    }

    fn create() -> PutOptions {
        PutOptions {
            mode: PutMode::Create,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn create_writes_once_and_rejects_the_second_write() {
        let store = store();
        let path = OsPath::from("catalog/v0.json");
        store
            .put_opts(&path, PutPayload::from_static(b"{}"), create())
            .await
            .expect("first create");
        let err = store
            .put_opts(&path, PutPayload::from_static(b"{}"), create())
            .await
            .expect_err("second create must fail");
        assert!(
            matches!(err, object_store::Error::AlreadyExists { .. }),
            "got {err:?}"
        );
    }

    #[tokio::test]
    async fn create_on_a_missing_object_succeeds() {
        let store = store();
        let path = OsPath::from("leases/p0/e1.json");
        store
            .put_opts(&path, PutPayload::from_static(b"{}"), create())
            .await
            .expect("create");
        assert!(store.head(&path).await.is_ok());
    }

    #[tokio::test]
    async fn overwrite_passes_through() {
        let store = store();
        let path = OsPath::from("catalog/v0.json");
        store
            .put(&path, PutPayload::from_static(b"a"))
            .await
            .expect("put");
        store
            .put(&path, PutPayload::from_static(b"b"))
            .await
            .expect("overwrite");
        let got = store
            .get(&path)
            .await
            .expect("get")
            .bytes()
            .await
            .expect("bytes");
        assert_eq!(got.as_ref(), Bytes::from_static(b"b").as_ref());
    }

    #[test]
    fn the_mode_parser_is_strict() {
        assert_eq!(parse_mode(""), Ok(ConditionalPut::Required));
        assert_eq!(parse_mode("require"), Ok(ConditionalPut::Required));
        assert_eq!(parse_mode("EMULATE"), Ok(ConditionalPut::Emulated));
        assert_eq!(parse_mode("off"), Ok(ConditionalPut::Emulated));
        assert!(parse_mode("maybe").is_err());
    }
}
