//! Live Backblaze B2 check for the emulated create-if-absent.
//!
//! Skips unless the four BASIN_TEST_B2_* variables are set, the way the other
//! integration tests skip without DATABASE_URL:
//!
//! ```text
//! BASIN_TEST_B2_ENDPOINT=https://s3.us-east-005.backblazeb2.com \
//! BASIN_TEST_B2_BUCKET=copod-database \
//! BASIN_TEST_B2_KEY_ID=... BASIN_TEST_B2_SECRET=... \
//!   cargo test -p basin-catalog --test b2_emulated_create
//! ```

use std::sync::Arc;

use basin_catalog::conditional_put::EmulatedConditionalPut;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload};

fn emulated_store() -> Option<Arc<dyn ObjectStore>> {
    let endpoint = std::env::var("BASIN_TEST_B2_ENDPOINT").ok()?;
    let bucket = std::env::var("BASIN_TEST_B2_BUCKET").ok()?;
    let key = std::env::var("BASIN_TEST_B2_KEY_ID").ok()?;
    let secret = std::env::var("BASIN_TEST_B2_SECRET").ok()?;

    let inner: Arc<dyn ObjectStore> = Arc::new(
        AmazonS3Builder::new()
            .with_bucket_name(bucket)
            .with_endpoint(endpoint)
            .with_region("us-east-005")
            .with_access_key_id(key)
            .with_secret_access_key(secret)
            .build()
            .expect("build the S3 store"),
    );
    Some(Arc::new(EmulatedConditionalPut::new(inner)))
}

#[tokio::test]
async fn create_if_absent_survives_a_store_without_conditional_put() {
    let Some(store) = emulated_store() else {
        eprintln!("skipping: BASIN_TEST_B2_* is not set");
        return;
    };

    let key = Path::from(format!("b2-probe/{}.json", ulid::Ulid::new()));
    let create = || PutOptions {
        mode: PutMode::Create,
        ..Default::default()
    };

    store
        .put_opts(&key, PutPayload::from_static(b"{\"v\":1}"), create())
        .await
        .expect("first create");

    let err = store
        .put_opts(&key, PutPayload::from_static(b"{\"v\":2}"), create())
        .await
        .expect_err("a second create must conflict");
    assert!(
        matches!(err, object_store::Error::AlreadyExists { .. }),
        "got {err:?}"
    );

    // Plain overwrites must still pass through: the data plane uses them.
    store
        .put(&key, PutPayload::from_static(b"{\"v\":3}"))
        .await
        .expect("overwrite");
    let stored = store
        .get(&key)
        .await
        .expect("get")
        .bytes()
        .await
        .expect("bytes");
    assert_eq!(stored.as_ref(), b"{\"v\":3}".as_slice());

    store.delete(&key).await.expect("delete the probe");
}
