mod metadata_store;

pub use metadata_store::NatsMetadataStore;

pub fn sanitize_nats_key(key: &str) -> String {
    key.replace('#', ".")
        .replace(':', ".")
        .replace('/', ".")
        .replace('=', "_")
}

pub fn sanitize_nats_bucket(bucket: &str) -> String {
    sanitize_nats_key(bucket).replace('.', "-")
}
