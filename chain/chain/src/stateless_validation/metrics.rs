use near_o11y::metrics::{
    HistogramVec, IntCounter, IntCounterVec, IntGauge, exponential_buckets, linear_buckets,
    try_create_histogram_vec, try_create_int_counter, try_create_int_counter_vec,
    try_create_int_gauge,
};
use near_primitives::stateless_validation::state_witness::ChunkStateWitness;
use std::sync::LazyLock;

pub static SAVE_LATEST_WITNESS_GENERATE_UPDATE_TIME: LazyLock<HistogramVec> = LazyLock::new(|| {
    try_create_histogram_vec(
        "near_save_latest_witness_generate_update_time",
        "Time taken to generate an update of latest witnesses",
        &["shard_id"],
        Some(exponential_buckets(0.001, 1.6, 20).unwrap()),
    )
    .unwrap()
});

pub static SAVE_LATEST_WITNESS_COMMIT_UPDATE_TIME: LazyLock<HistogramVec> = LazyLock::new(|| {
    try_create_histogram_vec(
        "near_save_latest_witness_commit_update_time",
        "Time taken to commit the update of latest witnesses",
        &["shard_id"],
        Some(exponential_buckets(0.001, 1.6, 20).unwrap()),
    )
    .unwrap()
});

pub static SAVED_LATEST_WITNESSES_COUNT: LazyLock<IntGauge> = LazyLock::new(|| {
    try_create_int_gauge(
        "near_saved_latest_witnesses_count",
        "Total number of saved latest witnesses",
    )
    .unwrap()
});

pub static SAVED_LATEST_WITNESSES_SIZE: LazyLock<IntGauge> = LazyLock::new(|| {
    try_create_int_gauge(
        "near_saved_latest_witnesses_size",
        "Total size of saved latest witnesses (in bytes)",
    )
    .unwrap()
});

pub static SAVE_INVALID_WITNESS_GENERATE_UPDATE_TIME: LazyLock<HistogramVec> =
    LazyLock::new(|| {
        try_create_histogram_vec(
            "near_save_invalid_witness_generate_update_time",
            "Time taken to generate an update of invalid witnesses",
            &["shard_id"],
            Some(exponential_buckets(0.001, 1.6, 20).unwrap()),
        )
        .unwrap()
    });

pub static SAVE_INVALID_WITNESS_COMMIT_UPDATE_TIME: LazyLock<HistogramVec> = LazyLock::new(|| {
    try_create_histogram_vec(
        "near_save_invalid_witness_commit_update_time",
        "Time taken to commit the update of invalid witnesses",
        &["shard_id"],
        Some(exponential_buckets(0.001, 1.6, 20).unwrap()),
    )
    .unwrap()
});

pub static SAVED_INVALID_WITNESSES_COUNT: LazyLock<IntGauge> = LazyLock::new(|| {
    try_create_int_gauge(
        "near_saved_invalid_witnesses_count",
        "Total number of saved invalid witnesses",
    )
    .unwrap()
});

pub static SAVED_INVALID_WITNESSES_SIZE: LazyLock<IntGauge> = LazyLock::new(|| {
    try_create_int_gauge(
        "near_saved_invalid_witnesses_size",
        "Total size of saved invalid witnesses (in bytes)",
    )
    .unwrap()
});

pub static CHUNK_STATE_WITNESS_ENCODE_TIME: LazyLock<HistogramVec> = LazyLock::new(|| {
    try_create_histogram_vec(
        "near_chunk_state_witness_encode_time",
        "State witness encoding (serialization + compression) latency in seconds",
        &["shard_id"],
        Some(linear_buckets(0.025, 0.025, 20).unwrap()),
    )
    .unwrap()
});

pub static PROCESS_CONTRACT_CODE_REQUEST_TIME: LazyLock<HistogramVec> = LazyLock::new(|| {
    try_create_histogram_vec(
        "near_process_contract_code_request_time",
        "Total time taken to process contract code request from a chunk validator",
        &["shard_id"],
        Some(exponential_buckets(0.001, 2.0, 10).unwrap()),
    )
    .unwrap()
});

pub static SHADOW_CHUNK_VALIDATION_FAILED_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    try_create_int_counter(
        "near_shadow_chunk_validation_failed_total",
        "Shadow chunk validation failures count",
    )
    .unwrap()
});

pub static CHUNK_WITNESS_VALIDATION_FAILED_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    try_create_int_counter_vec(
        "near_chunk_witness_validation_failed_total",
        "Witness validation failure count",
        &["shard_id", "error"],
    )
    .unwrap()
});

pub(crate) static CHUNK_STATE_WITNESS_VALIDATION_TIME: LazyLock<HistogramVec> =
    LazyLock::new(|| {
        try_create_histogram_vec(
            "near_chunk_state_witness_validation_time",
            "State witness validation latency in seconds",
            &["shard_id"],
            Some(exponential_buckets(0.01, 2.0, 12).unwrap()),
        )
        .unwrap()
    });

pub(crate) static CHUNK_STATE_WITNESS_TOTAL_SIZE: LazyLock<HistogramVec> = LazyLock::new(|| {
    try_create_histogram_vec(
        "near_chunk_state_witness_total_size",
        "Stateless validation compressed state witness size in bytes",
        &["shard_id"],
        Some(exponential_buckets(100_000.0, 1.2, 32).unwrap()),
    )
    .unwrap()
});

pub(crate) static CHUNK_STATE_WITNESS_RAW_SIZE: LazyLock<HistogramVec> = LazyLock::new(|| {
    try_create_histogram_vec(
        "near_chunk_state_witness_raw_size",
        "Stateless validation uncompressed (raw) state witness size in bytes",
        &["shard_id"],
        Some(exponential_buckets(100_000.0, 1.2, 32).unwrap()),
    )
    .unwrap()
});

pub static CHUNK_STATE_WITNESS_DECODE_TIME: LazyLock<HistogramVec> = LazyLock::new(|| {
    try_create_histogram_vec(
        "near_chunk_state_witness_decode_time",
        "State witness decoding (decompression + deserialization) latency in seconds",
        &["shard_id"],
        Some(linear_buckets(0.025, 0.025, 20).unwrap()),
    )
    .unwrap()
});

pub static VALIDATE_CHUNK_WITH_ENCODED_MERKLE_ROOT_TIME: LazyLock<HistogramVec> =
    LazyLock::new(|| {
        try_create_histogram_vec(
            "near_validate_chunk_with_encoded_merkle_root_time",
            "Time taken to validate a chunk with encoded merkle root",
            &["shard_id"],
            Some(
                // Buckets from 1ms to 500ms
                // 5ms = expected
                // 20ms = concerning but OK
                // 50ms = bad
                // >=100ms = very bad
                vec![0.001, 0.005, 0.01, 0.02, 0.05, 0.1, 0.5], // seconds
            ),
        )
        .unwrap()
    });

pub(crate) static CHUNK_STATE_WITNESS_MAIN_STATE_TRANSITION_SIZE: LazyLock<HistogramVec> =
    LazyLock::new(|| {
        try_create_histogram_vec(
            "near_chunk_state_witness_main_state_transition_size",
            "Size of ChunkStateWitness::main_state_transition (storage proof needed to execute receipts)",
            &["shard_id"],
            Some(buckets_for_witness_field_size()),
        )
            .unwrap()
    });

pub(crate) static CHUNK_STATE_WITNESS_SOURCE_RECEIPT_PROOFS_SIZE: LazyLock<HistogramVec> =
    LazyLock::new(|| {
        try_create_histogram_vec(
            "near_chunk_state_witness_source_receipt_proofs_size",
            "Size of ChunkStateWitness::source_receipt_proofs (incoming receipts proofs)",
            &["shard_id"],
            Some(buckets_for_witness_field_size()),
        )
        .unwrap()
    });

pub fn record_witness_size_metrics(
    decoded_size: usize,
    encoded_size: usize,
    witness: &ChunkStateWitness,
) {
    if let Err(err) = record_witness_size_metrics_fallible(decoded_size, encoded_size, witness) {
        tracing::warn!(target:"client", "Failed to record witness size metrics!, error: {}", err);
    }
}

fn record_witness_size_metrics_fallible(
    decoded_size: usize,
    encoded_size: usize,
    witness: &ChunkStateWitness,
) -> Result<(), std::io::Error> {
    let shard_id = witness.chunk_header().shard_id().to_string();
    CHUNK_STATE_WITNESS_RAW_SIZE
        .with_label_values(&[shard_id.as_str()])
        .observe(decoded_size as f64);
    CHUNK_STATE_WITNESS_TOTAL_SIZE
        .with_label_values(&[&shard_id.as_str()])
        .observe(encoded_size as f64);
    CHUNK_STATE_WITNESS_MAIN_STATE_TRANSITION_SIZE
        .with_label_values(&[shard_id.as_str()])
        .observe(borsh::object_length(&witness.main_state_transition())? as f64);
    CHUNK_STATE_WITNESS_SOURCE_RECEIPT_PROOFS_SIZE
        .with_label_values(&[&shard_id.as_str()])
        .observe(borsh::object_length(&witness.source_receipt_proofs())? as f64);
    Ok(())
}

/// Buckets from 0 to 10MB
/// Meant for measuring size of a single field inside ChunkSizeWitness.
fn buckets_for_witness_field_size() -> Vec<f64> {
    vec![
        10_000.,
        20_000.,
        50_000.,
        100_000.,
        200_000.,
        300_000.,
        500_000.,
        750_000.,
        1000_000.,
        1500_000.,
        2000_000.,
        2500_000.,
        3000_000.,
        3500_000.,
        4000_000.,
        4500_000.,
        5000_000.,
        6000_000.,
        7000_000.,
        8000_000.,
        9000_000.,
        10_000_000.,
    ]
}
