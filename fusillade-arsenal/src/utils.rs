pub const DEFAULT_AVG_CUSTOM_ID_LENGTH: usize = 20;

pub const DEFAULT_AVG_STATUS_CODE: i16 = 200;

pub fn calculate_response_body_size(response_body: &str) -> Option<i64> {
    i64::try_from(response_body.len()).ok()
}

pub fn calculate_error_message_size(error_json: &str) -> Option<i64> {
    i64::try_from(error_json.len()).ok()
}

pub fn calculate_jsonl_output_overhead_per_line(
    custom_id: &Option<String>,
    response_status: i16,
) -> i64 {
    let mut overhead = 7 +
        10 +
        36 +
        2;

    overhead += match custom_id {
        Some(id) => {
            14 + id.len() + 1
        }
        None => {
            16
        }
    };

    overhead += 13 +
        14 +
        response_status.to_string().len() +
        8 +
        3 +
        50;

    overhead as i64
}

pub fn calculate_jsonl_error_overhead_per_line(custom_id: &Option<String>) -> i64 {
    let mut overhead = 7 +
        10 +
        36 +
        2;

    overhead += match custom_id {
        Some(id) => {
            13 + id.len() + 1
        }
        None => {
            16
        }
    };

    overhead += 10 +
        21 +
        16 +
        4 +
        20;

    overhead as i64
}

pub fn estimate_output_file_size(
    raw_body_size_sum: i64,
    request_count: i64,
    avg_custom_id_len: Option<usize>,
) -> Option<i64> {
    let avg_custom_id_len = avg_custom_id_len.unwrap_or(DEFAULT_AVG_CUSTOM_ID_LENGTH);
    let avg_status_code = DEFAULT_AVG_STATUS_CODE;

    let avg_overhead = calculate_jsonl_output_overhead_per_line(
        &Some("x".repeat(avg_custom_id_len)),
        avg_status_code,
    );

    avg_overhead
        .checked_mul(request_count)
        .and_then(|overhead_total| raw_body_size_sum.checked_add(overhead_total))
}

pub fn estimate_error_file_size(
    raw_error_size_sum: i64,
    request_count: i64,
    avg_custom_id_len: Option<usize>,
) -> Option<i64> {
    let avg_custom_id_len = avg_custom_id_len.unwrap_or(DEFAULT_AVG_CUSTOM_ID_LENGTH);

    let avg_overhead =
        calculate_jsonl_error_overhead_per_line(&Some("x".repeat(avg_custom_id_len)));

    avg_overhead
        .checked_mul(request_count)
        .and_then(|overhead_total| raw_error_size_sum.checked_add(overhead_total))
}
