
use metrics::{counter, gauge, histogram};

pub fn record_layer_wired(wired: bool) {
    gauge!("dwctl_continuation_layer_wired").set(if wired { 1.0 } else { 0.0 });
}

pub fn record_outcome(outcome: &'static str, reason: &'static str, origin: &'static str) {
    counter!("dwctl_continuation_outcome_total", "outcome" => outcome, "reason" => reason, "origin" => origin).increment(1);
}

pub fn record_eligible_stream(model: &str, origin: &'static str) {
    counter!("dwctl_continuation_eligible_streams_total", "model" => model.to_owned(), "origin" => origin).increment(1);
}

pub fn record_resume_leg(model: &str, attempt: u32) {
    counter!(
        "dwctl_continuation_resume_legs_total",
        "model" => model.to_owned(),
        "attempt" => attempt_label(attempt)
    )
    .increment(1);
}

fn attempt_label(attempt: u32) -> &'static str {
    match attempt {
        1 => "1",
        2 => "2",
        3 => "3",
        _ => "4+",
    }
}

pub fn record_seam(model: &str, provider: &'static str, seconds: f64) {
    histogram!("dwctl_continuation_seam_seconds", "model" => model.to_owned(), "provider" => provider).record(seconds);
}

pub fn record_leg_served(model: &str, provider: &'static str, prompt_tokens: u64) {
    counter!("dwctl_continuation_legs_served_total", "model" => model.to_owned(), "provider" => provider).increment(1);
    if prompt_tokens > 0 {
        counter!("dwctl_continuation_leg_served_tokens_total", "model" => model.to_owned(), "provider" => provider)
            .increment(prompt_tokens);
    }
}

pub fn record_eaten_prompt_tokens(model: &str, tokens: u64) {
    if tokens > 0 {
        counter!("dwctl_continuation_eaten_prompt_tokens_total", "model" => model.to_owned()).increment(tokens);
    }
}

pub fn record_usage_anomaly(kind: &'static str) {
    counter!("dwctl_continuation_usage_anomaly_total", "kind" => kind).increment(1);
}

pub fn record_max_frame_gap(model: &str, seconds: f64) {
    histogram!("dwctl_continuation_max_frame_gap_seconds", "model" => model.to_owned()).record(seconds);
}

pub fn record_render(outcome: &'static str) {
    counter!("dwctl_continuation_render_total", "outcome" => outcome).increment(1);
}

pub fn record_render_duration(seconds: f64) {
    histogram!("dwctl_continuation_render_duration_seconds").record(seconds);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn attempt_labels_are_bounded() {
        assert_eq!(attempt_label(1), "1");
        assert_eq!(attempt_label(3), "3");
        assert_eq!(attempt_label(4), "4+");
        assert_eq!(attempt_label(9_999), "4+");
    }
}
