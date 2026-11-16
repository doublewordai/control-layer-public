
use serde_json::Value;

use crate::request_logging::serializers::CacheTokens;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CreationTier {
    #[default]
    FiveMinute,
    OneHour,
    TwentyFourHour,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct CacheReading {
    pub tokens: CacheTokens,
    pub tier_inferred: bool,
}

pub fn cache_tokens_both_shapes(usage: &Value, flat_tier: CreationTier) -> CacheReading {
    let read = usage.get("cache_read_input_tokens").and_then(Value::as_i64).unwrap_or(0).max(0);

    let nested = |k: &str| {
        usage
            .get("cache_creation")
            .and_then(|c| c.get(k))
            .and_then(Value::as_i64)
            .unwrap_or(0)
            .max(0)
    };
    let (n5, n1, n24) = (
        nested("ephemeral_5m_input_tokens"),
        nested("ephemeral_1h_input_tokens"),
        nested("ephemeral_24h_input_tokens"),
    );

    if n5 + n1 + n24 > 0 {
        return CacheReading {
            tokens: CacheTokens {
                read,
                creation_5m: n5,
                creation_1h: n1,
                creation_24h: n24,
            },
            tier_inferred: false,
        };
    }

    let flat = usage.get("cache_creation_input_tokens").and_then(Value::as_i64).unwrap_or(0).max(0);

    if flat == 0 {
        return CacheReading {
            tokens: CacheTokens {
                read,
                ..CacheTokens::default()
            },
            tier_inferred: false,
        };
    }

    let mut tokens = CacheTokens {
        read,
        ..CacheTokens::default()
    };
    match flat_tier {
        CreationTier::FiveMinute => tokens.creation_5m = flat,
        CreationTier::OneHour => tokens.creation_1h = flat,
        CreationTier::TwentyFourHour => tokens.creation_24h = flat,
    }
    CacheReading {
        tokens,
        tier_inferred: true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn flat_creation_is_recovered_and_marked_inferred() {
        let usage = json!({
            "input_tokens": 565,
            "output_tokens": 658,
            "cache_read_input_tokens": 17631,
            "cache_creation_input_tokens": 538
        });
        let got = cache_tokens_both_shapes(&usage, CreationTier::FiveMinute);
        assert_eq!(got.tokens.read, 17_631);
        assert_eq!(got.tokens.creation_5m, 538, "flat total recovered");
        assert_eq!(got.tokens.creation_1h, 0);
        assert!(got.tier_inferred, "tier was assigned by rule, not read");
    }

    #[test]
    fn nested_wins_over_flat_and_is_not_inferred() {
        let usage = json!({
            "cache_read_input_tokens": 12000,
            "cache_creation_input_tokens": 9999,
            "cache_creation": { "ephemeral_1h_input_tokens": 3000 }
        });
        let got = cache_tokens_both_shapes(&usage, CreationTier::FiveMinute);
        assert_eq!(got.tokens.creation_1h, 3_000);
        assert_eq!(got.tokens.creation_5m, 0);
        assert!(!got.tier_inferred, "the body stated the tier");
    }

    #[test]
    fn no_creation_at_all_is_not_flagged_as_inferred() {
        let usage = json!({ "cache_read_input_tokens": 12000 });
        let got = cache_tokens_both_shapes(&usage, CreationTier::FiveMinute);
        assert_eq!(got.tokens.read, 12_000);
        assert_eq!(got.tokens.creation_5m, 0);
        assert!(!got.tier_inferred, "nothing was inferred - there were no writes");
    }

    #[test]
    fn flat_total_honours_the_requested_tier() {
        let usage = json!({ "cache_creation_input_tokens": 500 });
        let bucket_5m: fn(CacheTokens) -> i64 = |c| c.creation_5m;
        for (tier, get) in [
            (CreationTier::FiveMinute, bucket_5m),
            (CreationTier::OneHour, |c: CacheTokens| c.creation_1h),
            (CreationTier::TwentyFourHour, |c: CacheTokens| c.creation_24h),
        ] {
            let got = cache_tokens_both_shapes(&usage, tier);
            assert_eq!(get(got.tokens), 500);
            assert!(got.tier_inferred);
        }
    }

    #[test]
    fn negative_counts_are_floored() {
        let usage = json!({
            "cache_read_input_tokens": -5,
            "cache_creation_input_tokens": -10
        });
        let got = cache_tokens_both_shapes(&usage, CreationTier::FiveMinute);
        assert_eq!(got.tokens.read, 0);
        assert_eq!(got.tokens.creation_5m, 0);
        assert!(!got.tier_inferred, "a floored-to-zero total infers nothing");
    }
}
