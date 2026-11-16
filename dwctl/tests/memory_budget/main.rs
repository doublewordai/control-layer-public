
mod alloc;
mod harness;
mod measure;
mod payload;
mod tokenizer;
mod upstream;

use std::sync::{Mutex, MutexGuard};

use harness::{Harness, MODEL_ALIAS, Options, UPSTREAM_MODEL};
use measure::{Load, Scaling, Target, rounds, scaling};
use upstream::Reply;

#[global_allocator]
static ALLOCATOR: alloc::CountingAllocator = alloc::CountingAllocator;

static ONE_AT_A_TIME: Mutex<()> = Mutex::new(());

const REQUEST_BYTES: [usize; 2] = [96 * 1024, 300 * 1024];
const STREAMED_TOKENS: [usize; 2] = [256, 2304];
const COMPLETE_TOKENS: [usize; 2] = [8192, 32768];
const API_KEYS: usize = 10_000;

struct Budget {
    copies: f64,
    fixed_kib: f64,
}

#[allow(clippy::print_stdout, reason = "the measurement, shown with --nocapture")]
fn check(name: &str, scaling: &Scaling, budget: Budget) {
    println!("{name}: {scaling}");
    let fixed_kib = scaling.fixed / 1024.0;
    assert!(
        scaling.copies <= budget.copies && fixed_kib <= budget.fixed_kib,
        "{name} is over budget: copies {:.2} (budget {:.2}), fixed {fixed_kib:.1} KiB (budget {:.1} KiB)",
        scaling.copies,
        budget.copies,
        budget.fixed_kib,
    );
}

fn one_at_a_time() -> MutexGuard<'static, ()> {
    ONE_AT_A_TIME.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[test]
fn load_generator_is_not_counted() {
    let _one_at_a_time = one_at_a_time();
    let harness = Harness::start(Options { api_keys: 1 });
    let body = payload::chat_request(MODEL_ALIAS, REQUEST_BYTES[1], false);
    let worst = |round: &measure::Round| round.held.abs().max(round.peak.abs()).max(round.retained.abs());
    let round = rounds(&harness, || Load {
        target: Target::Upstream,
        path: "/ai/v1/chat/completions",
        size: body.len(),
        body: body.clone(),
        reply: Reply::Hold,
    })
    .min_by(|a, b| worst(a).total_cmp(&worst(b)))
    .expect("at least one attempt");
    let kib = |bytes: f64| bytes / 1024.0;
    println!(
        "load generator and upstream alone: held {:.1} KiB, peak {:.1} KiB, retained {:.1} KiB per request",
        kib(round.held),
        kib(round.peak),
        kib(round.retained)
    );
    for bytes in [round.held, round.peak, round.retained] {
        assert!(kib(bytes).abs() < 4.0, "load generator counted: {round:?}");
    }
}

#[test]
fn chat_completion_request_body() {
    let _one_at_a_time = one_at_a_time();
    let harness = Harness::start(Options { api_keys: API_KEYS });
    let scaling = scaling(
        &harness,
        |size| {
            let body = payload::chat_request(MODEL_ALIAS, size, false);
            Load {
                target: Target::Application,
                path: "/ai/v1/chat/completions",
                size: body.len(),
                body,
                reply: Reply::Hold,
            }
        },
        REQUEST_BYTES,
    );
    check(
        "chat completion request body",
        &scaling,
        Budget {
            copies: 10.35,
            fixed_kib: 310.0,
        },
    );
}

#[test]
fn chat_completion_streamed_response() {
    let _one_at_a_time = one_at_a_time();
    let harness = Harness::start(Options { api_keys: API_KEYS });
    let scaling = scaling(
        &harness,
        |tokens| Load {
            target: Target::Application,
            path: "/ai/v1/chat/completions",
            body: payload::chat_request(MODEL_ALIAS, 0, true),
            reply: Reply::StreamThenHold { tokens },
            size: payload::stream(UPSTREAM_MODEL, tokens).content_bytes(),
        },
        STREAMED_TOKENS,
    );
    check(
        "chat completion streamed response",
        &scaling,
        Budget {
            copies: 1.25,
            fixed_kib: 150.0,
        },
    );
}

#[test]
fn chat_completion_response() {
    let _one_at_a_time = one_at_a_time();
    let harness = Harness::start(Options { api_keys: API_KEYS });
    let scaling = scaling(
        &harness,
        |tokens| Load {
            target: Target::Application,
            path: "/ai/v1/chat/completions",
            body: payload::chat_request(MODEL_ALIAS, 0, false),
            reply: Reply::Complete { tokens },
            size: payload::completion(UPSTREAM_MODEL, tokens).len(),
        },
        COMPLETE_TOKENS,
    );
    check(
        "chat completion response",
        &scaling,
        Budget {
            copies: 2.0,
            fixed_kib: 70.0,
        },
    );
}
