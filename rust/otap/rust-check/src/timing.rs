//! The timing loop both halves of `bench.sh` use (the Dagr side here, otel-arrow's in
//! rust-check/src/bin/bench.rs — arrow 53 and otel-arrow's datafusion cannot be linked into
//! one binary, their chrono requirements conflict). Keep the two copies identical.

use std::time::Instant;

/// Best-of-7 mean µs per call over 200 calls, after 50 warm-up calls; and the last output.
pub fn time<F: FnMut() -> usize>(mut f: F) -> (f64, usize) {
    let mut out = 0;
    for _ in 0..50 {
        out = f();
    }
    let mut best = f64::MAX;
    for _ in 0..7 {
        let n = 200;
        let t = Instant::now();
        for _ in 0..n {
            out = std::hint::black_box(f());
        }
        best = best.min(t.elapsed().as_secs_f64() * 1e6 / n as f64);
    }
    (best, out)
}

/// The batches both halves time.
pub const BATCHES: [(&str, &str); 5] =
    [("traces", "batch"), ("logs", "batch"), ("metrics", "batch"), ("traces", "wide"), ("metrics", "wide")];
