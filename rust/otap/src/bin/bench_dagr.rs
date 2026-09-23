//! The Dagr half of `bench.sh`: `<signal>-<name> <µs> <bytes>` per batch — lazy Dagr
//! accessors → Arrow columns → IPC → serialized BatchArrowRecords.
use otlp_dagr_otap::timing::{BATCHES, STREAM, time};

fn main() {
    let dir = std::path::PathBuf::from(std::env::args().nth(1).expect("work dir"));
    for (sig, name) in BATCHES {
        let dagr = std::fs::read(dir.join(format!("{sig}-{name}.dagr"))).unwrap();
        let mut p = otlp_dagr_otap::Producer::default();
        let (us, n) = time(|| {
            match sig {
                "traces" => p.traces(&dagr),
                "logs" => p.logs(&dagr),
                _ => p.metrics(&dagr),
            }
            .unwrap()
            .len()
        });
        // cold: the first batch of a fresh producer (every schema and dictionary sent);
        // then the same with zstd, cold and repeated
        let size = |zstd: bool, times: usize| {
            let mut p = otlp_dagr_otap::Producer::default().with_zstd(zstd);
            let mut n = 0;
            for _ in 0..times {
                n = match sig {
                    "traces" => p.traces(&dagr),
                    "logs" => p.logs(&dagr),
                    _ => p.metrics(&dagr),
                }
                .unwrap()
                .len();
            }
            n
        };
        println!("{sig}-{name} {us:.1} {} {n} {} {}", size(false, 1), size(true, 1), size(true, 2));
    }
    // the stream: 20 consecutive batches through ONE producer — `<signal>-stream µs/batch
    // plain-total zstd-total`
    for sig in ["traces", "logs", "metrics"] {
        let batches: Vec<Vec<u8>> =
            (0..STREAM).map(|i| std::fs::read(dir.join(format!("{sig}-stream-{i:02}.dagr"))).unwrap()).collect();
        let run = |zstd: bool| {
            let mut p = otlp_dagr_otap::Producer::default().with_zstd(zstd);
            let t = std::time::Instant::now();
            let mut total = 0;
            for b in &batches {
                total += match sig {
                    "traces" => p.traces(b),
                    "logs" => p.logs(b),
                    _ => p.metrics(b),
                }
                .unwrap()
                .len();
            }
            (t.elapsed().as_secs_f64() * 1e6 / STREAM as f64, total)
        };
        let _ = run(false); // warm
        let (us, plain) = (0..5).map(|_| run(false)).fold((f64::MAX, 0), |a, b| (a.0.min(b.0), b.1));
        let (_, z) = run(true);
        println!("{sig}-stream {us:.1} {plain} {z}");
    }
}
