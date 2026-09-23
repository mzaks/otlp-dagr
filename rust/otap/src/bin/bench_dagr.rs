//! The Dagr half of `bench.sh`: `<signal>-<name> <µs> <bytes>` per batch — lazy Dagr
//! accessors → Arrow columns → IPC → serialized BatchArrowRecords.
use otlp_dagr_otap::timing::{BATCHES, time};

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
        println!("{sig}-{name} {us:.1} {n}");
    }
}
