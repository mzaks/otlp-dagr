//! A long loop over one batch, for a sampling profiler: `prof_dagr <file.dagr> <signal> <n>`.
fn main() {
    let a: Vec<String> = std::env::args().collect();
    let buf = std::fs::read(&a[1]).unwrap();
    let n: usize = a[3].parse().unwrap();
    let mut p = otlp_dagr_otap::Producer::default();
    let mut total = 0;
    for _ in 0..n {
        total += match a[2].as_str() {
            "traces" => p.traces(&buf),
            "logs" => p.logs(&buf),
            _ => p.metrics(&buf),
        }
        .unwrap()
        .len();
    }
    println!("{total}");
}
