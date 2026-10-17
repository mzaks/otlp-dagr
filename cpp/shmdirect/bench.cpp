// What a direct recording costs from C++ (spec/38 §10.3.1) — the rows of §7.1 / §7.1.1's
// Go table: one thread, then every hardware thread on ONE stream, with one copy of the
// values region and with one per thread.
//
//   shmdirect_bench <dir>
#include <chrono>
#include <cstdio>
#include <string>
#include <thread>
#include <vector>

#include "shmdirect.hpp"

template <class F>
static double ns_per_op(unsigned threads, long per, F f) {
  std::vector<std::thread> ts;
  const auto t0 = std::chrono::steady_clock::now();
  for (unsigned w = 0; w < threads; w++)
    ts.emplace_back([&, w] {
      for (long i = 0; i < per; i++) f(w, i);
    });
  for (auto& t : ts) t.join();
  const double ns = std::chrono::duration<double, std::nano>(std::chrono::steady_clock::now() - t0).count();
  return ns / (double(per) * threads);   // wall time over every thread's operations
}

int main(int argc, char** argv) {
  if (argc != 2) {
    std::fprintf(stderr, "usage: %s <dir>\n", argv[0]);
    return 2;
  }
  const unsigned hw = std::max(1u, std::thread::hardware_concurrency());
  std::printf("%-22s %10s %10s %12s %12s\n", "", "1 thr/1 cp", "1 thr/N cp", "N thr/1 cp", "N thr/N cp");
  for (int kind = 0; kind < 2; kind++) {
    double r[4];
    for (int c = 0; c < 2; c++) {
      const std::size_t shards = c == 0 ? 1 : hw;
      auto p = shmdirect::Provider::create(std::string(argv[1]) + "/bench.dagrshm", "bench", {},
                                           shmdirect::Options{shards});
      auto ctr = p->int64_counter("b", "hits", "1");
      auto h = p->float64_histogram("b", "lat", "ms");
      auto op = [&](unsigned, long i) {
        if (kind == 0) ctr.add(1);
        else h.record(double(i & 1023));
      };
      ns_per_op(1, 2000000, op);   // warm up
      r[c] = ns_per_op(1, 20000000, op);
      r[2 + c] = ns_per_op(hw, 5000000, op);
      p->shutdown();
    }
    std::printf("%-22s %8.2f ns %8.2f ns %10.2f ns %10.2f ns\n", kind == 0 ? "counter add" : "histogram record",
                r[0], r[1], r[2], r[3]);
  }
  std::printf("(%u hardware threads)\n", hw);
  return 0;
}
