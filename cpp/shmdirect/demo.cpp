// The producer half of the C++ → Go gate (spec/38 §10.3.1): records a fixed script of
// direct instruments into `<dir>/cpp.dagrshm(.values)`, which the Go `dagrshm` receiver
// then reads (go/shmreceiver/cpp_test.go). Driven over stdio:
//
//   prints "ready" once the script is recorded; each line it then reads ("more") adds 1 to
//   http.requests and prints "ready" again; end of input removes both regions and exits.
//
//   shmdirect_demo <dir>
#include <cstdio>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "shmdirect.hpp"

int main(int argc, char** argv) {
  if (argc != 2) {
    std::fprintf(stderr, "usage: %s <dir>\n", argv[0]);
    return 2;
  }
  std::string err;
  // 4 copies, 8 recording threads: two threads share each copy, and every total must
  // still be exact
  auto p = shmdirect::Provider::create(std::string(argv[1]) + "/cpp.dagrshm", "cppapp",
                                       {{"host.name", "h1"}, {"process.pid", std::int64_t(::getpid())}},
                                       shmdirect::Options{4}, &err);
  if (!p) {
    std::fprintf(stderr, "%s\n", err.c_str());
    return 1;
  }
  auto reqs = p->int64_counter("shop", "http.requests", "{request}", {{"route", "/pay"}, {"code", 200}});
  auto bytes = p->float64_counter("shop", "net.bytes", "By");
  auto depth = p->int64_up_down_counter("shop", "queue.depth", "1");
  auto temp = p->float64_gauge("shop", "cpu.temp", "Cel", {{"sensor", 2.5}, {"ok", true}});
  auto lat = p->float64_histogram("shop", "http.duration", "ms");
  auto hits = p->int64_counter("stress", "hits", "1");
  auto hlat = p->float64_histogram("stress", "lat", "ms", {10, 100});
  // what does not fit is refused, not written half
  auto refused = p->int64_counter("shop", std::string(65, 'n'), "1");
  if (!reqs.valid() || !bytes.valid() || !depth.valid() || !temp.valid() || !lat.valid() ||
      !hits.valid() || !hlat.valid() || refused.valid()) {
    std::fprintf(stderr, "registration\n");
    return 1;
  }
  reqs.add(7);
  reqs.add(5);
  bytes.add(1.5);
  depth.add(3);
  depth.add(-1);
  temp.record(61.5);
  for (double v : {3.0, 42.0, 42.0, 900.0, 12000.0}) lat.record(v);

  std::vector<std::thread> ts;
  for (int w = 0; w < 8; w++) {
    ts.emplace_back([&] {
      for (int i = 0; i < 100000; i++) {
        hits.add(1);
        hlat.record(double(i % 200));
      }
    });
  }
  for (auto& t : ts) t.join();

  std::cout << "ready" << std::endl;
  for (std::string line; std::getline(std::cin, line);) {
    reqs.add(1);
    std::cout << "ready" << std::endl;
  }
  p->shutdown();
  return 0;
}
