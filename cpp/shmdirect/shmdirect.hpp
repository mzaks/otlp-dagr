// shmdirect — direct metric instruments over shared memory, from C++ (Dagr spec/38 §7.1,
// §10.3.1; spec/40 §2). The C++ twin of `go/shmdirect`: the same two region files, the
// same layout, the same create-then-rename protocol, so the collector's `dagrshm`
// receiver reads a C++ app exactly as it reads a Go one.
//
//   <name>.dagrshm         the descriptors (MetricsRegion, seqlock): scope, name, unit,
//                          kind, attributes and bounds of every stream, written when a
//                          stream is registered
//   <name>.dagrshm.values  the values (MetricsValues, atomic): slot i is stream i's value,
//                          count, sum, min, max and bucket counts — in `shards` copies of
//                          the region, each on a 128-byte boundary (spec/38 §7.1.1)
//
// Instruments are BOUND (the attribute set is fixed at registration), so a recording is
// the atomic operation and nothing else: `Counter::add` is one atomic add into shared
// memory. This is not the opentelemetry-cpp metric API.
//
// Which copy: C++ has no portable "which core am I on", so a THREAD takes a copy on its
// first recording, round robin, and keeps it. With no more threads than copies no two
// threads share one; past that, two threads share a copy — still correct, since every
// update is atomic. (Go uses the scheduler slot, `procPin`; spec/38 §7.1.1.)
//
// Registration has one writer (a mutex); recordings need none. Header-only, POSIX
// (mmap), C++17, no dependency beyond the generated headers.
#pragma once

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "otlpgen/metrics_region.hpp"
#include "otlpgen/metrics_values.hpp"

namespace shmdirect {

namespace mr = otlpgen::metrics_region;
namespace mv = otlpgen::metrics_values;

// `MetricsRegionMagic` / `Version` and `MetricsValuesMagic` / `Version` in schema.py.
inline constexpr std::uint32_t region_magic = 0x524D4744;   // "DGMR" little-endian
inline constexpr std::uint16_t region_version = 1;
inline constexpr std::uint32_t values_magic = 0x564D4744;   // "DGMV" little-endian
inline constexpr std::uint32_t values_version = 2;

// The distance between two copies of the values region: a 128-byte boundary (Apple
// silicon's cache line, twice x86's), so no line holds words of two copies.
inline constexpr std::size_t stride = (mv::byte_size + 127) & ~std::size_t{127};
inline constexpr std::size_t max_shards = 64;
inline constexpr std::size_t max_streams = 512;

// The OTel SDK's default explicit histogram buckets.
inline const std::vector<double>& default_bounds() {
  static const std::vector<double> b{0, 5, 10, 25, 50, 75, 100, 250, 500, 750, 1000, 2500, 5000, 7500, 10000};
  return b;
}

// One attribute of a stream: a string, an integer, a double or a bool.
struct Attr {
  std::string key;
  mr::AttrKind kind = mr::AttrKind::String;
  std::string s;
  std::int64_t i = 0;
  double d = 0;

  Attr(std::string k, const char* v) : key(std::move(k)), s(v) {}
  Attr(std::string k, std::string v) : key(std::move(k)), s(std::move(v)) {}
  Attr(std::string k, int v) : key(std::move(k)), kind(mr::AttrKind::Int), i(v) {}
  Attr(std::string k, std::int64_t v) : key(std::move(k)), kind(mr::AttrKind::Int), i(v) {}
  Attr(std::string k, double v) : key(std::move(k)), kind(mr::AttrKind::Double), d(v) {}
  Attr(std::string k, bool v) : key(std::move(k)), kind(mr::AttrKind::Bool), i(v ? 1 : 0) {}
};

namespace detail {

inline std::uint64_t now_unix_nano() {
  return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
      std::chrono::system_clock::now().time_since_epoch()).count());
}

inline std::string temp_name(const std::string& path) {
  const auto slash = path.rfind('/');
  const std::string dir = slash == std::string::npos ? "" : path.substr(0, slash + 1);
  const std::string base = slash == std::string::npos ? path : path.substr(slash + 1);
  return dir + "." + base + ".tmp";
}

// A file mapped MAP_SHARED: created under a temporary name, sized (zeroed), stamped, and
// only then renamed into place — a reader never maps a short or unstamped file.
class Mapping {
 public:
  Mapping() = default;
  Mapping(const Mapping&) = delete;
  Mapping& operator=(const Mapping&) = delete;
  ~Mapping() { close(); }

  template <class Stamp>
  bool create(const std::string& path, std::size_t size, Stamp&& stamp, std::string* err) {
    const std::string tmp = temp_name(path);
    fd_ = ::open(tmp.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (fd_ < 0) return fail(err, "open " + tmp);
    if (::ftruncate(fd_, static_cast<off_t>(size)) != 0) { ::unlink(tmp.c_str()); return fail(err, "truncate " + tmp); }
    void* m = ::mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);
    if (m == MAP_FAILED) { ::unlink(tmp.c_str()); return fail(err, "mmap " + tmp); }
    mem_ = static_cast<std::uint8_t*>(m);
    size_ = size;
    stamp(mem_);
    if (::rename(tmp.c_str(), path.c_str()) != 0) { ::unlink(tmp.c_str()); close(); return fail(err, "rename " + tmp); }
    path_ = path;
    return true;
  }

  std::uint8_t* mem() const { return mem_; }

  void close() {
    if (mem_ != nullptr) ::munmap(mem_, size_);
    if (fd_ >= 0) ::close(fd_);
    mem_ = nullptr;
    fd_ = -1;
  }

  // A writer's shutdown: the collector stops seeing the app.
  void remove() {
    close();
    if (!path_.empty()) ::unlink(path_.c_str());
    path_.clear();
  }

 private:
  bool fail(std::string* err, const std::string& what) {
    if (err != nullptr) *err = "shmdirect: " + what + " failed";
    close();
    return false;
  }

  int fd_ = -1;
  std::uint8_t* mem_ = nullptr;
  std::size_t size_ = 0;
  std::string path_;
};

// The copy a recording on this thread goes to: one chosen on the thread's first recording.
inline std::size_t thread_copy(std::size_t n) {
  static std::atomic<std::size_t> next{0};
  thread_local const std::size_t mine = next.fetch_add(1, std::memory_order_relaxed);
  return mine < n ? mine : mine % n;
}

// A stream's value slot in every copy.
class Slots {
 public:
  Slots() = default;
  explicit Slots(std::vector<mv::Value> v) : v_(std::move(v)) {}
  mv::Value mine() const { return v_.size() == 1 ? v_[0] : v_[thread_copy(v_.size())]; }
  mv::Value first() const { return v_[0]; }
  bool valid() const { return !v_.empty(); }

 private:
  std::vector<mv::Value> v_;
};

inline bool put_attr(mr::Attr a, const Attr& at) {
  if (at.key.size() > 32) return false;
  a.set_kind(at.kind);
  switch (at.kind) {
    case mr::AttrKind::String:
      if (!a.set_string_value(at.s)) return false;
      break;
    case mr::AttrKind::Double:
      a.set_double_value(at.d);
      break;
    default:   // int, bool (0 / 1)
      a.set_int_value(at.i);
  }
  return a.set_key(at.key);
}

}  // namespace detail

// A monotonic or up-down sum of integers. `add` is one atomic add.
class Int64Counter {
 public:
  Int64Counter() = default;
  explicit Int64Counter(detail::Slots s) : s_(std::move(s)) {}
  void add(std::int64_t n) const noexcept { mv::Value v = s_.mine(); v.add_int_value(n); }
  bool valid() const { return s_.valid(); }

 private:
  detail::Slots s_;
};

// A monotonic sum of doubles (a CAS loop: there is no atomic float add).
class Float64Counter {
 public:
  Float64Counter() = default;
  explicit Float64Counter(detail::Slots s) : s_(std::move(s)) {}
  void add(double x) const noexcept { mv::Value v = s_.mine(); v.add_double_value(x); }
  bool valid() const { return s_.valid(); }

 private:
  detail::Slots s_;
};

// The last recorded value — in copy 0 only: "last" does not merge over copies.
class Float64Gauge {
 public:
  Float64Gauge() = default;
  explicit Float64Gauge(detail::Slots s) : s_(std::move(s)) {}
  void record(double x) const noexcept { mv::Value v = s_.first(); v.set_double_value(x); }
  bool valid() const { return s_.valid(); }

 private:
  detail::Slots s_;
};

// Values counted into explicit buckets: bucket k holds (bounds[k-1], bounds[k]].
class Float64Histogram {
 public:
  Float64Histogram() = default;
  Float64Histogram(detail::Slots s, std::vector<double> bounds) : s_(std::move(s)), bounds_(std::move(bounds)) {}

  // A few atomic operations: the bucket, the count, the sum, and min / max when the value
  // extends them.
  void record(double x) const noexcept {
    mv::Value v = s_.mine();
    v.add_buckets(static_cast<std::size_t>(std::lower_bound(bounds_.begin(), bounds_.end(), x) - bounds_.begin()), 1);
    v.add_count(1);
    v.add_double_value(x);
    for (double m = v.min(); x < m && !v.compare_and_swap_min(m, x); m = v.min()) {
    }
    for (double m = v.max(); x > m && !v.compare_and_swap_max(m, x); m = v.max()) {
    }
  }
  bool valid() const { return s_.valid(); }

 private:
  detail::Slots s_;
  std::vector<double> bounds_;
};

struct Options {
  // Per-core copies of the values region: 0 is std::thread::hardware_concurrency(),
  // capped at max_shards; 1 turns sharding off.
  std::size_t shards = 0;
};

// An app's two regions.
class Provider {
 public:
  // Creates the regions at `path` (e.g. /run/otlp-dagr/checkout-1.dagrshm) for `service`.
  // nullptr and `*err` on failure.
  static std::unique_ptr<Provider> create(const std::string& path, std::string_view service,
                                          const std::vector<Attr>& resource = {}, Options o = {},
                                          std::string* err = nullptr) {
    std::size_t n = o.shards != 0 ? o.shards : std::max(1u, std::thread::hardware_concurrency());
    n = std::min(n, max_shards);
    std::unique_ptr<Provider> p(new Provider());
    // the values first: a receiver that sees the descriptors must find them
    if (!p->vals_.create(path + ".values", n * stride, [n](std::uint8_t* m) {
          for (std::size_t k = 0; k < n; k++) {
            mv::Values v = mv::wrap(m + k * stride);
            v.set_magic(values_magic);
            v.set_version(values_version);
            v.set_shards(static_cast<std::uint32_t>(n));
          }
        }, err)) {
      return nullptr;
    }
    for (std::size_t k = 0; k < n; k++) p->copies_.push_back(mv::wrap(p->vals_.mem() + k * stride));
    // the descriptor region: stamped through its seqlock before it gets its name
    const bool ok = p->desc_.create(path, mr::byte_size, [&](std::uint8_t* m) {
      p->lock_ = std::make_unique<mr::Seqlock>(m);
      mr::State w = p->lock_->begin_write();
      w.set_magic(region_magic);
      w.set_version(region_version);
      w.set_service(service.substr(0, 64));
      std::uint8_t len = 0;
      for (const Attr& a : resource) {
        if (len == 16 || !detail::put_attr(w.resource(len), a)) break;
        w.set_resource_len(++len);
      }
      w.set_start_unix_nano(detail::now_unix_nano());
      p->lock_->end_write();
    }, err);
    if (!ok) {
      p->vals_.remove();
      return nullptr;
    }
    return p;
  }

  ~Provider() { shutdown(); }

  std::size_t shards() const { return copies_.size(); }

  // Removes both regions: a collector stops seeing the app.
  void shutdown() {
    std::lock_guard<std::mutex> g(mu_);
    vals_.remove();
    desc_.remove();
  }

  // Each instrument is invalid (`valid() == false`) when the regions are full or a name,
  // unit, scope, attribute or bound list exceeds their capacity.
  Int64Counter int64_counter(std::string_view scope, std::string_view name, std::string_view unit,
                             const std::vector<Attr>& attrs = {}) {
    return Int64Counter(reg(scope, name, unit, mr::PointKind::SumInt, true, {}, attrs));
  }
  Int64Counter int64_up_down_counter(std::string_view scope, std::string_view name, std::string_view unit,
                                     const std::vector<Attr>& attrs = {}) {
    return Int64Counter(reg(scope, name, unit, mr::PointKind::SumInt, false, {}, attrs));
  }
  Float64Counter float64_counter(std::string_view scope, std::string_view name, std::string_view unit,
                                 const std::vector<Attr>& attrs = {}) {
    return Float64Counter(reg(scope, name, unit, mr::PointKind::SumDouble, true, {}, attrs));
  }
  Float64Gauge float64_gauge(std::string_view scope, std::string_view name, std::string_view unit,
                             const std::vector<Attr>& attrs = {}) {
    return Float64Gauge(reg(scope, name, unit, mr::PointKind::GaugeDouble, false, {}, attrs));
  }
  Float64Histogram float64_histogram(std::string_view scope, std::string_view name, std::string_view unit,
                                     std::vector<double> bounds = default_bounds(),
                                     const std::vector<Attr>& attrs = {}) {
    detail::Slots s = reg(scope, name, unit, mr::PointKind::Histogram, false, bounds, attrs);
    return Float64Histogram(std::move(s), std::move(bounds));
  }

 private:
  Provider() = default;

  // Writes stream n's descriptor and hands out its value slot in every copy.
  detail::Slots reg(std::string_view scope, std::string_view name, std::string_view unit, mr::PointKind kind,
                    bool monotonic, const std::vector<double>& bounds, const std::vector<Attr>& attrs) {
    if (scope.size() > 32 || name.size() > 64 || unit.size() > 16 || attrs.size() > 8 || bounds.size() > 15) return {};
    std::lock_guard<std::mutex> g(mu_);
    if (n_ == max_streams || lock_ == nullptr) return {};
    const std::size_t i = n_;
    std::vector<mv::Value> vs;
    for (mv::Values& c : copies_) {
      mv::Value v = *c.points(i);
      v.set_min(std::numeric_limits<double>::infinity());
      v.set_max(-std::numeric_limits<double>::infinity());
      vs.push_back(v);
    }
    mr::State w = lock_->begin_write();
    mr::Point pt = w.points(i);
    pt.set_scope(scope);
    pt.set_name(name);
    pt.set_unit(unit);
    pt.set_kind(kind);
    pt.set_monotonic(monotonic);
    for (std::size_t k = 0; k < attrs.size(); k++) {
      if (!detail::put_attr(pt.attrs(k), attrs[k])) {
        // the staging copy is never published: the region keeps its last state
        return {};
      }
    }
    pt.set_attrs_len(static_cast<std::uint8_t>(attrs.size()));
    for (std::size_t k = 0; k < bounds.size(); k++) pt.set_bounds(k, bounds[k]);
    pt.set_bounds_len(static_cast<std::uint8_t>(bounds.size()));
    if (kind == mr::PointKind::Histogram) pt.set_bucket_counts_len(static_cast<std::uint8_t>(bounds.size() + 1));
    w.set_points_len(static_cast<std::uint16_t>(i + 1));
    w.set_exports(w.exports() + 1);
    w.set_write_unix_nano(detail::now_unix_nano());
    lock_->end_write();
    for (mv::Values& c : copies_) c.set_points_len(static_cast<std::uint32_t>(i + 1));   // a reader takes the least
    n_++;
    return detail::Slots(std::move(vs));
  }

  std::mutex mu_;   // registration: the descriptor region has one writer
  detail::Mapping desc_;
  detail::Mapping vals_;
  std::unique_ptr<mr::Seqlock> lock_;   // holds a staging copy of the region (~719 KB)
  std::vector<mv::Values> copies_;
  std::size_t n_ = 0;
};

}  // namespace shmdirect
