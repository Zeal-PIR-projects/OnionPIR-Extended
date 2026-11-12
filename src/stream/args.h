#pragma once
// Tiny "--flag value" parser shared by the onion_server / onion_client apps.

#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <map>
#include <string>

namespace stream {

class Args {
public:
  Args(int argc, char **argv, std::string usage) : usage_(std::move(usage)) {
    for (int i = 1; i < argc; ++i) {
      std::string key = argv[i];
      if (key == "-h" || key == "--help") fail("");
      if (key.rfind("--", 0) != 0 || i + 1 >= argc) fail("bad argument: " + key);
      values_[key.substr(2)] = argv[++i];
    }
  }
  std::string str(const std::string &key, const std::string &def = "") {
    used_[key] = true;
    auto it = values_.find(key);
    if (it != values_.end()) return it->second;
    if (def.empty()) fail("missing --" + key);
    return def;
  }
  uint64_t num(const std::string &key, int64_t def = -1) {
    used_[key] = true;
    auto it = values_.find(key);
    if (it == values_.end()) {
      if (def < 0) fail("missing --" + key);
      return static_cast<uint64_t>(def);
    }
    char *end = nullptr;
    const uint64_t v = std::strtoull(it->second.c_str(), &end, 10);
    if (!end || *end) fail("--" + key + " expects an integer");
    return v;
  }
  // Call after reading every flag: rejects unknown flags.
  void finish() {
    for (const auto &[k, v] : values_)
      if (!used_.count(k)) fail("unknown flag --" + k);
  }

private:
  [[noreturn]] void fail(const std::string &msg) {
    if (!msg.empty()) std::cerr << "error: " << msg << "\n";
    std::cerr << usage_;
    std::exit(2);
  }
  std::string usage_;
  std::map<std::string, std::string> values_;
  std::map<std::string, bool> used_;
};

} // namespace stream
