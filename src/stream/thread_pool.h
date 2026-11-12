#pragma once
// Minimal fixed-size thread pool. Workers have stable ids in [0, size()) so
// callers can keep per-thread scratch (e.g. GSWEval, whose external product
// reuses internal buffers). Workers disable the global TimerLogger, which is
// not thread-safe.

#include "logging.h"
#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <exception>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>

namespace stream {

class ThreadPool {
public:
  explicit ThreadPool(size_t num_threads) {
    if (num_threads == 0) num_threads = 1;
    for (size_t id = 0; id < num_threads; ++id)
      workers_.emplace_back([this, id] { worker_loop(id); });
  }
  ~ThreadPool() {
    {
      std::lock_guard<std::mutex> lock(mu_);
      stopping_ = true;
    }
    cv_.notify_all();
    for (auto &t : workers_) t.join();
  }
  ThreadPool(const ThreadPool &) = delete;
  ThreadPool &operator=(const ThreadPool &) = delete;

  size_t size() const { return workers_.size(); }

  // Id of the calling worker, or SIZE_MAX off the pool.
  static size_t worker_id() { return tl_worker_id_; }

  void submit(std::function<void()> task) {
    {
      std::lock_guard<std::mutex> lock(mu_);
      tasks_.push_back(std::move(task));
    }
    cv_.notify_one();
  }

  // Run fn(i) for i in [0, n) across the pool and wait. Must not be called
  // from a worker. Rethrows the first exception.
  void parallel_for(size_t n, const std::function<void(size_t)> &fn) {
    if (n == 0) return;
    std::atomic<size_t> next{0};
    std::exception_ptr error;
    std::mutex done_mu;
    std::condition_variable done_cv;
    const size_t runners = std::min(n, size());
    size_t remaining = runners;
    for (size_t r = 0; r < runners; ++r) {
      submit([&] {
        try {
          for (size_t i = next++; i < n; i = next++) fn(i);
        } catch (...) {
          std::lock_guard<std::mutex> lock(done_mu);
          if (!error) error = std::current_exception();
          next = n;
        }
        std::lock_guard<std::mutex> lock(done_mu);
        if (--remaining == 0) done_cv.notify_one();
      });
    }
    std::unique_lock<std::mutex> lock(done_mu);
    done_cv.wait(lock, [&] { return remaining == 0; });
    if (error) std::rethrow_exception(error);
  }

private:
  void worker_loop(size_t id) {
    tl_worker_id_ = id;
    TimerLogger::setThreadEnabled(false);
    for (;;) {
      std::function<void()> task;
      {
        std::unique_lock<std::mutex> lock(mu_);
        cv_.wait(lock, [this] { return stopping_ || !tasks_.empty(); });
        if (tasks_.empty()) return;  // stopping and drained
        task = std::move(tasks_.front());
        tasks_.pop_front();
      }
      task();
    }
  }

  static inline thread_local size_t tl_worker_id_ = SIZE_MAX;
  std::vector<std::thread> workers_;
  std::deque<std::function<void()>> tasks_;
  std::mutex mu_;
  std::condition_variable cv_;
  bool stopping_ = false;
};

} // namespace stream
