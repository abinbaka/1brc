// clang-format off
#include <algorithm>
#include <atomic>
#include <cstdint>
#include <fcntl.h>
#include <immintrin.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <iostream>
#include <chrono>
#include <stdio.h>
//#include "xxh3.h"
#include <sched.h>
#include "rapidhash.h"
// clang-format on

struct TimeElaped final {
  std::string name;
  std::chrono::system_clock::time_point start;
  TimeElaped(const std::string &n)
      : name(n), start(std::chrono::system_clock::now()) {}
  ~TimeElaped(void) noexcept {
    const auto stop = std::chrono::system_clock::now();
    const auto span =
        std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
    std::cout << name << " " << span.count() << " nanoseconds" << std::endl;
  }
};

static constexpr int max_distinct_groups = 10000;
static constexpr int hash_buckets_size = 16384;
static constexpr int parallel_threads = 16;

struct StrView final {
  const char *ptr{nullptr};
  uint8_t len{0};
  StrView() {}
  StrView(const char *p, uint8_t n) : ptr(p), len(n) {}
};

constexpr uint32_t kInfoPaddingSize = 64 - sizeof(StrView) - 20;
struct alignas(64) Info final {
  StrView cn;
  int32_t slot_count{0};
  int32_t count{0};
  int64_t sum{0};
  int16_t min{0};
  int16_t max{0};
  // Info *next{nullptr};
  // char padding[kInfoPaddingSize];
  Info() {}
  // Info(StrView sv, int16_t v) : cn(sv), count(1), sum(v), min(v), max(v) {}
};

struct BrcMap final {
  Info infos[hash_buckets_size];

  inline void Insert(StrView city_name, int16_t temperature) noexcept {
    const size_t hv = /*XXH3_64bits*/ rapidhash(city_name.ptr, city_name.len);
    uint32_t slot_index = hv & (hash_buckets_size - 1);
  retry_t:
    if (infos[slot_index].slot_count == 0)
      [[unlikely]] {
        infos[slot_index].cn = city_name;
        infos[slot_index].count = 1;
        infos[slot_index].sum = temperature;
        infos[slot_index].min = temperature;
        infos[slot_index].max = temperature;
        ++infos[slot_index].slot_count;
      }
    else
      [[likely]] {
        Info *info = &infos[slot_index];
        if (info->cn.len == city_name.len &&
            memcmp(info->cn.ptr, city_name.ptr, city_name.len) == 0)
          [[likely]] {
            info->count += 1;
            info->sum += temperature;
            if (temperature < info->min) {
              info->min = temperature;
            } else if (temperature > info->max) {
              info->max = temperature;
            }

            // Found
            return;
          }
        else
          [[unlikely]] {
            // Move to next entry
            slot_index += 1;
            goto retry_t;
          }
      }
  }
};

constexpr uint32_t kTagCount = 58;
// clang-format off
// x 100
static constexpr int16_t PC1[kTagCount] = {
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 100, 
  200, 300, 400, 500, 600, 700, 800, 900
};

// x 10
static constexpr int8_t PC2[kTagCount] = {
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 10, 
  20, 30, 40, 50, 60, 70, 80, 90
};

// x 1
static constexpr int8_t PC3[kTagCount] = {
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 
  2, 3, 4, 5, 6, 7, 8, 9
};
// clang-format on

// branchless min/max (on some machines at least)
#define min(a, b) (a ^ ((b ^ a) & -(b < a)));
#define max(a, b) (a ^ ((a ^ b) & -(a < b)));

// parses a floating point number as an integer
// this is only possible because we know our data file has only a single decimal
static inline const char *parse_number(int16_t *dest, const char *s) noexcept {
  // parse sign
  int mod = 1;
  if (*s == '-') {
    mod = -1;
    s++;
  }

  if (s[2] == '.')
    [[likely]] { // s[] = xx.x
      *dest = (PC1[s[0]] + PC2[s[1]] + PC3[s[3]]) * mod;
      return s + 5;
    }
  else
    [[unlikely]] { // s[] = x.x
      *dest = (PC2[s[0]] + PC3[s[2]]) * mod;
      return s + 4;
    }
}

struct RangePair final {
  const char *begin;
  const char *end;
};
static constexpr int chunk_count = parallel_threads;
static uint64_t chunk_size = 0;
static RangePair range_pairs[chunk_count];
// pthread_barrier_t process_barrier;       // 16
// pthread_barrier_t merger_level1_barrier; // 8
// pthread_barrier_t merger_level2_barrier; // 4
// pthread_barrier_t merger_level3_barrier; // 2
BrcMap thd_entries[parallel_threads];
static int32_t processed_count[parallel_threads] = {0};
std::vector<Info *> final_result;

// chunk-0
static void *chunk_func_s(void *_data) noexcept {
  // TimeElaped te("chunk-0");
  BrcMap &mapping = thd_entries[0];
  const char *start = range_pairs[0].begin;
  const char *original_start = start;

  const char *end = range_pairs[0].end;
  while (*end != '\n') {
    end++;
  }
  end++;

  // flaming hot loop
  while (start < end) {
    const char *linestart = start;

    // find position of ;, since minimal city name is 1,
    // so start from s[1] to skip some comparation
    int len = 1;
    while (start[len] != ';') {
      len += 1;
    }

    // parse decimal number as int
    int16_t temperature;
    start = parse_number(&temperature, linestart + len + 1);
#if 1
    if ((start - original_start) & (64 << 10 - 1) == 0) {
      ::madvise((void *)original_start, start - original_start, MADV_DONTNEED);
    }

    // city name = [linestart, len]
    int32_t left = processed_count[7];
    int32_t right = processed_count[1];
    int32_t now = processed_count[0]++;
    if (left > 0 && right > 0 && now > left + 1024 && now > right + 1024) {
      sched_yield();
    }
#endif
    mapping.Insert({linestart, (uint8_t)len}, temperature);
  }

  // std::string dstr;
  // for (int n = 0; n < hash_buckets_size; n++) {
  //  if (mapping.infos[n].slot_count >= 2) {
  //    dstr.append(std::to_string(n) + "," +
  //    std::to_string(mapping.infos[n].slot_count) + " ; ");
  //  }
  //}
  // std::cout << dstr << std::endl;

  return nullptr;
}

static void *chunk_func_m(void *_data) noexcept {
  const uint32_t chunk_id = *(const uint32_t *)_data;
  // TimeElaped te("chunk-" + std::to_string(chunk_id));
  BrcMap &mapping = thd_entries[chunk_id];
  const char *start = range_pairs[chunk_id].begin;

  if (*(start - 1) != '\n') {
    while (*start != '\n') {
      start++;
    }
    start++;
  }
  const char *original_start = start;

  const char *end = range_pairs[chunk_id].end;
  while (*end != '\n') {
    end++;
  }
  end++;

  // flaming hot loop
  while (start < end) {
    const char *linestart = start;

    // find position of ;, since minimal city name is 1,
    // so start from s[1] to skip some comparation
    int len = 1;
    while (start[len] != ';') {
      len += 1;
    }

    // parse decimal number as int
    int16_t temperature;
    start = parse_number(&temperature, linestart + len + 1);
#if 1
    if ((start - original_start) & (64 << 10 - 1) == 0) {
      ::madvise((void *)original_start, start - original_start, MADV_DONTNEED);
    }

    // city name = [linestart, len]
    int32_t left = processed_count[7];
    int32_t right = processed_count[1];
    int32_t now = processed_count[0]++;
    if (left > 0 && right > 0 && now > left + 1024 && now > right + 1024) {
      sched_yield();
    }
#endif
    mapping.Insert({linestart, (uint8_t)len}, temperature);
  }

  // std::string dstr;
  // for (int n = 0; n < hash_buckets_size; n++) {
  //  if (mapping.infos[n].slot_count >= 2) {
  //    dstr.append(std::to_string(n) + "," +
  //    std::to_string(mapping.infos[n].slot_count) + " ; ");
  //  }
  //}
  // std::cout << dstr << std::endl;

  return nullptr;
}

static void *chunk_func_e(void *_data) noexcept {
  // TimeElaped te("chunk-" + std::to_string(chunk_count - 1));
  BrcMap &mapping = thd_entries[chunk_count - 1];
  const char *start = range_pairs[chunk_count - 1].begin;

  if (*(start - 1) != '\n') {
    while (*start != '\n') {
      start++;
    }
    start++;
  }
  const char *original_start = start;
  const char *end = range_pairs[chunk_count - 1].end;

  // flaming hot loop
  while (start < end) {
    const char *linestart = start;

    // find position of ;, since minimal city name is 1,
    // so start from s[1] to skip some comparation
    int len = 1;
    while (start[len] != ';') {
      len += 1;
    }

    // parse decimal number as int
    int16_t temperature;
    start = parse_number(&temperature, linestart + len + 1);
#if 1
    if ((start - original_start) & (64 << 10 - 1) == 0) {
      ::madvise((void *)original_start, start - original_start, MADV_DONTNEED);
    }

    // city name = [linestart, len]
    int32_t left = processed_count[7];
    int32_t right = processed_count[1];
    int32_t now = processed_count[0]++;
    if (left > 0 && right > 0 && now > left + 1024 && now > right + 1024) {
      sched_yield();
    }
#endif
    mapping.Insert({linestart, (uint8_t)len}, temperature);
  }

  // std::string dstr;
  // for (int n = 0; n < hash_buckets_size; n++) {
  //  if (mapping.infos[n].slot_count >= 2) {
  //    dstr.append(std::to_string(n) + "," +
  //    std::to_string(mapping.infos[n].slot_count) + " ; ");
  //  }
  //}
  // std::cout << dstr << std::endl;

  return nullptr;
}

static constexpr int std_out_size = 1 << 20;
static char std_out_buffer[std_out_size];
static constexpr uint32_t thd_params[parallel_threads] = {
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};

int main(int argc, char **argv) {
  const char *file = "measurements.txt";
  if (argc > 1)
    [[likely]] { file = argv[1]; }

  int fd = ::open(file, O_RDONLY | O_NOATIME);
  if (!fd)
    [[unlikely]] {
      perror("error opening file");
      exit(EXIT_FAILURE);
    }

  struct stat sb;
  if (::fstat(fd, &sb) == -1)
    [[unlikely]] {
      perror("error getting file size");
      exit(EXIT_FAILURE);
    }

  // mmap entire file into memory
  const size_t sz = (size_t)sb.st_size;
  const char *data = (const char *)::mmap(NULL, sz, PROT_READ,
                                          MAP_SHARED /*| MAP_POPULATE*/, fd, 0);
  if (data == MAP_FAILED)
    [[unlikely]] {
      perror("error mmapping file");
      exit(EXIT_FAILURE);
    }

  // distribute work among N worker threads
  chunk_size = sz / chunk_count;
  for (int i = 0; i < chunk_count - 1; i++) {
    range_pairs[i].begin = data + i * chunk_size;
    range_pairs[i].end = range_pairs[i].begin + chunk_size - 1;
    // std::cout << "chunk #" << i << ", "
    //  << range_pairs[i].begin - data << " - "
    //  << range_pairs[i].end - data << ", count = "
    //  << range_pairs[i].end - range_pairs[i].begin << std::endl;
  }
  range_pairs[chunk_count - 1].begin = data + (chunk_count - 1) * chunk_size;
  range_pairs[chunk_count - 1].end = data + sz - 1;
  // std::cout << "chunk #" << chunk_count - 1 << ", "
  //  << range_pairs[chunk_count - 1].begin - data << " - "
  //  << range_pairs[chunk_count - 1].end - data << ", count = "
  //  << range_pairs[chunk_count - 1].end - range_pairs[chunk_count - 1].begin
  //  << std::endl;

  //::pthread_barrier_init(&process_barrier, nullptr, 16);
  //::pthread_barrier_init(&merger_level1_barrier, nullptr, 8);
  //::pthread_barrier_init(&merger_level2_barrier, nullptr, 4);
  //::pthread_barrier_init(&merger_level3_barrier, nullptr, 2);

  pthread_t workers[parallel_threads];
  ::pthread_create(&workers[0], nullptr, chunk_func_s, nullptr);
  for (unsigned int i = 1; i < parallel_threads - 1; i++) {
    ::pthread_create(&workers[i], nullptr, chunk_func_m,
                     (void *)&thd_params[i]);
  }
  ::pthread_create(&workers[parallel_threads - 1], nullptr, chunk_func_e,
                   nullptr);

  // wait for all threads to finish
  for (unsigned int i = 0; i < parallel_threads; i++) {
    ::pthread_join(workers[i], nullptr);
  }

  {
    // TimeElaped te("merge");
    final_result.reserve(max_distinct_groups);
    for (int slot = 0; slot < hash_buckets_size; slot++) {
      Info *h_l = &thd_entries[0].infos[slot];
      for (int thd = 1; thd < parallel_threads; thd++) {
        Info *h_r = &thd_entries[thd].infos[slot];
        __builtin_prefetch(&thd_entries[thd].infos[slot + 1], 0, 0);
        if (h_r->count != 0) {
          h_l->count += h_r->count;
          h_l->sum += h_r->sum;
          h_l->min = min(h_l->min, h_r->min);
          h_l->max = max(h_l->max, h_r->max);
        }
      }

      if (h_l->count > 0) {
        final_result.emplace_back(h_l);
      }
    }

    std::sort(final_result.begin(), final_result.end(),
              [](const Info *s1, const Info *s2) -> bool {
                return strncmp(s1->cn.ptr, s2->cn.ptr, s1->cn.len) < 0;
              });
  }

  // printf results
  {
    // TimeElaped te("print");
    ::setvbuf(stdout, std_out_buffer, _IOFBF, std_out_size);
    constexpr float z = float(1) / 10.0;
    std::printf("{");
    for (size_t n = 0; n < final_result.size(); n++) {
      if (n != final_result.size() - 1)
        [[likely]] {
          std::printf(
              "%.*s=%.1f/%.1f/%.1f, ", final_result[n]->cn.len,
              final_result[n]->cn.ptr, (float)final_result[n]->min * z,
              ((float)final_result[n]->sum / (float)final_result[n]->count) * z,
              (float)final_result[n]->max * z);
        }
      else
        [[unlikely]] {
          std::printf(
              "%.*s=%.1f/%.1f/%.1f", final_result[n]->cn.len,
              final_result[n]->cn.ptr, (float)final_result[n]->min * z,
              ((float)final_result[n]->sum / (float)final_result[n]->count) * z,
              (float)final_result[n]->max * z);
        }
    }
    std::printf("}\n");
    ::fflush_unlocked(stdout);
  }

  // clean-up
  // munmap((void *)data, sz);
  // close(fd);
  return EXIT_SUCCESS;
}
