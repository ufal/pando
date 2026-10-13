#pragma once
// Writing posting lists (`.rev`) from postings produced in position order (P4.7c).
//
// Every builder of a `.rev` file has the same shape: count postings per key, take
// prefix sums (the `.rev.idx` offsets), then place each position of key k at the
// next free slot of k. Doing that through a writable mmap of the whole file writes
// each position at a random offset; on spinning / USB disks the dirty pages go back
// in random order and a 1.5B-token build stalled for hours.
//
// write_postings() instead processes key ranges whose postings fit a memory budget:
// per range it runs the scan once (sequential reads of the source), fills an
// ordinary buffer and appends it to the file. Same bytes as the mmap fill, written
// sequentially, bounded memory. PANDO_REV_BUILD=mmap selects the old fill (for
// differential tests); PANDO_INDEX_MEM_MB sets the budget per file (default 1024).

#include "core/types.h"

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <stdexcept>
#include <string>
#include <vector>

#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

namespace pando {
namespace postings_writer {

inline size_t budget_bytes() {
    static const size_t b = [] {
        size_t mb = 1024;
        if (const char* v = std::getenv("PANDO_INDEX_MEM_MB")) {
            try { mb = std::max<size_t>(16, std::stoull(v)); } catch (...) {}
        }
        return mb << 20;
    }();
    return b;
}

inline bool use_mmap() {
    static const bool m = [] {
        const char* v = std::getenv("PANDO_REV_BUILD");
        return v && std::string(v) == "mmap";
    }();
    return m;
}

inline void store(void* base, int64_t at, int width, CorpusPos pos) {
    switch (width) {
        case 2: static_cast<int16_t*>(base)[at] = static_cast<int16_t>(pos); break;
        case 4: static_cast<int32_t*>(base)[at] = static_cast<int32_t>(pos); break;
        default: static_cast<int64_t*>(base)[at] = static_cast<int64_t>(pos); break;
    }
}

/// Previous method: scattered fill through a writable mapping of the whole file.
template <typename Scan>
void write_mmap(const std::string& path, const std::vector<int64_t>& offsets, int width, Scan&& scan) {
    const size_t nkeys = offsets.size() - 1;
    const size_t bytes = static_cast<size_t>(offsets.back()) * static_cast<size_t>(width);
    const int fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) throw std::runtime_error("Cannot create " + path);
    if (bytes == 0) { ::close(fd); return; }
    if (::ftruncate(fd, static_cast<off_t>(bytes)) != 0) {
        ::close(fd);
        throw std::runtime_error("Cannot set size of " + path);
    }
    void* raw = ::mmap(nullptr, bytes, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (raw == MAP_FAILED) {
        ::close(fd);
        throw std::runtime_error("Cannot mmap " + path + " for writing");
    }
    std::vector<int64_t> cur(offsets.begin(), offsets.begin() + static_cast<std::ptrdiff_t>(nkeys));
    scan([&](size_t key, CorpusPos pos) { store(raw, cur[key]++, width, pos); });
    ::msync(raw, bytes, MS_SYNC);
    ::munmap(raw, bytes);
    ::close(fd);
}

/// Sequential fill by key ranges. `offsets` = prefix sums (size nkeys+1);
/// `scan(emit)` must call emit(key, pos) for every posting in ascending position
/// order (it is called once per key range).
template <typename Scan>
void write_ranges(const std::string& path, const std::vector<int64_t>& offsets, int width, Scan&& scan) {
    FILE* out = std::fopen(path.c_str(), "wb");
    if (!out) throw std::runtime_error("Cannot create " + path);
    const size_t nkeys = offsets.size() - 1;
    const size_t w = static_cast<size_t>(width);
    const size_t budget = budget_bytes();
    std::vector<char> buf;
    std::vector<int64_t> cur;
    size_t lo = 0;
    while (lo < nkeys) {
        size_t hi = lo + 1;           // [lo, hi): as many keys as fit the budget, at least one
        while (hi < nkeys && static_cast<size_t>(offsets[hi + 1] - offsets[lo]) * w <= budget) ++hi;
        const int64_t base = offsets[lo];
        const int64_t n = offsets[hi] - base;
        if (n > 0) {
            buf.assign(static_cast<size_t>(n) * w, 0);
            cur.assign(offsets.begin() + static_cast<std::ptrdiff_t>(lo),
                       offsets.begin() + static_cast<std::ptrdiff_t>(hi));
            for (auto& c : cur) c -= base;
            void* b = buf.data();
            if (lo == 0 && hi == nkeys)        // everything fits: no range test per posting
                scan([&](size_t key, CorpusPos pos) { store(b, cur[key]++, width, pos); });
            else
                scan([&](size_t key, CorpusPos pos) {
                    if (key >= lo && key < hi) store(b, cur[key - lo]++, width, pos);
                });
            if (std::fwrite(buf.data(), 1, buf.size(), out) != buf.size()) {
                std::fclose(out);
                throw std::runtime_error("Cannot write " + path);
            }
        }
        lo = hi;
    }
    if (std::fclose(out) != 0) throw std::runtime_error("Cannot write " + path);
}

/// Writes `path` (the `.rev`) with either method (PANDO_REV_BUILD).
template <typename Scan>
void write(const std::string& path, const std::vector<int64_t>& offsets, int width, Scan&& scan) {
    if (use_mmap()) write_mmap(path, offsets, width, scan);
    else write_ranges(path, offsets, width, scan);
}

}  // namespace postings_writer
}  // namespace pando
