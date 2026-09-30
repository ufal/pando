#include "core/mmap_file.h"

#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cerrno>
#include <cstring>
#include <fstream>

namespace pando {

MmapFile::~MmapFile() { close(); }

MmapFile::MmapFile(MmapFile&& o) noexcept
    : data_(o.data_), size_(o.size_) {
    o.data_ = nullptr;
    o.size_ = 0;
}

MmapFile& MmapFile::operator=(MmapFile&& o) noexcept {
    if (this != &o) {
        close();
        data_ = o.data_;  size_ = o.size_;
        o.data_ = nullptr; o.size_ = 0;
    }
    return *this;
}

void MmapFile::close() {
    if (data_) { munmap(data_, size_); data_ = nullptr; }
    size_ = 0;
}

MmapFile MmapFile::open(const std::string& path, bool preload) {
    // The descriptor is closed as soon as the file is mapped (a mapping stays valid
    // without it): an open corpus holds no file descriptors, so many handles fit
    // under a low `ulimit -n` (macOS: 256).
    MmapFile f;
    const int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0)
        throw std::runtime_error("Cannot open " + path + ": " + strerror(errno));

    struct stat st;
    if (fstat(fd, &st) < 0) {
        const int e = errno;
        ::close(fd);
        throw std::runtime_error("Cannot stat " + path + ": " + strerror(e));
    }
    f.size_ = static_cast<size_t>(st.st_size);
    if (f.size_ == 0) {
        ::close(fd);
        return f;
    }

    f.data_ = mmap(nullptr, f.size_, PROT_READ, MAP_PRIVATE, fd, 0);
    const int e = errno;
    ::close(fd);
    if (f.data_ == MAP_FAILED) {
        f.data_ = nullptr;
        f.size_ = 0;
        throw std::runtime_error("Cannot mmap " + path + ": " + strerror(e));
    }
    if (preload)
        f.preload();
    return f;
}

void MmapFile::preload() {
    if (!data_ || size_ == 0) return;
#ifdef MADV_WILLNEED
    madvise(data_, size_, MADV_WILLNEED);
#endif
    const long page_size = sysconf(_SC_PAGESIZE);
    const size_t step = (page_size > 0 && static_cast<size_t>(page_size) <= size_)
        ? static_cast<size_t>(page_size) : 4096u;
    const char* p = static_cast<const char*>(data_);
    for (size_t i = 0; i < size_; i += step)
        (void)*reinterpret_cast<const volatile char*>(p + i);
}

void write_file(const std::string& path, const void* data, size_t bytes) {
    std::ofstream out(path, std::ios::binary);
    if (!out) throw std::runtime_error("Cannot create " + path);
    if (bytes > 0)
        out.write(static_cast<const char*>(data), static_cast<std::streamsize>(bytes));
    if (!out) throw std::runtime_error("Write failed: " + path);
}

void write_strings(const std::string& path,
                   const std::vector<std::string>& strs,
                   std::vector<int64_t>& offsets_out) {
    offsets_out.clear();
    offsets_out.reserve(strs.size() + 1);

    std::ofstream out(path, std::ios::binary);
    if (!out) throw std::runtime_error("Cannot create " + path);

    int64_t offset = 0;
    for (const auto& s : strs) {
        offsets_out.push_back(offset);
        out.write(s.c_str(), static_cast<std::streamsize>(s.size() + 1));
        offset += static_cast<int64_t>(s.size()) + 1;
    }
    offsets_out.push_back(offset);
}

} // namespace pando
