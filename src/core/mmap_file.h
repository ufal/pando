#pragma once

#include <string>
#include <cstddef>
#include <cstdint>
#include <vector>
#include <stdexcept>

namespace pando {

class MmapFile {
public:
    MmapFile() = default;
    ~MmapFile();

    MmapFile(MmapFile&& o) noexcept;
    MmapFile& operator=(MmapFile&& o) noexcept;
    MmapFile(const MmapFile&) = delete;
    MmapFile& operator=(const MmapFile&) = delete;

    static MmapFile open(const std::string& path, bool preload = false);

    /// Force mapped pages into RAM (madvise MADV_WILLNEED + sequential touch). Call after open() or use open(path, true).
    void preload();

    const void* data() const { return data_; }
    size_t size() const { return size_; }
    bool valid() const { return data_ != nullptr; }

    template<typename T>
    const T* as() const { return reinterpret_cast<const T*>(data_); }

    template<typename T>
    size_t count() const { return size_ / sizeof(T); }

private:
    void close();
    void* data_  = nullptr;
    size_t size_  = 0;
};

void write_file(const std::string& path, const void* data, size_t bytes);

template<typename T>
void write_vec(const std::string& path, const std::vector<T>& v) {
    write_file(path, v.data(), v.size() * sizeof(T));
}

void write_strings(const std::string& path,
                   const std::vector<std::string>& strs,
                   std::vector<int64_t>& offsets_out);

} // namespace pando
