#pragma once

#include <cstddef>
#include <string>
#include <string_view>
#include <vector>

// A contiguous byte buffer. Each connection uses it only on its EventLoop thread.
// Consuming bytes advances the read index; compaction happens only when needed.
class Buffer {
public:
    static constexpr std::size_t kInitialSize = 1024;

    explicit Buffer(std::size_t initial_size = kInitialSize);
    Buffer(const Buffer&) = default;
    Buffer& operator=(const Buffer&) = default;
    Buffer(Buffer&& other) noexcept;
    Buffer& operator=(Buffer&& other) noexcept;

    std::size_t readableBytes() const { return writer_index_ - reader_index_; }
    std::size_t writableBytes() const { return buffer_.size() - writer_index_; }
    bool empty() const { return readableBytes() == 0; }

    // Storage pointers may be invalidated by compaction, growth, or assignment.
    const char* peek() const;
    char* beginWrite();
    const char* findEOL() const;

    // data must not refer to this buffer's own storage.
    void append(const char* data, std::size_t length);
    void append(std::string_view data) { append(data.data(), data.size()); }

    // Reserve writable space, then commit exactly the bytes written by recv().
    void ensureWritableBytes(std::size_t length);
    void hasWritten(std::size_t length);

    void retrieve(std::size_t length);
    void retrieveAll();
    std::string retrieveAsString(std::size_t length);
    std::string retrieveAllAsString() { return retrieveAsString(readableBytes()); }

private:
    std::vector<char> buffer_;
    std::size_t reader_index_ = 0;
    std::size_t writer_index_ = 0;
};
