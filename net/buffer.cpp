#include "buffer.h"

#include <algorithm>
#include <cstring>
#include <stdexcept>
#include <utility>

Buffer::Buffer(std::size_t initial_size)
    : buffer_(std::max(initial_size, std::size_t{1})) {}

Buffer::Buffer(Buffer&& other) noexcept
    : buffer_(std::move(other.buffer_)),
      reader_index_(std::exchange(other.reader_index_, 0)),
      writer_index_(std::exchange(other.writer_index_, 0)) {}

Buffer& Buffer::operator=(Buffer&& other) noexcept {
    if (this != &other) {
        buffer_ = std::move(other.buffer_);
        reader_index_ = std::exchange(other.reader_index_, 0);
        writer_index_ = std::exchange(other.writer_index_, 0);
    }
    return *this;
}

const char* Buffer::peek() const {
    return reader_index_ == 0 ? buffer_.data() : buffer_.data() + reader_index_;
}

char* Buffer::beginWrite() {
    return writer_index_ == 0 ? buffer_.data() : buffer_.data() + writer_index_;
}

const char* Buffer::findEOL() const {
    if (empty()) return nullptr;
    return static_cast<const char*>(std::memchr(peek(), '\n', readableBytes()));
}

void Buffer::append(const char* data, std::size_t length) {
    if (length == 0) return;
    if (!data) throw std::invalid_argument("Buffer::append: null data");
    ensureWritableBytes(length);
    std::memcpy(beginWrite(), data, length);
    hasWritten(length);
}

void Buffer::ensureWritableBytes(std::size_t length) {
    if (length <= writableBytes()) return;

    const std::size_t readable = readableBytes();
    if (length > buffer_.max_size() - readable) {
        throw std::length_error("Buffer: requested size is too large");
    }

    // Reuse the space before the reader before allocating more storage.
    if (reader_index_ != 0) {
        if (readable != 0) std::memmove(buffer_.data(), peek(), readable);
        reader_index_ = 0;
        writer_index_ = readable;
    }

    if (length > writableBytes()) {
        const std::size_t required = writer_index_ + length;
        const std::size_t grown = buffer_.size() > buffer_.max_size() / 2
                                     ? buffer_.max_size()
                                     : buffer_.size() * 2;
        buffer_.resize(std::max(required, grown));
    }
}

void Buffer::hasWritten(std::size_t length) {
    if (length > writableBytes()) {
        throw std::out_of_range("Buffer::hasWritten: not enough writable space");
    }
    writer_index_ += length;
}

void Buffer::retrieve(std::size_t length) {
    if (length > readableBytes()) {
        throw std::out_of_range("Buffer::retrieve: not enough readable bytes");
    }
    if (length == readableBytes()) {
        retrieveAll();
    } else {
        reader_index_ += length;
    }
}

void Buffer::retrieveAll() {
    reader_index_ = 0;
    writer_index_ = 0;
}

std::string Buffer::retrieveAsString(std::size_t length) {
    if (length > readableBytes()) {
        throw std::out_of_range("Buffer::retrieveAsString: not enough readable bytes");
    }
    if (length == 0) return {};
    std::string result(peek(), length);
    retrieve(length);
    return result;
}
