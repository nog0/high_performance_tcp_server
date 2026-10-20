#pragma once

#include <cstdint>

class TimerQueue;

// An opaque handle. Keeping a TimerId does not keep a timer alive.
class TimerId {
public:
    TimerId() = default;
    bool valid() const { return sequence_ != 0; }
    explicit operator bool() const { return valid(); }

    friend bool operator==(TimerId lhs, TimerId rhs) {
        return lhs.sequence_ == rhs.sequence_;
    }
    friend bool operator!=(TimerId lhs, TimerId rhs) { return !(lhs == rhs); }

private:
    friend class TimerQueue;
    explicit TimerId(std::uint64_t sequence) : sequence_(sequence) {}
    std::uint64_t sequence_ = 0;
};
