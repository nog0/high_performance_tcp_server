#include <queue>
#include <mutex>
#include <condition_variable>
#include <cstddef>
#include <stdexcept>
#include <utility>


template<typename T>
class blocking_queue {
public:
    explicit blocking_queue(std::size_t capacity) : capacity_(capacity) {
        if (capacity_ == 0) {
            throw std::invalid_argument("blocking_queue capacity must be positive");
        }
    }

    //放入队列元素，若队列已满则阻塞
    void put(T task){
        std::unique_lock<std::mutex> lock(mutex_);
        not_full_.wait(lock, [this] { return queue_.size() < capacity_; });
        queue_.push(std::move(task));
        lock.unlock();
        not_empty_.notify_all();
    };


    //去除队列元素，若队列为空则阻塞
    T take(){
        std::unique_lock<std::mutex> lock(mutex_);
        not_empty_.wait(lock, [this] { return !queue_.empty(); });
        T task = std::move(queue_.front());
        queue_.pop();
        lock.unlock();
        not_full_.notify_all();
        return task;
    };
    

private:
    std::queue<T> queue_;
    std::mutex mutex_;
    std::condition_variable not_empty_;
    std::condition_variable not_full_;
    std::size_t capacity_;
};