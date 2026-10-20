#pragma once

#include <atomic>
#include <cstddef>
#include <functional>
#include <thread>
#include <vector>

#include "blocking_queue.h"


class thread_pool {
public:
    // 创建线程池
    thread_pool(std::size_t thread_count, std::size_t queue_capacity);

    // 添加任务
    void submit(std::function<void()> task);

    //通知线程退出，并等待所有线程结束
    void stop();

    //真正创建并启动工作线程
    void start();

    // 销毁线程池
    ~thread_pool();

private:
    // 工作线程执行的循环
    void worker_loop();

private:
    std::vector<std::thread> workers_;
    blocking_queue<std::function<void()>> tasks_;
    std::size_t thread_count_;

    std::atomic<bool> stop_{false};
};