#ifndef FLEXQL_BUFFERED_SOCKET_H
#define FLEXQL_BUFFERED_SOCKET_H

#include <string>
#include <sys/socket.h>
#include <unistd.h>
#include <vector>

class BufferedSocket {
public:
    explicit BufferedSocket(int fd, size_t capacity = 64 * 1024)
        : fd_(fd), buffer_(capacity), start_(0), end_(0) {}

    bool read_exact(std::string &out, size_t bytes) {
        out.clear();
        out.reserve(bytes);

        while (bytes > 0) {
            if (start_ == end_ && bytes >= buffer_.size()) {
                std::string chunk(bytes, '\0');
                size_t received = 0;
                while (received < bytes) {
                    ssize_t rc = ::recv(fd_, chunk.data() + received, bytes - received, 0);
                    if (rc <= 0) {
                        return false;
                    }
                    received += static_cast<size_t>(rc);
                }
                out.append(chunk);
                return true;
            }

            if (start_ == end_ && !fill()) {
                return false;
            }

            size_t available = end_ - start_;
            size_t take = (available < bytes) ? available : bytes;
            out.append(buffer_.data() + start_, take);
            start_ += take;
            bytes -= take;
        }
        return true;
    }

    bool read_line(std::string &line) {
        line.clear();
        while (true) {
            if (start_ == end_ && !fill()) {
                return false;
            }

            for (size_t i = start_; i < end_; ++i) {
                if (buffer_[i] == '\n') {
                    line.append(buffer_.data() + start_, i - start_);
                    start_ = i + 1;
                    return true;
                }
            }

            line.append(buffer_.data() + start_, end_ - start_);
            start_ = end_;
        }
    }

private:
    bool fill() {
        start_ = 0;
        ssize_t rc = ::recv(fd_, buffer_.data(), buffer_.size(), 0);
        if (rc <= 0) {
            end_ = 0;
            return false;
        }
        end_ = static_cast<size_t>(rc);
        return true;
    }

    int fd_;
    std::vector<char> buffer_;
    size_t start_;
    size_t end_;
};

#endif
