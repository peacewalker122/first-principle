#include "wal.hpp"

#include <cerrno>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <unistd.h>
#include <vector>

namespace {

struct Record {
    std::uint64_t lsn;
    std::string payload;

    friend bool operator==(const Record&, const Record&) = default;
};

struct TempFile {
    std::filesystem::path path;

    ~TempFile() {
        std::error_code ignored;
        std::filesystem::remove(path, ignored);
    }
};

void check(bool condition, const char* message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}

std::span<const std::byte> bytes(std::string_view text) {
    return std::as_bytes(std::span<const char>{text.data(), text.size()});
}

void run() {
    char pattern[] = "/tmp/wal-smoke-XXXXXX";
    const int fd = ::mkstemp(pattern);
    if (fd < 0) {
        throw std::system_error(errno, std::generic_category(), "mkstemp");
    }
    TempFile file{pattern};
    if (::close(fd) < 0) {
        throw std::system_error(errno, std::generic_category(), "close temp file");
    }

    {
        wal::Wal log{file.path};
        check(log.append(bytes("set alpha 1")) == 1, "first LSN must be 1");
        check(log.append(bytes("set beta 2")) == 2, "second LSN must be 2");
    }

    {
        std::ofstream tail(file.path, std::ios::binary | std::ios::app);
        check(tail.good(), "could not open WAL to simulate torn tail");
        tail.write("\xa5\x5a", 2);
        tail.close();
        check(tail.good(), "could not write torn tail");
    }

    {
        wal::Wal log{file.path};
        check(log.append(bytes("set gamma 3")) == 3, "recovery must preserve next LSN");

        std::vector<Record> actual;
        log.replay([&](std::uint64_t lsn, std::span<const std::byte> payload) {
            actual.push_back({
                lsn,
                std::string(reinterpret_cast<const char*>(payload.data()), payload.size()),
            });
        });
        const std::vector<Record> expected{
            {1, "set alpha 1"},
            {2, "set beta 2"},
            {3, "set gamma 3"},
        };
        check(actual == expected, "replay must return intact records in LSN order");
    }

    {
        std::fstream log(file.path, std::ios::binary | std::ios::in | std::ios::out);
        check(log.good(), "could not open WAL to verify checksum");
        log.seekg(24);
        char byte{};
        log.read(&byte, 1);
        check(log.gcount() == 1, "could not read WAL payload for checksum test");
        byte = static_cast<char>(byte ^ 1);
        log.seekp(24);
        log.write(&byte, 1);
        log.flush();
        check(log.good(), "could not corrupt WAL payload for checksum test");
    }

    bool rejected_corruption = false;
    try {
        wal::Wal corrupted{file.path};
    } catch (const std::runtime_error&) {
        rejected_corruption = true;
    }
    check(rejected_corruption, "recovery must reject a checksum mismatch");
}

} // namespace

int main() {
    try {
        run();
        std::cout << "wal smoke: ok\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "wal smoke: " << error.what() << '\n';
        return 1;
    }
}
