#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <span>

namespace wal {
// Public API; POSIX I/O and the on-disk format are implemented in src/wal.cpp.

class Wal final {
public:
    using Visitor = std::function<void(std::uint64_t, std::span<const std::byte>)>;

    // Single-writer POSIX WAL. Successful append means the record was synced.
    explicit Wal(const std::filesystem::path& path);
    ~Wal();

    Wal(const Wal&) = delete;
    Wal& operator=(const Wal&) = delete;

    [[nodiscard]] std::uint64_t append(std::span<const std::byte> payload);
    void replay(const Visitor& visitor) const;

private:
    [[nodiscard]] std::uint64_t scan(bool repair_tail, const Visitor& visitor) const;

    int fd_{-1};
    std::uint64_t next_lsn_{1};
    bool poisoned_{false};
};

} // namespace wal
