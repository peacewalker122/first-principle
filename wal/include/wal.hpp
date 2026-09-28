#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace wal {

enum class RecordType { Update, Commit };

struct RedoInformation {
    std::vector<std::byte> payload;
    std::uint64_t offset{};
};

struct UndoInformation {
    std::uint64_t offset{};
    std::uint64_t size{};
};

struct Record {
    std::uint64_t lsn{}, txid{}, prevLsn{};
    RecordType recordType{RecordType::Update};
    std::uint64_t pageID{};
    RedoInformation redoInformation;
    UndoInformation undoInformation;
    std::vector<std::byte> beforeImage;
};

struct Page {
    std::uint64_t pageID{}, lsn{}, recoveredLSN{};
    std::vector<std::byte> data;
};

class Wal final {
public:
    explicit Wal(const std::filesystem::path& path);
    ~Wal();

    Wal(const Wal&) = delete;
    Wal& operator=(const Wal&) = delete;

    [[nodiscard]] std::uint64_t append(const Record& record);
    void replay(const std::function<void(const Record&)>& visitor) const;
    void recover(Page& page);

private:
    struct RecoveryPlan {
        std::vector<Record> page_records;
        std::vector<std::size_t> undo_indices;
        std::vector<std::size_t> redo_indices;
        std::uint64_t latest_lsn{};
        std::uint64_t page_lsn{};
    };

    [[nodiscard]] std::uint64_t scan(
        bool repair_tail,
        const std::function<void(Record&&)>& visitor) const;
    [[nodiscard]] RecoveryPlan analysis(const Page& page) const;
    void undo(Page& page, const RecoveryPlan& plan) const;
    void redo(Page& page, const RecoveryPlan& plan) const;

    int fd_{-1};
    std::uint64_t next_lsn_{1};
    std::unordered_map<std::uint64_t, std::uint64_t> previous_lsn_;
    std::unordered_set<std::uint64_t> committed_txids_;
    bool poisoned_{false};
};

} // namespace wal
