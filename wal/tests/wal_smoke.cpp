#include "wal.hpp"

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <span>
#include <stdexcept>
#include <string_view>
#include <system_error>
#include <unistd.h>
#include <vector>

namespace {

struct TempFile {
  std::filesystem::path path;

  TempFile() {
    char pattern[] = "/tmp/wal-smoke-XXXXXX";
    const int fd = ::mkstemp(pattern);
    if (fd < 0) {
      throw std::system_error(errno, std::generic_category(), "mkstemp");
    }
    path = pattern;
    if (::close(fd) < 0) {
      throw std::system_error(errno, std::generic_category(), "close temp file");
    }
  }

  ~TempFile() {
    std::error_code ignored;
    std::filesystem::remove(path, ignored);
  }
};

std::span<const std::byte> bytes(std::string_view text) {
  return std::as_bytes(std::span<const char>{text.data(), text.size()});
}

std::vector<std::byte> byte_vector(std::string_view text) {
  const auto data = bytes(text);
  return {data.begin(), data.end()};
}

wal::Record update(std::uint64_t txid, std::uint64_t page_id,
                   std::uint64_t offset, std::string_view before,
                   std::string_view after) {
  wal::Record record{};
  record.txid = txid;
  record.recordType = wal::RecordType::Update;
  record.pageID = page_id;
  record.redoInformation.payload = byte_vector(after);
  record.redoInformation.offset = offset;
  record.undoInformation.offset = offset;
  record.undoInformation.size = before.size();
  record.beforeImage = byte_vector(before);
  return record;
}

wal::Record commit(std::uint64_t txid) {
  wal::Record record{};
  record.txid = txid;
  record.recordType = wal::RecordType::Commit;
  return record;
}

wal::Record with_chain(wal::Record record, std::uint64_t lsn,
                       std::uint64_t prev_lsn) {
  record.lsn = lsn;
  record.prevLsn = prev_lsn;
  return record;
}

bool same_record(const wal::Record &left, const wal::Record &right) {
  return left.lsn == right.lsn && left.txid == right.txid &&
         left.prevLsn == right.prevLsn &&
         left.recordType == right.recordType && left.pageID == right.pageID &&
         left.redoInformation.payload == right.redoInformation.payload &&
         left.redoInformation.offset == right.redoInformation.offset &&
         left.undoInformation.offset == right.undoInformation.offset &&
         left.undoInformation.size == right.undoInformation.size &&
         left.beforeImage == right.beforeImage;
}

void append(wal::Wal &log, const wal::Record &record,
            std::uint64_t expected_lsn) {
  REQUIRE(log.append(record) == expected_lsn);
}

void expect_page(const wal::Page &page, std::string_view data,
                 std::uint64_t lsn, std::uint64_t recovered_lsn) {
  CHECK(page.data == byte_vector(data));
  CHECK(page.lsn == lsn);
  CHECK(page.recoveredLSN == recovered_lsn);
}

void append_recovery_history(wal::Wal &log) {
  append(log, update(10, 1, 0, "00", "AA"), 1);
  append(log, commit(10), 2);
  append(log, update(20, 1, 0, "AA", "LL"), 3);
  append(log, update(20, 1, 0, "LL", "MM"), 4);
  append(log, update(30, 1, 0, "MM", "BB"), 5);
  append(log, commit(30), 6);
  append(log, update(40, 2, 2, "00", "XY"), 7);
  append(log, commit(40), 8);
}

} // namespace

TEST_CASE("new WAL writes version 2 header with zero checkpoint LSN") {
  TempFile file;
  {
    wal::Wal log{file.path};
  }

  std::ifstream input(file.path, std::ios::binary);
  std::array<char, 16> header{};
  input.read(header.data(), static_cast<std::streamsize>(header.size()));
  REQUIRE(input.gcount() == static_cast<std::streamsize>(header.size()));
  CHECK((std::string_view{header.data(), 8} == std::string_view{"WAL1\0\0\0\2", 8}));
  CHECK(std::all_of(header.begin() + 8, header.end(), [](char byte) { return byte == 0; }));
}

TEST_CASE("checkpoint persists the minimum page LSN and recovery accepts that boundary") {
  TempFile file;
  wal::Wal log{file.path};
  append(log, update(10, 1, 0, "0", "A"), 1);
  append(log, commit(10), 2);
  append(log, update(20, 2, 0, "0", "B"), 3);
  append(log, commit(20), 4);

  wal::Page first{};
  first.pageID = 1;
  first.lsn = 1;
  first.data = byte_vector("A");
  wal::Page second{};
  second.pageID = 2;
  second.lsn = 3;
  second.data = byte_vector("B");
  CHECK(log.checkpoint({first, second}) == 1);
  {
    wal::Wal reopened{file.path};
    wal::Page behind{};
    behind.pageID = 2;
    behind.data = byte_vector("0");
    CHECK_THROWS_AS(reopened.recover(behind), std::runtime_error);
    reopened.recover(first);
    expect_page(first, "A", 1, 4);
  }
}

TEST_CASE("checkpoint rejects pages behind the recorded checkpoint") {
  TempFile file;
  wal::Wal log{file.path};
  append(log, update(10, 1, 0, "0", "A"), 1);
  wal::Page current{};
  current.pageID = 1;
  current.lsn = 1;
  current.data = byte_vector("A");
  REQUIRE(log.checkpoint({current}) == 1);

  wal::Page behind{};
  behind.pageID = 1;
  behind.data = byte_vector("0");
  CHECK_THROWS_AS(log.recover(behind), std::runtime_error);
}

TEST_CASE("append after reopen preserves LSN and prevLsn chain") {
  TempFile file;
  const auto first = update(10, 1, 0, "0", "A");
  const auto second = update(20, 1, 0, "A", "B");
  const auto second_after_reopen = update(20, 1, 0, "B", "C");

  {
    wal::Wal log{file.path};
    append(log, first, 1);
    append(log, second, 2);
  }

  {
    wal::Wal log{file.path};
    append(log, second_after_reopen, 3);
    std::vector<wal::Record> records;
    log.replay([&](const wal::Record &record) { records.push_back(record); });

    REQUIRE(records.size() == 3);
    CHECK(same_record(records[0], with_chain(first, 1, 0)));
    CHECK(same_record(records[1], with_chain(second, 2, 0)));
    CHECK(same_record(records[2], with_chain(second_after_reopen, 3, 2)));
  }
}

TEST_CASE("reopen repairs torn tail before appending next record") {
  TempFile file;
  const auto update_record = update(10, 1, 0, "0", "A");
  const auto commit_record = commit(10);

  {
    wal::Wal log{file.path};
    append(log, update_record, 1);
  }

  {
    std::ofstream tail(file.path, std::ios::binary | std::ios::app);
    REQUIRE(tail.good());
    tail.write("\xa5\x5a", 2);
    tail.close();
    REQUIRE(tail.good());
  }

  {
    wal::Wal log{file.path};
    append(log, commit_record, 2);
    std::vector<wal::Record> records;
    log.replay([&](const wal::Record &record) { records.push_back(record); });

    REQUIRE(records.size() == 2);
    CHECK(same_record(records[0], with_chain(update_record, 1, 0)));
    CHECK(same_record(records[1], with_chain(commit_record, 2, 1)));
  }
}

TEST_CASE("replay preserves complete record fields") {
  TempFile file;
  const auto update_record = update(77, 9, 4, "before", "after!");
  const auto commit_record = commit(77);
  wal::Wal log{file.path};
  append(log, update_record, 1);
  append(log, commit_record, 2);

  std::vector<wal::Record> records;
  log.replay([&](const wal::Record &record) { records.push_back(record); });

  REQUIRE(records.size() == 2);
  CHECK(same_record(records[0], with_chain(update_record, 1, 0)));
  CHECK(same_record(records[1], with_chain(commit_record, 2, 1)));
}

TEST_CASE("recovery undoes loser updates and redoes committed updates") {
  TempFile file;
  wal::Wal log{file.path};
  append_recovery_history(log);

  wal::Page page{};
  page.pageID = 1;
  page.lsn = 4;
  page.data = byte_vector("MM00");
  log.recover(page);

  expect_page(page, "BB00", 5, 8);
}

TEST_CASE("recovery rebuilds page with committed winner already applied") {
  TempFile file;
  wal::Wal log{file.path};
  append_recovery_history(log);

  wal::Page page{};
  page.pageID = 1;
  page.lsn = 5;
  page.data = byte_vector("BB00");
  log.recover(page);

  expect_page(page, "BB00", 5, 8);
}

TEST_CASE("recovery redoes committed update when page is behind WAL") {
  TempFile file;
  wal::Wal log{file.path};
  append_recovery_history(log);

  wal::Page page{};
  page.pageID = 2;
  page.data = byte_vector("0000");
  log.recover(page);

  expect_page(page, "00XY", 7, 8);
}

TEST_CASE("recovery is idempotent") {
  TempFile file;
  wal::Wal log{file.path};
  append_recovery_history(log);

  wal::Page page{};
  page.pageID = 1;
  page.lsn = 4;
  page.data = byte_vector("MM00");
  log.recover(page);
  expect_page(page, "BB00", 5, 8);

  log.recover(page);
  expect_page(page, "BB00", 5, 8);
}

TEST_CASE("late commit redoes update after earlier recovery") {
  TempFile file;
  wal::Wal log{file.path};
  append(log, update(50, 3, 0, "00", "AA"), 1);

  wal::Page page{};
  page.pageID = 3;
  page.data = byte_vector("00");
  log.recover(page);
  expect_page(page, "00", 0, 1);

  append(log, commit(50), 2);
  log.recover(page);
  expect_page(page, "AA", 1, 2);

  log.recover(page);
  expect_page(page, "AA", 1, 2);
}

TEST_CASE("recovery rejects page ahead of WAL without mutation") {
  TempFile file;
  wal::Wal log{file.path};
  append(log, update(10, 2, 0, "00", "AA"), 1);

  wal::Page page{};
  page.pageID = 2;
  page.lsn = 2;
  page.recoveredLSN = 2;
  page.data = byte_vector("0000");

  CHECK_THROWS_AS(log.recover(page), std::runtime_error);
  expect_page(page, "0000", 2, 2);
}

TEST_CASE("WAL rejects checksum-corrupted complete frame") {
  TempFile file;
  {
    wal::Wal log{file.path};
    append(log, update(10, 1, 0, "00", "AA"), 1);
  }

  {
    std::fstream log(file.path,
                     std::ios::binary | std::ios::in | std::ios::out);
    REQUIRE(log.good());
    constexpr std::streamoff first_record_redo_offset = 16 + 16 + 52;
    log.seekg(first_record_redo_offset);
    char byte{};
    log.read(&byte, 1);
    REQUIRE(log.gcount() == 1);
    byte = static_cast<char>(byte ^ 1);
    log.seekp(first_record_redo_offset);
    log.write(&byte, 1);
    log.flush();
    REQUIRE(log.good());
  }

  CHECK_THROWS_AS(wal::Wal{file.path}, std::runtime_error);
}
