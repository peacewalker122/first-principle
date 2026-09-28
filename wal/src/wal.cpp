#include "wal.hpp"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdint>
#include <fcntl.h>
#include <limits>
#include <span>
#include <stdexcept>
#include <sys/stat.h>
#include <system_error>
#include <unordered_map>
#include <unordered_set>
#include <unistd.h>
#include <utility>
#include <vector>
namespace wal {
namespace {

// Eight-byte file header: "WAL1" magic followed by big-endian version 1.
constexpr std::array<std::byte, 8> kFileHeader{
    std::byte{0x57}, std::byte{0x41}, std::byte{0x4c}, std::byte{0x31},
    std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, std::byte{0x01},
};
constexpr std::size_t kFrameHeaderSize = 16;
constexpr std::size_t kRecordPayloadHeaderSize = 52;
constexpr std::uint32_t kMaxRecordSize = 64U * 1024U * 1024U;
constexpr std::uint64_t kMaxUpdateSize =
    (kMaxRecordSize - kRecordPayloadHeaderSize) / 2;

[[noreturn]] void throw_errno(const char *operation) {
  throw std::system_error(errno, std::generic_category(), operation);
}

void write_all(int fd, std::span<const std::byte> bytes) {
  while (!bytes.empty()) {
    const auto written = ::write(fd, bytes.data(), bytes.size());
    if (written < 0) {
      if (errno == EINTR) {
        continue;
      }
      throw_errno("write WAL");
    }
    if (written == 0) {
      throw std::system_error(std::make_error_code(std::errc::io_error),
                              "write WAL");
    }
    bytes = bytes.subspan(static_cast<std::size_t>(written));
  }
}

bool read_exact_at(int fd, off_t offset, std::span<std::byte> bytes) {
  std::size_t received = 0;
  while (received < bytes.size()) {
    const auto count =
        ::pread(fd, bytes.data() + received, bytes.size() - received,
                offset + static_cast<off_t>(received));
    if (count == 0) {
      return false;
    }
    if (count < 0) {
      if (errno == EINTR) {
        continue;
      }
      throw_errno("read WAL");
    }
    received += static_cast<std::size_t>(count);
  }
  return true;
}

void sync_data(int fd) {
  int result;
  do {
    result = ::fdatasync(fd);
  } while (result < 0 && errno == EINTR);
  if (result < 0) {
    throw_errno("fdatasync WAL");
  }
}

void sync_parent_directory(const std::filesystem::path &path) {
  auto parent = path.parent_path();
  if (parent.empty()) {
    parent = ".";
  }
  const int directory_fd =
      ::open(parent.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
  if (directory_fd < 0) {
    throw_errno("open WAL directory");
  }

  int result;
  do {
    result = ::fsync(directory_fd);
  } while (result < 0 && errno == EINTR);
  const int error = errno;
  (void)::close(directory_fd);
  if (result < 0) {
    throw std::system_error(error, std::generic_category(),
                            "fsync WAL directory");
  }
}

void store_u32(std::span<std::byte> output, std::uint32_t value) {
  for (unsigned int i = 0; i < 4; ++i) {
    output[i] = static_cast<std::byte>((value >> (i * 8U)) & 0xffU);
  }
}

std::uint32_t load_u32(std::span<const std::byte> input) {
  std::uint32_t value = 0;
  for (unsigned int i = 0; i < 4; ++i) {
    value |= static_cast<std::uint32_t>(std::to_integer<std::uint8_t>(input[i]))
             << (i * 8U);
  }
  return value;
}

void store_u64(std::span<std::byte> output, std::uint64_t value) {
  for (unsigned int i = 0; i < 8; ++i) {
    output[i] = static_cast<std::byte>((value >> (i * 8U)) & 0xffU);
  }
}

std::uint64_t load_u64(std::span<const std::byte> input) {
  std::uint64_t value = 0;
  for (unsigned int i = 0; i < 8; ++i) {
    value |= static_cast<std::uint64_t>(std::to_integer<std::uint8_t>(input[i]))
             << (i * 8U);
  }
  return value;
}

std::uint32_t update_crc32(std::uint32_t crc,
                           std::span<const std::byte> bytes) {
  for (const auto byte : bytes) {
    crc ^= std::to_integer<std::uint8_t>(byte);
    for (unsigned int bit = 0; bit < 8; ++bit) {
      crc = (crc >> 1U) ^ (0xedb88320U & (0U - (crc & 1U)));
    }
  }
  return crc;
}

std::uint32_t checksum(std::span<const std::byte> header,
                       std::span<const std::byte> payload) {
  auto crc = update_crc32(0xffffffffU, header);
  crc = update_crc32(crc, payload);
  return ~crc;
}

void validate_record_shape(const Record& record, bool from_disk) {
  const auto invalid = [from_disk](const char* message) {
    if (from_disk) {
      throw std::runtime_error(message);
    }
    throw std::invalid_argument(message);
  };

  if (record.txid == 0) {
    invalid("WAL transaction ID must be nonzero");
  }

  switch (record.recordType) {
  case RecordType::Update: {
    if (record.pageID == 0) {
      invalid("WAL update page ID must be nonzero");
    }
    if (record.redoInformation.payload.size() > kMaxUpdateSize) {
      if (from_disk) {
        throw std::runtime_error("WAL record exceeds 64 MiB");
      }
      throw std::length_error("WAL record exceeds 64 MiB");
    }
    const auto size =
        static_cast<std::uint64_t>(record.redoInformation.payload.size());
    if (size == 0 || record.undoInformation.size != size ||
        record.beforeImage.size() != size ||
        record.redoInformation.offset != record.undoInformation.offset) {
      invalid("WAL update images and ranges are inconsistent");
    }
    if (size > std::numeric_limits<std::uint64_t>::max() -
                   record.redoInformation.offset) {
      invalid("WAL update range overflows");
    }
    break;
  }
  case RecordType::Commit:
    if (record.pageID != 0 || !record.redoInformation.payload.empty() ||
        record.redoInformation.offset != 0 ||
        record.undoInformation.offset != 0 ||
        record.undoInformation.size != 0 || !record.beforeImage.empty()) {
      invalid("WAL commit record contains page data");
    }
    break;
  default:
    invalid("WAL record type is invalid");
  }
}

std::vector<std::byte> encode_record_payload(const Record& record,
                                             std::uint64_t prev_lsn) {
  const auto size = static_cast<std::size_t>(record.undoInformation.size);
  std::vector<std::byte> payload(kRecordPayloadHeaderSize + 2 * size);
  const auto bytes = std::span<std::byte>{payload};
  store_u32(bytes.first(4), static_cast<std::uint32_t>(record.recordType));
  store_u64(bytes.subspan(4, 8), record.txid);
  store_u64(bytes.subspan(12, 8), prev_lsn);
  store_u64(bytes.subspan(20, 8), record.pageID);
  store_u64(bytes.subspan(28, 8), record.redoInformation.offset);
  store_u64(bytes.subspan(36, 8), record.undoInformation.offset);
  store_u64(bytes.subspan(44, 8), record.undoInformation.size);
  std::copy(record.redoInformation.payload.begin(),
            record.redoInformation.payload.end(),
            payload.begin() + kRecordPayloadHeaderSize);
  std::copy(record.beforeImage.begin(), record.beforeImage.end(),
            bytes.subspan(kRecordPayloadHeaderSize + size).begin());
  return payload;
}

Record decode_record(std::uint64_t lsn, std::span<const std::byte> payload) {
  if (payload.size() < kRecordPayloadHeaderSize) {
    throw std::runtime_error("WAL record payload is too short");
  }

  Record record{};
  record.lsn = lsn;
  const auto type = load_u32(payload.first(4));
  if (type == static_cast<std::uint32_t>(RecordType::Update)) {
    record.recordType = RecordType::Update;
  } else if (type == static_cast<std::uint32_t>(RecordType::Commit)) {
    record.recordType = RecordType::Commit;
  } else {
    throw std::runtime_error("WAL record type is invalid");
  }
  record.txid = load_u64(payload.subspan(4, 8));
  record.prevLsn = load_u64(payload.subspan(12, 8));
  record.pageID = load_u64(payload.subspan(20, 8));
  record.redoInformation.offset = load_u64(payload.subspan(28, 8));
  record.undoInformation.offset = load_u64(payload.subspan(36, 8));
  record.undoInformation.size = load_u64(payload.subspan(44, 8));

  if (record.undoInformation.size > kMaxUpdateSize) {
    throw std::runtime_error("WAL record exceeds 64 MiB");
  }
  if (record.recordType == RecordType::Commit &&
      record.undoInformation.size != 0) {
    throw std::runtime_error("WAL commit record contains page data");
  }
  const auto size =
      static_cast<std::size_t>(record.undoInformation.size);
  if (payload.size() != kRecordPayloadHeaderSize + 2 * size) {
    throw std::runtime_error("WAL record payload length is invalid");
  }
  record.redoInformation.payload.resize(size);
  record.beforeImage.resize(size);
  const auto redo_bytes = payload.subspan(kRecordPayloadHeaderSize, size);
  const auto before_bytes =
      payload.subspan(kRecordPayloadHeaderSize + size, size);
  std::copy(redo_bytes.begin(), redo_bytes.end(),
            record.redoInformation.payload.begin());
  std::copy(before_bytes.begin(), before_bytes.end(),
            record.beforeImage.begin());
  validate_record_shape(record, true);
  return record;
}

} // namespace

Wal::Wal(const std::filesystem::path& path) {
  fd_ = ::open(path.c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0644);
  if (fd_ < 0) {
    throw_errno("open WAL");
  }

  try {
    struct stat status{};
    if (::fstat(fd_, &status) < 0) {
      throw_errno("fstat WAL");
    }
    if (!S_ISREG(status.st_mode)) {
      throw std::runtime_error("WAL path is not a regular file");
    }
    if (status.st_size < 0) {
      throw std::runtime_error("WAL has invalid file size");
    }

    if (static_cast<std::uint64_t>(status.st_size) < kFileHeader.size()) {
      if (::ftruncate(fd_, 0) < 0 || ::lseek(fd_, 0, SEEK_SET) < 0) {
        throw_errno("reset incomplete WAL header");
      }
      write_all(fd_, kFileHeader);
      sync_data(fd_);
      sync_parent_directory(path);
    }
    next_lsn_ = scan(true, [&](Record&& record) {
      previous_lsn_[record.txid] = record.lsn;
      if (record.recordType == RecordType::Commit) {
        committed_txids_.insert(record.txid);
      }
    });
  } catch (...) {
    (void)::close(fd_);
    fd_ = -1;
    throw;
  }
}

Wal::~Wal() {
  if (fd_ >= 0) {
    (void)::close(fd_);
  }
}

std::uint64_t Wal::append(const Record& input) {
  if (poisoned_) {
    throw std::runtime_error(
        "WAL is unusable after a failed append; reopen it");
  }
  if (next_lsn_ == 0) {
    throw std::overflow_error("WAL LSN space exhausted");
  }
  if (input.txid != 0 && committed_txids_.contains(input.txid)) {
    throw std::invalid_argument("WAL transaction ID is already committed");
  }

  const auto previous = previous_lsn_.find(input.txid);
  const auto prev_lsn =
      previous == previous_lsn_.end() ? 0 : previous->second;
  validate_record_shape(input, false);
  auto payload = encode_record_payload(input, prev_lsn);

  std::array<std::byte, kFrameHeaderSize> header{};
  auto header_span = std::span<std::byte>{header};
  store_u64(header_span.first(8), next_lsn_);
  store_u32(header_span.subspan(8, 4),
            static_cast<std::uint32_t>(payload.size()));
  store_u32(header_span.subspan(12, 4),
            checksum(std::span<const std::byte>{header.data(), 12}, payload));

  auto [previous_entry, inserted] =
      previous_lsn_.try_emplace(input.txid, prev_lsn);
  (void)inserted;
  if (input.recordType == RecordType::Commit) {
    committed_txids_.insert(input.txid);
  }

  const auto lsn = next_lsn_;
  try {
    write_all(fd_, header);
    write_all(fd_, payload);
    sync_data(fd_);
  } catch (...) {
    poisoned_ = true;
    throw;
  }

  previous_entry->second = lsn;
  next_lsn_ = lsn == std::numeric_limits<std::uint64_t>::max() ? 0 : lsn + 1;
  return lsn;
}

void Wal::replay(const std::function<void(const Record&)>& visitor) const {
  if (!visitor) {
    throw std::invalid_argument("WAL replay requires a visitor");
  }
  (void)scan(false, [&](Record&& record) { visitor(record); });
}

void Wal::recover(Page& page) {
  const auto plan = analysis(page);
  undo(page, plan);
  redo(page, plan);
  page.lsn = plan.page_lsn;
  page.recoveredLSN = plan.latest_lsn;
}

std::uint64_t Wal::scan(
    bool repair_tail,
    const std::function<void(Record&&)>& visitor) const {
  struct stat status{};
  if (::fstat(fd_, &status) < 0) {
    throw_errno("fstat WAL");
  }
  if (status.st_size < 0 ||
      static_cast<std::uint64_t>(status.st_size) < kFileHeader.size()) {
    throw std::runtime_error("WAL header is incomplete");
  }

  std::array<std::byte, kFileHeader.size()> file_header{};
  if (!read_exact_at(fd_, 0, file_header)) {
    throw std::runtime_error("WAL header is incomplete");
  }
  if (file_header != kFileHeader) {
    throw std::runtime_error("invalid WAL file header");
  }

  const auto file_size = static_cast<std::uint64_t>(status.st_size);
  auto offset = static_cast<std::uint64_t>(kFileHeader.size());
  std::uint64_t expected_lsn = 1;
  bool incomplete_tail = false;
  std::vector<std::byte> payload;
  std::unordered_map<std::uint64_t, std::uint64_t> previous_by_tx;
  std::unordered_set<std::uint64_t> committed_txids;

  while (offset < file_size) {
    const auto remaining = file_size - offset;
    if (remaining < kFrameHeaderSize) {
      incomplete_tail = true;
      break;
    }

    std::array<std::byte, kFrameHeaderSize> header{};
    if (!read_exact_at(fd_, static_cast<off_t>(offset), header)) {
      throw std::runtime_error("WAL changed while being read");
    }
    const auto header_span = std::span<const std::byte>{header};
    const auto lsn = load_u64(header_span.first(8));
    const auto length = load_u32(header_span.subspan(8, 4));
    const auto stored_crc = load_u32(header_span.subspan(12, 4));
    if (length > kMaxRecordSize) {
      throw std::runtime_error("WAL record length exceeds 64 MiB");
    }
    if (expected_lsn == 0 || lsn != expected_lsn) {
      throw std::runtime_error("WAL LSN sequence is invalid");
    }

    const auto frame_size =
        static_cast<std::uint64_t>(kFrameHeaderSize) + length;
    if (remaining < frame_size) {
      incomplete_tail = true;
      break;
    }

    payload.resize(length);
    if (!read_exact_at(fd_, static_cast<off_t>(offset + kFrameHeaderSize),
                       payload)) {
      throw std::runtime_error("WAL changed while being read");
    }
    if (checksum(header_span.first(12), payload) != stored_crc) {
      throw std::runtime_error("WAL record checksum mismatch");
    }

    auto record = decode_record(lsn, payload);
    const auto previous = previous_by_tx.find(record.txid);
    const auto expected_previous =
        previous == previous_by_tx.end() ? 0 : previous->second;
    if (record.prevLsn != expected_previous) {
      throw std::runtime_error("WAL transaction previous-LSN chain is invalid");
    }
    if (committed_txids.contains(record.txid)) {
      throw std::runtime_error("WAL record follows transaction commit");
    }
    previous_by_tx[record.txid] = lsn;
    if (record.recordType == RecordType::Commit) {
      committed_txids.insert(record.txid);
    }
    if (visitor) {
      visitor(std::move(record));
    }

    expected_lsn =
        lsn == std::numeric_limits<std::uint64_t>::max() ? 0 : lsn + 1;
    offset += frame_size;
  }

  if (incomplete_tail) {
    if (!repair_tail) {
      throw std::runtime_error(
          "WAL has an incomplete tail; reopen to recover it");
    }
    if (::ftruncate(fd_, static_cast<off_t>(offset)) < 0) {
      throw_errno("truncate incomplete WAL tail");
    }
    sync_data(fd_);
  }

  if (repair_tail && ::lseek(fd_, static_cast<off_t>(offset), SEEK_SET) < 0) {
    throw_errno("seek WAL end");
  }
  return expected_lsn;
}

Wal::RecoveryPlan Wal::analysis(const Page& page) const {
  if (page.pageID == 0) {
    throw std::invalid_argument("WAL recovery page ID must be nonzero");
  }

  RecoveryPlan plan{};
  std::unordered_map<std::uint64_t, std::uint64_t> commit_lsn_by_tx;
  bool found_page_lsn = page.lsn == 0;
  const auto next_lsn = scan(false, [&](Record&& record) {
    if (record.recordType == RecordType::Commit) {
      commit_lsn_by_tx.emplace(record.txid, record.lsn);
      return;
    }
    if (record.pageID != page.pageID) {
      return;
    }
    if (record.redoInformation.payload.size() !=
            record.undoInformation.size ||
        record.beforeImage.size() != record.undoInformation.size ||
        record.redoInformation.offset != record.undoInformation.offset) {
      throw std::runtime_error("WAL page update images are inconsistent");
    }
    const auto page_size = static_cast<std::uint64_t>(page.data.size());
    if (record.undoInformation.offset > page_size ||
        record.undoInformation.size >
            page_size - record.undoInformation.offset) {
      throw std::runtime_error("WAL page update is outside page bounds");
    }
    if (record.lsn == page.lsn) {
      found_page_lsn = true;
    }
    plan.page_records.push_back(std::move(record));
  });

  plan.latest_lsn =
      next_lsn == 0 ? std::numeric_limits<std::uint64_t>::max() : next_lsn - 1;
  if (page.lsn > plan.latest_lsn || page.recoveredLSN > plan.latest_lsn) {
    throw std::runtime_error("page LSN is ahead of WAL");
  }
  if (!found_page_lsn) {
    throw std::runtime_error("page LSN does not identify an update for page");
  }

  std::uint64_t baseline_page_lsn = 0;
  for (std::size_t index = 0; index < plan.page_records.size(); ++index) {
    const auto& record = plan.page_records[index];
    const auto committed = commit_lsn_by_tx.find(record.txid);
    const bool is_committed = committed != commit_lsn_by_tx.end();

    if (record.lsn > page.recoveredLSN && record.lsn <= page.lsn) {
      plan.undo_indices.push_back(index);
    }
    if (!is_committed) {
      continue;
    }
    if (record.lsn <= page.recoveredLSN &&
        committed->second <= page.recoveredLSN) {
      baseline_page_lsn = std::max(baseline_page_lsn, record.lsn);
      plan.page_lsn = std::max(plan.page_lsn, record.lsn);
      continue;
    }
    plan.redo_indices.push_back(index);
    plan.page_lsn = std::max(plan.page_lsn, record.lsn);
  }

  if (page.lsn <= page.recoveredLSN && page.lsn != baseline_page_lsn) {
    throw std::runtime_error("page LSN does not match recovered page prefix");
  }
  return plan;
}

void Wal::undo(Page& page, const RecoveryPlan& plan) const {
  for (auto index = plan.undo_indices.rbegin();
       index != plan.undo_indices.rend(); ++index) {
    const auto& record = plan.page_records[*index];
    std::copy(record.beforeImage.begin(), record.beforeImage.end(),
              page.data.data() +
                  static_cast<std::size_t>(record.undoInformation.offset));
  }
}

void Wal::redo(Page& page, const RecoveryPlan& plan) const {
  for (const auto index : plan.redo_indices) {
    const auto& record = plan.page_records[index];
    std::copy(record.redoInformation.payload.begin(),
              record.redoInformation.payload.end(),
              page.data.data() +
                  static_cast<std::size_t>(record.redoInformation.offset));
  }
}
} // namespace wal
