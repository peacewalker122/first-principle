#include "wal.hpp"

#include <array>
#include <cerrno>
#include <cstdint>
#include <fcntl.h>
#include <limits>
#include <stdexcept>
#include <sys/stat.h>
#include <system_error>
#include <unistd.h>
#include <vector>

namespace wal {
namespace {

// Eight-byte file header: "WAL1" magic followed by big-endian version 1.
constexpr std::array<std::byte, 8> kFileHeader{
    std::byte{0x57}, std::byte{0x41}, std::byte{0x4c}, std::byte{0x31},
    std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, std::byte{0x01},
};
constexpr std::size_t kRecordHeaderSize = 16;
constexpr std::uint32_t kMaxRecordSize = 64U * 1024U * 1024U;

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

} // namespace

Wal::Wal(const std::filesystem::path &path) {
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
    next_lsn_ = scan(true, {});
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

std::uint64_t Wal::append(std::span<const std::byte> payload) {
  if (poisoned_) {
    throw std::runtime_error(
        "WAL is unusable after a failed append; reopen it");
  }
  if (next_lsn_ == 0) {
    throw std::overflow_error("WAL LSN space exhausted");
  }
  if (payload.size() > kMaxRecordSize) {
    throw std::length_error("WAL record exceeds 64 MiB");
  }

  std::array<std::byte, kRecordHeaderSize> header{};
  auto header_span = std::span<std::byte>{header};
  store_u64(header_span.first(8), next_lsn_);
  store_u32(header_span.subspan(8, 4),
            static_cast<std::uint32_t>(payload.size()));
  const auto crc =
      checksum(std::span<const std::byte>{header.data(), 12}, payload);
  store_u32(header_span.subspan(12, 4), crc);

  const auto lsn = next_lsn_;
  try {
    write_all(fd_, header);
    write_all(fd_, payload);
    sync_data(fd_);
  } catch (...) {
    poisoned_ = true;
    throw;
  }

  next_lsn_ = lsn == std::numeric_limits<std::uint64_t>::max() ? 0 : lsn + 1;
  return lsn;
}

void Wal::replay(const Visitor &visitor) const {
  if (!visitor) {
    throw std::invalid_argument("WAL replay requires a visitor");
  }
  (void)scan(false, visitor);
}

std::uint64_t Wal::scan(bool repair_tail, const Visitor &visitor) const {
  struct stat status{};
  if (::fstat(fd_, &status) < 0) {
    throw_errno("fstat WAL");
  }
  if (status.st_size < 0 ||
      static_cast<std::uint64_t>(status.st_size) < kFileHeader.size()) {
    throw std::runtime_error("WAL header is incomplete");
  }

  std::array<std::byte, kFileHeader.size()> file_header{};
  if (!read_exact_at(fd_, 0, file_header) || file_header != kFileHeader) {
    throw std::runtime_error("invalid WAL file header");
  }

  const auto file_size = static_cast<std::uint64_t>(status.st_size);
  auto offset = static_cast<std::uint64_t>(kFileHeader.size());
  std::uint64_t expected_lsn = 1;
  bool incomplete_tail = false;
  std::vector<std::byte> payload;

  while (offset < file_size) {
    const auto remaining = file_size - offset;
    if (remaining < kRecordHeaderSize) {
      incomplete_tail = true;
      break;
    }

    std::array<std::byte, kRecordHeaderSize> header{};
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
        static_cast<std::uint64_t>(kRecordHeaderSize) + length;
    if (remaining < frame_size) {
      incomplete_tail = true;
      break;
    }

    payload.resize(length);
    if (!read_exact_at(fd_, static_cast<off_t>(offset + kRecordHeaderSize),
                       payload)) {
      throw std::runtime_error("WAL changed while being read");
    }
    if (checksum(header_span.first(12), payload) != stored_crc) {
      throw std::runtime_error("WAL record checksum mismatch");
    }
    if (visitor) {
      visitor(lsn, payload);
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

} // namespace wal
