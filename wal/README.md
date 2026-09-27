# First-principles WAL

Small C++23 write-ahead log on POSIX file APIs. `wal::Wal` stores opaque byte records; it does not implement a database, page store, transactions, checkpoints, or concurrent writers.

## Contract

- `append(payload)` adds one monotonically numbered record and returns only after `fdatasync` succeeds.
- `replay(visitor)` verifies framing, sequential LSNs, and CRC-32 before passing each payload to the visitor. Payload span is valid only during that callback.
- Opening a WAL truncates an incomplete final frame to the last complete record. A complete frame with a bad checksum or invalid sequence fails; it is not silently discarded.
- One process and one writer per WAL file. Every append syncs separately; no group commit.
- Payload limit: 64 MiB. WAL file must be on a POSIX filesystem; parent directory is synced when initializing its header.

## Format

File header is eight bytes: `WAL1 00 00 00 01`. Each record is little-endian LSN (`u64`), payload length (`u32`), CRC-32/IEEE (`u32`), then payload. CRC covers first 12 header bytes and payload. This makes torn tails detectable and complete-record corruption rejectable.

## Build and check

```sh
cmake -S . -B build
cmake --build build
ctest --test-dir build --output-on-failure
```

The smoke test covers append/reopen/replay, incomplete-tail recovery, and checksum rejection.
`CMake` exports `compile_commands.json`; `.clangd` points clangd to `build/` so Neovim resolves project include paths.

## Minimal use

```cpp
#include "wal.hpp"
#include <string>

wal::Wal log{"data.wal"};
std::string command = "set key value";
log.append(std::as_bytes(std::span{command.data(), command.size()}));

log.replay([](std::uint64_t lsn, std::span<const std::byte> payload) {
    // Decode and apply record to application state here.
});
```
