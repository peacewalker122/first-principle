# First-principles WAL

Small C++23 write-ahead log on POSIX file APIs. `wal::Wal` stores transaction records and recovers one caller-owned fixed-size page at a time. It does not own page storage or support concurrent writers.

## Contract

- `append(const Record&)` requires a nonzero `txid`. It assigns the next sequential `lsn` and the previous LSN for that transaction (`prevLsn`, or zero for its first record), then returns only after `fdatasync` succeeds.
- An `Update` names `pageID`, `redoInformation.payload`, and matching redo/undo offsets. `beforeImage.size()`, `undoInformation.size`, and the redo payload size must match. Updates are fixed-size writes.
- A `Commit` has `pageID == 0` and empty redo, undo, and before-image fields. A transaction is committed only if its WAL contains a `Commit` record.
- `replay(std::function<void(const Record&)>)` validates and passes full decoded records to the visitor in LSN order, including transaction IDs and `prevLsn` links.
- Opening a WAL truncates an incomplete final frame to the last complete record. A complete frame with a bad checksum, invalid record, or invalid sequence fails instead of being discarded.
- The WAL supports one process and one writer per file. Each append syncs separately; there is no group commit. The encoded frame body limit is 64 MiB. The WAL requires a POSIX filesystem; initialization syncs the parent directory.

## Page recovery

`Page.lsn` is the highest update LSN reflected in `Page.data`. `Page.recoveredLSN` is the latest WAL LSN already reconciled with that page. Load and persist `data`, `lsn`, and `recoveredLSN` together.

Before logging an update, the caller supplies the old bytes in `beforeImage`; the WAL must sync before the caller writes the new bytes to the page. Recovery assumes page data includes every update to that page in `(recoveredLSN, lsn]`, applied in LSN order. Keep page metadata consistent with the data. Recovery rejects metadata ahead of the current WAL before changing the page.

`recover(Page&)` finds committed transactions, undoes page updates in reverse LSN order over `(recoveredLSN, lsn]` using their before-images, then redoes committed page updates whose update LSN or transaction's `Commit` LSN exceeds the prior `recoveredLSN`, in LSN order. Thus, a later `Commit` can make an earlier update eligible for redo even when its update LSN is at or below the prior `recoveredLSN`. Undo includes committed updates so recovery can rebuild a known baseline before redo; updates from transactions without a commit are not redone. Recovery sets `lsn` to the last committed update applied to the page and advances `recoveredLSN` to the latest WAL LSN. Repeating recovery with the resulting page is idempotent.

The before-image is required because an offset and size identify overwritten bytes but cannot restore them. The caller must log the exact old bytes for each fixed-size write.

## Format

The eight-byte file header is `WAL1 00 00 00 01`: `WAL1` magic followed by big-endian version 1. Each frame has a 16-byte header: little-endian LSN (`u64`), body length (`u32`), and CRC-32/IEEE (`u32`). The checksum covers the first 12 frame-header bytes and the body.

The body starts with a fixed 52-byte prefix: record type (`u32`, 0 for `Update`, 1 for `Commit`), then `txid`, `prevLsn`, `pageID`, redo offset, undo offset, and size as little-endian `u64` values. The frame header stores `Record.lsn`. An update body continues with `size` redo bytes followed by `size` before-image bytes. A commit body ends after the prefix; its page ID, offsets, and size are zero. Invalid types, lengths, or metadata fail validation.

## Build and test

```sh
cmake -S . -B build -DBUILD_TESTING=ON
cmake --build build
ctest --test-dir build --output-on-failure
```

With testing enabled, CMake prefers an installed Catch2 v3 package. If it is unavailable, CMake fetches pinned version 3.7.1 with FetchContent. Offline environments can install Catch2 v3 before configuring.

List named cases with `./build/wal_tests --list-tests`; run one directly with `./build/wal_tests "append after reopen preserves LSN and prevLsn chain"`. `catch_discover_tests` registers each case with CTest. List them with `ctest --test-dir build -N`, or select one with `ctest --test-dir build --output-on-failure -R "append after reopen"`.

The cases cover append/reopen and LSN chains, torn-tail repair, complete record replay, loser undo and committed redo (including a winner already on the page), pages behind the WAL, idempotence, late commits, page-ahead rejection without mutation, and checksum corruption rejection. `CMake` exports `compile_commands.json`; `.clangd` points clangd to `build/` so Neovim resolves project include paths.

## Minimal use

```cpp
#include "wal.hpp"

#include <array>
#include <cstddef>

int main() {
  wal::Wal log{"data.wal"};
  const std::array<std::byte, 4> before{};
  const std::array<std::byte, 4> after{
      std::byte{0x41}, std::byte{0x42}, std::byte{0x43}, std::byte{0x44}};

  wal::Page page{};
  page.pageID = 7;
  page.data.assign(before.begin(), before.end());

  wal::Record update{};
  update.txid = 1;
  update.recordType = wal::RecordType::Update;
  update.pageID = page.pageID;
  update.redoInformation.payload.assign(after.begin(), after.end());
  update.redoInformation.offset = 0;
  update.undoInformation.offset = 0;
  update.undoInformation.size = before.size();
  update.beforeImage.assign(before.begin(), before.end());

  const auto update_lsn = log.append(update);
  page.data.assign(after.begin(), after.end());
  page.lsn = update_lsn;
  // Persist data, lsn, and recoveredLSN together in the page store.

  wal::Record commit{};
  commit.txid = 1;
  commit.recordType = wal::RecordType::Commit;
  log.append(commit);

  log.recover(page);
  // Persist the recovered data and both LSN fields together.
}
```
