# BrickDB

[![Rust Test](https://github.com/a-poor/brickdb/actions/workflows/rust-test.yml/badge.svg)](https://github.com/a-poor/brickdb/actions/workflows/rust-test.yml)
[![Crates.io](https://img.shields.io/crates/v/brickdb)](https://crates.io/crates/brickdb)
[![Crates.io](https://img.shields.io/crates/l/brickdb)](https://crates.io/crates/brickdb)
[![docs.rs](https://img.shields.io/docsrs/brickdb)](https://docs.rs/brickdb)



_created by Austin Poor_

A small basic proof-of-concept database written in Rust. I wouldn't recommend using this in production.


## Notes

- [x] Update the `storage::level::Level::new` implementation to format the path using the `path` argument as a _parent directory path_
- [x] The LSMTree should be able to move data between levels (eg memtable -> level-1-sstable) without downtime (if possible). Can it craete an sstable from the memtable, write the memtable to disk, and then clear the memtable? Should there be a _frozen_ memtable that can be read from but not written to, while the on-disk operations are working?
- [x] Like the above, should also (maybe) work with SSTables? Or do they not need to, since they're read-only?
    - Maybe they don't need to but need to be able to mark which table-ids are being compacted. So compaction can start on level 1 (say) and while it's happening, they can still be read from, until the new level-2 table is ready, at which point the level-1 tables can be removed. But since a new level one table might have been added (probably _shouldn't_ have been but _could_ have been), the compaction process should remember which tables it's including and not just say _all tables in level 1_.
- [x] Compress data written to disk with snappy compression? (try `snap`)
- [ ] Update `todo!()`s in tests (and the commented-out tests)
- [ ] Add the ability to encrypt data on disk (`aes` with [ring](https://docs.rs/ring/latest/ring)?)
- [ ] The `bloom` create hasn't had any updates in the past 7 years. Consider changing to a different implementation or writing it myself.
- [ ] Add metadata for the `storage::lsm::LSMTree` so it can be read back in.
- [ ] Create a separate reader/writer implementation for reading/writing data. It can simplify async writing of `bson` data, compression (share encoders/decoders?), and encryption.

