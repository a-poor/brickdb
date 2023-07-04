# BrickDB

[![Rust Test](https://github.com/a-poor/brickdb/actions/workflows/rust-test.yml/badge.svg)](https://github.com/a-poor/brickdb/actions/workflows/rust-test.yml)
[![Crates.io](https://img.shields.io/crates/v/brickdb)](https://crates.io/crates/brickdb)
[![Crates.io](https://img.shields.io/crates/l/brickdb)](https://crates.io/crates/brickdb)
[![docs.rs](https://img.shields.io/docsrs/brickdb)](https://docs.rs/brickdb)



_created by Austin Poor_

A small basic proof-of-concept database written in Rust. I wouldn't recommend using this in production.


## Notes

- [ ] Update the `storage::level::Level::new` implementation to format the path using the `path` argument as a _parent directory path_
- [ ] The LSMTree should be able to move data between levels (eg memtable -> level-1-sstable) without downtime (if possible). Can it craete an sstable from the memtable, write the memtable to disk, and then clear the memtable? Should there be a _frozen_ memtable that can be read from but not written to, while the on-disk operations are working?
- [ ] Like the above, should also (maybe) work with SSTables? Or do they not need to, since they're read-only?
    - Maybe they don't need to but need to be able to mark which table-ids are being compacted. So compaction can start on level 1 (say) and while it's happening, they can still be read from, until the new level-2 table is ready, at which point the level-1 tables can be removed. But since a new level one table might have been added (probably _shouldn't_ have been but _could_ have been), the compaction process should remember which tables it's including and not just say _all tables in level 1_.
- [ ] Compress data written to disk with snappy compression? (try [snap](https://stackoverflow.com/questions/40740752/how-to-lay-out-b-tree-data-on-disk))
- [ ] Update tests

