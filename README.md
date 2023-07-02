# BrickDB

[![Rust Test](https://github.com/a-poor/brickdb/actions/workflows/rust-test.yml/badge.svg)](https://github.com/a-poor/brickdb/actions/workflows/rust-test.yml)
[![Crates.io](https://img.shields.io/crates/v/brickdb)](https://crates.io/crates/brickdb)
[![Crates.io](https://img.shields.io/crates/l/brickdb)](https://crates.io/crates/brickdb)
[![docs.rs](https://img.shields.io/docsrs/brickdb)](https://docs.rs/brickdb)



_created by Austin Poor_

A small basic proof-of-concept database written in Rust. I wouldn't recommend using this in production.


## Notes

- Update the `storage::level::Level::new` implementation to format the path using the `path` argument as a _parent directory path_
- The LSMTree should be able to move data between levels (eg memtable -> level-1-sstable) without downtime (if possible). Can it craete an sstable from the memtable, write the memtable to disk, and then clear the memtable? Should there be a _frozen_ memtable that can be read from but not written to, while the on-disk operations are working?

