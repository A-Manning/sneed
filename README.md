# Sneed

[![crates.io](https://img.shields.io/crates/v/sneed.svg)](https://crates.io/crates/sneed)

A safe wrapper around heed, with better errors and observability.

Formerly Chuck's

## Differences from Heed
* Uses `fallible-iterator` by default for iterators
* Improved errors: Include relevant DB paths, keys, values, etc., in
error messages
* Observable DBs: Receive a notification via channel when a database is updated
via a write txn.
* Read-only DBs: Enforce better mutability boundaries by exposing databases as
read-only
* Type-level tags to distinguish between different DB envs
* Unit key encoder/decoder: Use `()` as a DB key
