## pg_async.rs
Asynchronous, HA (master-master) PostgreSQL driver on top of libpq.

[![crate](https://img.shields.io/crates/v/pg_async.svg)](https://crates.io/crates/pg_async)
![](https://tokei.rs/b1/github/ArtemGr/pg_async.rs)

## Vision
- [x] Works with a master-master replicated database, such as BDR.
- [x] Uses `libpq`, leveraging it's asynchronous support.
- [x] Maintains an asynchronous connection to every node of the replicated cluster.
- [ ] Pings the nodes (with `SELECT 1`) to see who's closer/faster.
- [x] Every operation is a separate transaction.
- [ ] If a node fails, the operation is transparently retried on another node.
- [x] Operations are exposed as `futures`.
- [x] `futures` are backed by a thread or two and can be used without a `tokio` reactor (because KISS).
- [ ] Fast mode: send the operation to every node and return the first answer.
- [ ] Pin mode: send the operation to one of the nodes only (useful to avoid some of the master-master conflicts).
- [ ] There is a JSON helper converting table rows to serde_json objects.
