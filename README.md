## pg_async.rs
Asynchronous, HA (master-master) PostgreSQL driver on top of libpq.

## Vision
* Works with a master-master replicated database, such as BDR.
* Uses `libpq`, leveraging it's asynchronous support.
* Maintains an asynchronous connection to every node of the replicated cluster.
* Pings the nodes (with `SELECT 1`) to see who's closer/faster.
* Every operation is a separate transaction.
* If a node fails, the operation is transparently retried on another node.
* Operations are exposed as `futures`.
* `futures` are backed by a thread or two and can be used without a `tokio` reactor (because KISS).
* Fast mode: send the operation to every node and return the first answer.
* Pin mode: send the operation to one of the nodes only (useful to avoid some of the master-master conflicts).
