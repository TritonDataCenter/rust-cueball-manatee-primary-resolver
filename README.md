# cueball-manatee-primary-resolver

## About

This is an implementation of the[cueball](https://github.com/joyent/rust-cueball) `Resolver` trait that provides
a list of backends for use by the connection pool. See the cueball documentation
for more information.

This resolver is specific to the Joyent [manatee](https://github.com/joyent/manatee) project. It queries a zookeeper
cluster to determine the PostgreSQL replication primary from a set of PostgreSQL
replication peers.
