# @sharesync/sharedb-mysql

MySQL database adapter for [ShareDB (the ShareSync
fork)](https://github.com/sharesync/sharedb) — the **main backend
op/snapshot store**, following the `sharedb-mongo` naming convention.

> Not a DurableStore adapter. The `@sharesync/sharedb-storage-*`
> packages are client-side offline stores; this package is the
> authoritative database behind a ShareDB server.

## Usage

```js
var ShareDB = require('@sharesync/sharedb');
var ShareDBMySQL = require('@sharesync/sharedb-mysql');

var db = new ShareDBMySQL({uri: 'mysql://user:pass@host:3306/mydb'});
// or share an existing mysql2/promise pool (the adapter won't close it):
// var db = new ShareDBMySQL({pool: myPool});
// or mysql2 pool options:
// var db = new ShareDBMySQL({connection: {host, user, password, database}});

var backend = new ShareDB({db: db});
```

## Schema

Two tables — an append-only op log keyed `(collection, doc_id,
version)` and one snapshot row per doc. They are created
automatically on first use; pass `ensureTables: false` and run
[schema.sql](./schema.sql) yourself if your deployment separates DDL
from runtime (the statements are also exported as
`ShareDBMySQL.DDL`).

## Semantics

- **Concurrency control is the op log's primary key.** ShareDB's
  `commit()` contract — exactly one of two racing submissions at a
  version may succeed — is enforced by the duplicate-key rejection on
  the op insert, inside a transaction with the snapshot write.
- **A document's version is its op count** (the MemoryDB reference
  semantics): a deleted document still reports the version implied by
  its op log.
- **No live queries.** `query()` is unimplemented on purpose; keep
  your queryable state in your own tables and use this adapter as the
  op/snapshot store.
- Snapshot `data` distinguishes *no data* (SQL `NULL` →
  `undefined`) from *JSON null* (stored as JSON `null`).

## Tests

Runs the ShareSync fork's own pluggable DB acceptance suite plus
MySQL-specific tests (commit races, JSON-null round-tripping):

```sh
MYSQL_TEST_URI=mysql://root@localhost:3306 npm test
```

The test database (`sharedb_mysql_test`) is dropped and recreated per
run.

## License

MIT
