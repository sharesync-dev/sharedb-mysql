var DB = require('@sharesync/sharedb/lib/db');
var Snapshot = require('@sharesync/sharedb/lib/snapshot');
var mysql = require('mysql2/promise');

/**
 * MySQL database adapter for ShareDB (the ShareSync fork).
 *
 * This is the MAIN BACKEND database — the authoritative op log and
 * snapshot store behind a ShareDB server — following the
 * sharedb-mongo convention. It is NOT a DurableStore storage adapter
 * (those live under the @sharesync/sharedb-storage-* prefix and are
 * client-side).
 *
 * Two tables (see schema.sql): one append-only op log keyed
 * (collection, doc_id, version) and one snapshot row per doc. The op
 * log's primary key is the concurrency control: ShareDB's contract
 * for commit() is that exactly one of two racing submissions at the
 * same version may succeed, and a duplicate-key rejection on the op
 * insert is precisely that.
 *
 * Version semantics mirror ShareDB's MemoryDB reference: a document's
 * version is its op count, so a deleted document (no snapshot row)
 * still reports the version implied by its op log.
 *
 * No live-query support: query() is unimplemented on purpose. Use a
 * real query layer over your own tables; this adapter is the
 * op/snapshot store.
 *
 * Usage:
 *   var ShareDBMySQL = require('@sharesync/sharedb-mysql');
 *   var db = new ShareDBMySQL({uri: 'mysql://user:pass@host:3306/db'});
 *   // or: new ShareDBMySQL({pool: existingMysql2PromisePool})
 *   var backend = new ShareDB({db: db});
 *
 * Tables are created automatically on first use unless
 * options.ensureTables === false (then run schema.sql yourself).
 */
module.exports = ShareDBMySQL;

var OPS_TABLE = 'sharedb_ops';
var SNAPSHOTS_TABLE = 'sharedb_snapshots';

var CREATE_OPS =
  'CREATE TABLE IF NOT EXISTS ' + OPS_TABLE + ' (' +
  '  collection VARCHAR(255) NOT NULL,' +
  '  doc_id VARCHAR(255) NOT NULL,' +
  '  version INT UNSIGNED NOT NULL,' +
  '  operation JSON NOT NULL,' +
  '  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,' +
  '  PRIMARY KEY (collection, doc_id, version)' +
  ') CHARACTER SET utf8mb4 COLLATE utf8mb4_bin';

var CREATE_SNAPSHOTS =
  'CREATE TABLE IF NOT EXISTS ' + SNAPSHOTS_TABLE + ' (' +
  '  collection VARCHAR(255) NOT NULL,' +
  '  doc_id VARCHAR(255) NOT NULL,' +
  '  doc_type VARCHAR(255) NOT NULL,' +
  '  version INT UNSIGNED NOT NULL,' +
  '  data JSON NULL,' +
  '  metadata JSON NULL,' +
  '  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,' +
  '  PRIMARY KEY (collection, doc_id)' +
  ') CHARACTER SET utf8mb4 COLLATE utf8mb4_bin';

function ShareDBMySQL(options) {
  if (!(this instanceof ShareDBMySQL)) return new ShareDBMySQL(options);
  DB.call(this, options);
  options = options || {};

  if (options.pool) {
    this.pool = options.pool;
    this._ownsPool = false;
  } else if (options.uri) {
    this.pool = mysql.createPool(options.uri);
    this._ownsPool = true;
  } else if (options.connection) {
    this.pool = mysql.createPool(options.connection);
    this._ownsPool = true;
  } else {
    throw new Error('ShareDBMySQL requires options.pool, options.uri, or options.connection');
  }

  this._ensureTables = options.ensureTables !== false;
  this._readyPromise = null;
  this.closed = false;
}

ShareDBMySQL.prototype = Object.create(DB.prototype);
ShareDBMySQL.prototype.constructor = ShareDBMySQL;

// Exposed for callers who prefer running DDL themselves (see
// schema.sql for the same statements as a file).
ShareDBMySQL.DDL = [CREATE_OPS, CREATE_SNAPSHOTS];

ShareDBMySQL.prototype._ready = function() {
  var db = this;
  if (!this._readyPromise) {
    this._readyPromise = (async function() {
      if (!db._ensureTables) return;
      await db.pool.query(CREATE_OPS);
      await db.pool.query(CREATE_SNAPSHOTS);
    })();
  }
  return this._readyPromise;
};

ShareDBMySQL.prototype.close = function(callback) {
  var db = this;
  var promise = (async function() {
    if (db.closed) return;
    db.closed = true;
    if (db._ownsPool) await db.pool.end();
  })();
  if (callback) {
    promise.then(function() {
      callback();
    }, callback);
  }
};

// Persist an op and snapshot if it is for the next version.
// callback(err, succeeded). The op insert's duplicate-key rejection
// is the loser's signal in a race; the pre-insert version read only
// rejects stale or future submissions cheaply and cannot itself
// race (two readers of the same head both pass, and the PK decides).
ShareDBMySQL.prototype.commit = function(collection, id, op, snapshot, options, callback) {
  if (typeof callback !== 'function') throw new Error('Callback required');
  var db = this;
  (async function() {
    await db._ready();
    var connection = await db.pool.getConnection();
    try {
      await connection.beginTransaction();

      var version = await currentVersion(connection, collection, id);
      if (snapshot.v !== version + 1) {
        await connection.rollback();
        return false;
      }

      try {
        await connection.query(
          'INSERT INTO ' + OPS_TABLE + ' (collection, doc_id, version, operation) VALUES (?, ?, ?, ?)',
          [collection, id, op.v, JSON.stringify(op)]
        );
      } catch (err) {
        if (err && err.code === 'ER_DUP_ENTRY') {
          await connection.rollback();
          return false;
        }
        throw err;
      }

      if (!snapshot.type) {
        // No type means delete (MemoryDB convention).
        await connection.query(
          'DELETE FROM ' + SNAPSHOTS_TABLE + ' WHERE collection = ? AND doc_id = ?',
          [collection, id]
        );
      } else {
        await connection.query(
          'INSERT INTO ' + SNAPSHOTS_TABLE + ' (collection, doc_id, doc_type, version, data, metadata)' +
          ' VALUES (?, ?, ?, ?, ?, ?)' +
          ' ON DUPLICATE KEY UPDATE doc_type = VALUES(doc_type), version = VALUES(version),' +
          ' data = VALUES(data), metadata = VALUES(metadata)',
          [
            collection,
            id,
            snapshot.type,
            snapshot.v,
            snapshot.data === undefined ? null : JSON.stringify(snapshot.data),
            snapshot.m == null ? null : JSON.stringify(snapshot.m)
          ]
        );
      }

      await connection.commit();
      return true;
    } catch (err) {
      try {
        await connection.rollback();
      } catch (rollbackErr) {
        // The original error is the story; rollback failure is noise.
      }
      throw err;
    } finally {
      connection.release();
    }
  })().then(function(succeeded) {
    callback(null, succeeded);
  }, callback);
};

// callback(err, snapshot). A never-created (or deleted) doc reports
// the version implied by its op log, with a null type.
ShareDBMySQL.prototype.getSnapshot = function(collection, id, fields, options, callback) {
  if (typeof callback !== 'function') throw new Error('Callback required');
  var includeMetadata = (fields && fields.$submit) || (options && options.metadata);
  var db = this;
  (async function() {
    await db._ready();
    // ONE statement, on purpose: the snapshot row and the op-derived
    // version must come from a single consistent read view. As two
    // queries (potentially on two pool connections) they can tear —
    // a reader racing a delete→create sequence once saw "no snapshot
    // row" from before the create and an op count from after it,
    // reported the doc as deleted at the new version, and let a
    // resubmitted create commit twice.
    var rows = (await db.pool.query(
      'SELECT s.doc_type, s.version, s.data, s.metadata,' +
      ' s.data IS NULL AS data_is_null, s.doc_id IS NOT NULL AS has_row,' +
      ' (SELECT COALESCE(MAX(version) + 1, 0) FROM ' + OPS_TABLE +
      '   WHERE collection = ? AND doc_id = ?) AS ops_version' +
      ' FROM (SELECT 1) AS one' +
      ' LEFT JOIN ' + SNAPSHOTS_TABLE + ' s ON s.collection = ? AND s.doc_id = ?',
      [collection, id, collection, id]
    ))[0];
    var row = rows[0];
    if (row.has_row) {
      var meta = includeMetadata ? parseMaybe(row.metadata) : null;
      // data IS NULL discriminates SQL NULL ("no data", i.e.
      // undefined) from a stored JSON null — mysql2 hands both back
      // as JS null.
      var data = row.data_is_null ? undefined : parseMaybe(row.data);
      return new Snapshot(id, row.version, row.doc_type, data, meta);
    }
    // No snapshot row: deleted or never created. The version is the
    // op count (MemoryDB semantics). MAX()+1 promotes to BIGINT,
    // which mysql2 hands back as a string — and ShareDB rejects a
    // non-number version outright.
    return new Snapshot(id, Number(row.ops_version), null, undefined, null);
  })().then(function(snapshot) {
    callback(null, snapshot);
  }, callback);
};

// Get operations in [from, to). to == null means to the end. Errors
// with 'Missing ops' if the range cannot be fully served — a gap in
// the op log must never be silently skipped over.
ShareDBMySQL.prototype.getOps = function(collection, id, from, to, options, callback) {
  if (typeof callback !== 'function') throw new Error('Callback required');
  var includeMetadata = options && options.metadata;
  var db = this;
  (async function() {
    await db._ready();
    if (!from) from = 0;
    var sql =
      'SELECT version, operation FROM ' + OPS_TABLE +
      ' WHERE collection = ? AND doc_id = ? AND version >= ?';
    var params = [collection, id, from];
    if (to != null) {
      sql += ' AND version < ?';
      params.push(to);
    }
    sql += ' ORDER BY version ASC';
    var rows = (await db.pool.query(sql, params))[0];

    // Contiguity check: versions must run from..from+n-1, and a
    // bounded request must be fully satisfied.
    for (var i = 0; i < rows.length; i++) {
      if (rows[i].version !== from + i) throw new Error('Missing ops');
    }
    if (to != null && rows.length < to - from) throw new Error('Missing ops');

    return rows.map(function(row) {
      var op = parseMaybe(row.operation);
      if (!includeMetadata) delete op.m;
      return op;
    });
  })().then(function(ops) {
    callback(null, ops);
  }, callback);
};

// Optional interface member (MemoryDB has it too): hard-delete a
// range of ops, e.g. after milestone compaction. [from, to) with
// to == null meaning to the end.
ShareDBMySQL.prototype.deleteOps = function(collection, id, from, to, options, callback) {
  if (typeof callback !== 'function') throw new Error('Callback required');
  var db = this;
  (async function() {
    await db._ready();
    if (!from) from = 0;
    var sql = 'DELETE FROM ' + OPS_TABLE + ' WHERE collection = ? AND doc_id = ? AND version >= ?';
    var params = [collection, id, from];
    if (to != null) {
      sql += ' AND version < ?';
      params.push(to);
    }
    await db.pool.query(sql, params);
  })().then(function() {
    callback(null);
  }, callback);
};

async function currentVersion(connection, collection, id) {
  var rows = (await connection.query(
    'SELECT COALESCE(MAX(version) + 1, 0) AS v FROM ' + OPS_TABLE +
    ' WHERE collection = ? AND doc_id = ?',
    [collection, id]
  ))[0];
  // BIGINT-promoted aggregate: mysql2 returns it as a string.
  return Number(rows[0].v);
}

// mysql2 parses JSON columns to objects already, but returns strings
// under some configurations (and always for TEXT fallbacks) — accept
// both.
function parseMaybe(value) {
  if (value == null) return null;
  if (typeof value === 'string') return JSON.parse(value);
  return value;
}

