var mysql = require('mysql2/promise');
var ShareDBMySQL = require('../index');

/**
 * Runs the ShareSync fork's own pluggable DB acceptance suite against
 * the adapter, plus MySQL-specific tests (the commit race, JSON-null
 * vs no-data round-tripping).
 *
 * Needs a reachable MySQL; override with MYSQL_TEST_URI. The test
 * database is created fresh and dropped per run.
 */
var ADMIN_URI = process.env.MYSQL_TEST_URI || 'mysql://root@localhost:3306';
var TEST_DB = 'sharedb_mysql_test';

// The database is recreated ONCE per run; each test gets fresh state
// by truncating the two tables (the suite calls create() per test,
// and DROP DATABASE per test is minutes of pure DDL).
var prepared = null;
function prepare() {
  if (!prepared) {
    prepared = (async function() {
      var admin = await mysql.createConnection(ADMIN_URI);
      await admin.query('DROP DATABASE IF EXISTS ' + TEST_DB);
      await admin.query('CREATE DATABASE ' + TEST_DB + ' CHARACTER SET utf8mb4');
      await admin.end();
    })();
  }
  return prepared;
}

function create(callback) {
  (async function() {
    await prepare();
    var db = new ShareDBMySQL({uri: ADMIN_URI + '/' + TEST_DB});
    await db._ready();
    await db.pool.query('TRUNCATE TABLE sharedb_ops');
    await db.pool.query('TRUNCATE TABLE sharedb_snapshots');
    return db;
  })().then(function(db) {
    callback(null, db);
  }, callback);
}

require('@sharesync/sharedb/test/db')({create: create});

describe('mysql-specific', function() {
  var db;
  beforeEach(function(done) {
    create(function(err, created) {
      if (err) return done(err);
      db = created;
      done();
    });
  });
  afterEach(function(done) {
    db.close(done);
  });

  it('exactly one of N racing commits at the same version succeeds', function(done) {
    var expect = require('chai').expect;
    var N = 8;
    var results = [];
    var finished = 0;
    for (var i = 0; i < N; i++) {
      (function(i) {
        var op = {v: 0, create: {type: 'http://sharejs.org/types/JSONv0', data: {racer: i}}};
        var snapshot = {id: 'race', v: 1, type: 'http://sharejs.org/types/JSONv0', data: {racer: i}};
        db.commit('testcollection', 'race', op, snapshot, null, function(err, succeeded) {
          if (err) return done(err);
          results.push(succeeded);
          finished++;
          if (finished === N) {
            var winners = results.filter(Boolean);
            expect(winners).to.have.length(1);
            done();
          }
        });
      })(i);
    }
  });

  it('getSnapshot never tears between the snapshot row and the op count', function(done) {
    // Regression: as two separate queries, a reader racing a
    // delete→create could see "no snapshot row" from before the
    // create and an op count from after it — reporting the doc
    // deleted at the NEW version, which let a resubmitted create
    // commit twice (caught by the acceptance suite's resubmit test,
    // ~5% of runs). The read is now one statement; this loop makes a
    // regression loud rather than rare.
    var expect = require('chai').expect;
    var Backend = require('@sharesync/sharedb/lib/backend');
    var async = require('async');
    this.timeout(30000);
    var iterations = 25;
    var iteration = 0;
    function runOnce(next) {
      iteration++;
      var backend = new Backend({db: db});
      var connection1 = backend.connect();
      var connection2 = backend.connect();
      var docId = 'torn-' + iteration;
      var doc1 = connection1.get('dogs', docId);
      var doc2 = connection2.get('dogs', docId);
      async.series([
        doc1.create.bind(doc1, {age: 3}),
        doc1.del.bind(doc1),
        function(step) {
          var interrupted = false;
          backend.use('submit', function(request, nextMw) {
            if (!interrupted) {
              interrupted = true;
              connection2.close();
              backend.connect(connection2);
            }
            nextMw();
          });
          doc2.create({name: 'Fido'}, function(err) {
            if (err) return step(err);
            expect(doc2.version).to.equal(3);
            step();
          });
        }
      ], function(err) {
        if (err) return next(err);
        // The backend shares the suite-owned db; do not close it here.
        next();
      });
    }
    var tasks = [];
    for (var i = 0; i < iterations; i++) tasks.push(runOnce);
    async.series(tasks, done);
  });

  it('round-trips JSON null data distinctly from no data', function(done) {
    var expect = require('chai').expect;
    var type = 'http://sharejs.org/types/JSONv0';
    var op = {v: 0, create: {type: type, data: null}};
    var snapshot = {id: 'nulldoc', v: 1, type: type, data: null};
    db.commit('testcollection', 'nulldoc', op, snapshot, null, function(err, succeeded) {
      if (err) return done(err);
      expect(succeeded).to.equal(true);
      db.getSnapshot('testcollection', 'nulldoc', null, null, function(err, snap) {
        if (err) return done(err);
        expect(snap.data).to.equal(null);
        expect(snap.type).to.equal(type);
        db.getSnapshot('testcollection', 'never-created', null, null, function(err, missing) {
          if (err) return done(err);
          expect(missing.data).to.equal(undefined);
          expect(missing.type).to.equal(null);
          expect(missing.v).to.equal(0);
          done();
        });
      });
    });
  });
});
