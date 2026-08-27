-- @sharesync/sharedb-mysql: the main backend op/snapshot store.
-- These are the same statements the adapter runs automatically on
-- first use (unless options.ensureTables === false).

CREATE TABLE IF NOT EXISTS sharedb_ops (
  collection VARCHAR(255) NOT NULL,
  doc_id VARCHAR(255) NOT NULL,
  version INT UNSIGNED NOT NULL,
  operation JSON NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (collection, doc_id, version)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

CREATE TABLE IF NOT EXISTS sharedb_snapshots (
  collection VARCHAR(255) NOT NULL,
  doc_id VARCHAR(255) NOT NULL,
  doc_type VARCHAR(255) NOT NULL,
  version INT UNSIGNED NOT NULL,
  data JSON NULL,
  metadata JSON NULL,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (collection, doc_id)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;
