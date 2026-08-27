import { Pool, PoolOptions } from 'mysql2/promise';

declare interface ShareDBMySQLOptions {
  /** An existing mysql2/promise pool. The adapter will NOT close it. */
  pool?: Pool;
  /** A mysql:// connection URI. The adapter owns and closes this pool. */
  uri?: string;
  /** mysql2 pool options. The adapter owns and closes this pool. */
  connection?: PoolOptions;
  /** Create the two tables on first use (default true). */
  ensureTables?: boolean;
}

/**
 * MySQL database adapter for ShareDB (ShareSync fork) — the MAIN
 * BACKEND op/snapshot store, following the sharedb-mongo convention.
 * Not a DurableStore storage adapter.
 */
declare class ShareDBMySQL {
  constructor(options: ShareDBMySQLOptions);
  /** The CREATE TABLE statements, for callers running DDL themselves. */
  static DDL: string[];
  close(callback?: (err?: Error) => void): void;
  commit(
    collection: string,
    id: string,
    op: unknown,
    snapshot: unknown,
    options: unknown,
    callback: (err: Error | null, succeeded?: boolean) => void
  ): void;
  getSnapshot(
    collection: string,
    id: string,
    fields: unknown,
    options: unknown,
    callback: (err: Error | null, snapshot?: unknown) => void
  ): void;
  getOps(
    collection: string,
    id: string,
    from: number | null,
    to: number | null,
    options: unknown,
    callback: (err: Error | null, ops?: unknown[]) => void
  ): void;
  deleteOps(
    collection: string,
    id: string,
    from: number | null,
    to: number | null,
    options: unknown,
    callback: (err: Error | null) => void
  ): void;
}

export = ShareDBMySQL;
