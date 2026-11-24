export interface OpenOptions {
  cacheSizeBytes?: number;
}

export interface WriteOptions {
  durability?: "immediate" | "none";
}

export interface ScanOptions {
  limit?: number;
  reverse?: boolean;
}

export interface KvEntry {
  key: string;
  value: string;
}

export interface KvBufferEntry {
  key: string;
  value: Buffer;
}

export type BatchOpType =
  | "set"
  | "setBuffer"
  | "delete"
  | "assertEq"
  | "assertExists"
  | "assertMissing"
  | "clearTable"
  | "deleteTable";

export interface BatchOp {
  op: BatchOpType;
  table: string;
  key?: string;
  value?: string;
  bufferValue?: Buffer;
}

export interface TableStat {
  name: string;
  entries: number;
}

export interface DbStats {
  fileSizeBytes: number;
  tables: Array<TableStat>;
}

export class RedbDatabase {
  constructor(dbPath: string, options?: OpenOptions | null);
  cloneHandle(): RedbDatabase;

  openTable(tableName: string): Promise<void>;
  openTableSync(tableName: string): void;
  deleteTable(tableName: string): Promise<boolean>;
  deleteTableSync(tableName: string): boolean;
  clearTable(tableName: string): Promise<void>;
  clearTableSync(tableName: string): void;
  listTables(): Promise<Array<string>>;
  listTablesSync(): Array<string>;
  count(tableName: string): Promise<number>;
  countSync(tableName: string): number;

  get(tableName: string, key: string): Promise<string | null>;
  getSync(tableName: string, key: string): string | null;
  getBuffer(tableName: string, key: string): Promise<Buffer | null>;
  getBufferSync(tableName: string, key: string): Buffer | null;
  getMany(tableName: string, keys: Array<string>): Promise<Array<string | null>>;
  getManySync(tableName: string, keys: Array<string>): Array<string | null>;
  has(tableName: string, key: string): Promise<boolean>;
  hasSync(tableName: string, key: string): boolean;

  set(
    tableName: string,
    key: string,
    value: string,
    options?: WriteOptions | null
  ): Promise<void>;
  setSync(
    tableName: string,
    key: string,
    value: string,
    options?: WriteOptions | null
  ): void;
  setBuffer(
    tableName: string,
    key: string,
    value: Buffer,
    options?: WriteOptions | null
  ): Promise<void>;
  setBufferSync(
    tableName: string,
    key: string,
    value: Buffer,
    options?: WriteOptions | null
  ): void;
  delete(
    tableName: string,
    key: string,
    options?: WriteOptions | null
  ): Promise<boolean>;
  deleteSync(
    tableName: string,
    key: string,
    options?: WriteOptions | null
  ): boolean;

  scanPrefix(
    tableName: string,
    prefix: string,
    options?: ScanOptions | null
  ): Promise<Array<KvEntry>>;
  scanPrefixSync(
    tableName: string,
    prefix: string,
    options?: ScanOptions | null
  ): Array<KvEntry>;
  scanPrefixBuffer(
    tableName: string,
    prefix: string,
    options?: ScanOptions | null
  ): Promise<Array<KvBufferEntry>>;
  scanPrefixBufferSync(
    tableName: string,
    prefix: string,
    options?: ScanOptions | null
  ): Array<KvBufferEntry>;
  scanRange(
    tableName: string,
    startKey: string,
    endKey: string,
    options?: ScanOptions | null
  ): Promise<Array<KvEntry>>;
  scanRangeSync(
    tableName: string,
    startKey: string,
    endKey: string,
    options?: ScanOptions | null
  ): Array<KvEntry>;
  scanRangeBuffer(
    tableName: string,
    startKey: string,
    endKey: string,
    options?: ScanOptions | null
  ): Promise<Array<KvBufferEntry>>;
  scanRangeBufferSync(
    tableName: string,
    startKey: string,
    endKey: string,
    options?: ScanOptions | null
  ): Array<KvBufferEntry>;

  transaction(
    ops: Array<BatchOp>,
    options?: WriteOptions | null
  ): Promise<void>;
  transactionSync(
    ops: Array<BatchOp>,
    options?: WriteOptions | null
  ): void;

  compact(): Promise<boolean>;
  compactSync(): boolean;
  checkIntegrity(): Promise<boolean>;
  checkIntegritySync(): boolean;
  stats(): Promise<DbStats>;
  statsSync(): DbStats;
}

export class ReadOnlyRedbDatabase {
  constructor(dbPath: string, options?: OpenOptions | null);
  get(tableName: string, key: string): Promise<string | null>;
  getSync(tableName: string, key: string): string | null;
  listTablesSync(): Array<string>;
}
