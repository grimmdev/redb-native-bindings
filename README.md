# @grimmdev/redb-native-bindings

[![Quality & Tests](https://github.com/grimmdev/redb-native-bindings/actions/workflows/ci.yml/badge.svg)](https://github.com/grimmdev/redb-native-bindings/actions/workflows/ci.yml)
[![Security Scans](https://github.com/grimmdev/redb-native-bindings/actions/workflows/security.yml/badge.svg)](https://github.com/grimmdev/redb-native-bindings/actions/workflows/security.yml)
[![Release](https://github.com/grimmdev/redb-native-bindings/actions/workflows/release.yml/badge.svg)](https://github.com/grimmdev/redb-native-bindings/actions/workflows/release.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE)
[![Rust](https://img.shields.io/badge/rust-stable-000000.svg?logo=rust)](https://www.rust-lang.org/)
[![Node.js](https://img.shields.io/badge/node-%3E%3D18-339933.svg?logo=node.js&logoColor=white)](https://nodejs.org/)

General-purpose, disk-backed [`redb`](https://github.com/cberner/redb) key-value database compiled as an N-API native addon for Node.js and Bun, exposing both async and sync APIs with bounded memory usage.

## Highlights

- **Bounded RAM footprint:** Disk-backed copy-on-write B-tree with configurable `cacheSizeBytes` (defaults to 32 MB).
- **Multi-threaded MVCC reads:** Backed by `Arc<RwLock<Database>>` so concurrent async reads (`get`, `getMany`, `scanPrefix`, `scanRange`) execute in parallel across worker threads without blocking each other or writers.
- **Full async and sync parity:** Run non-blocking `Promise` methods on hot server paths (such as Hono WebSockets) or `*Sync` methods for scripts and startup initialization.
- **Atomic cross-table CAS transactions:** Combine `assertEq`, `assertExists`, `assertMissing`, `set`, `setBuffer`, `delete`, `clearTable`, and `deleteTable` across multiple tables in one ACID write transaction.
- **Binary and read-only support:** Native `Buffer` storage/scans and a dedicated `ReadOnlyRedbDatabase` handle.

## Installation (GitHub Packages)

Configure npm to resolve the `@grimmdev` scope from GitHub Packages in your `.npmrc`:

```ini
@grimmdev:registry=https://npm.pkg.github.com
//npm.pkg.github.com/:_authToken=${NODE_AUTH_TOKEN}
```

Then install the package:

```bash
npm install @grimmdev/redb-native-bindings
```

## Quick Start

```ts
import { RedbDatabase, ReadOnlyRedbDatabase } from "@grimmdev/redb-native-bindings";

const db = new RedbDatabase("./data/app.redb", {
  cacheSizeBytes: 32 * 1024 * 1024,
});

await db.set("accounts", "acc:1", JSON.stringify({ role: "admin" }));
const val = db.getSync("accounts", "acc:1");

const items = await db.scanPrefix("inventory", "char_1:", {
  limit: 50,
  reverse: false,
});

await db.transaction([
  { op: "assertMissing", table: "ledger", key: "tx:001" },
  { op: "set", table: "ledger", key: "tx:001", value: "committed" },
  { op: "delete", table: "inventory", key: "char_1:ore_1" },
  { op: "set", table: "inventory", key: "char_2:ore_1", value: "{}" },
]);

const reader = new ReadOnlyRedbDatabase("./data/app.redb");
const tables = reader.listTablesSync();
```

## Local Development & Publishing

Requires Rust (stable) and Node.js 18+ (or Bun).

```bash
npm install
npm run build
npm run test:rust
npm run typecheck
```

Push a version tag (for example `v0.1.0`) to trigger the GitHub Actions release workflow, which cross-compiles prebuilt binaries for Linux (x64/arm64), macOS (arm64/x64), and Windows (x64) and publishes them to GitHub Packages.

## License

MIT. See [LICENSE](./LICENSE).
