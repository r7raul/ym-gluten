# Iceberg v2 + Gluten Test Plan

Tests for Gluten's native offload of all Iceberg v2 operations on macOS aarch64.
All tests run in a single `spark-sql` session (standard launch command from `build-macos.md` Step 6).

**Pass criteria for every query:**
1. Result rows match the `-- expect:` comment
2. `EXPLAIN` shows `IcebergScanTransformer` (not `BatchScanExec` / `FileScan parquet`) — confirms Gluten offload

**Required launch setting:**

```bash
--conf spark.default.parallelism=1
```

Without this, `INSERT INTO ... VALUES (...)` produces one file per row. The root cause is `LocalTableScanExec` (Spark's executor for VALUES literals), which splits rows into `min(numRows, defaultParallelism)` partitions. In `local[*]` mode `defaultParallelism` equals the number of CPU cores, so a 5-row insert on a 5-core machine produces 5 partitions → 5 write tasks → 5 files. This must be set at launch time; it cannot be changed inside a running session.

---

## How MoR merging works internally

### How position delete files are generated

The core question is: how does the engine know the physical row position of each deleted row?

Iceberg exposes two **virtual metadata columns** during any scan:

```
_file  — absolute path of the data file containing this row
_pos   — 0-based physical row index within that file (equals the Parquet reader's row counter)
```

When you run `DELETE FROM t WHERE id IN (2, 8, 12)` on a MoR table, the execution proceeds as follows:

```
Step 1: Scan with metadata columns injected
  ┌────┬───────┬──────────────────────────┬──────┐
  │ id │ name  │ _file                    │ _pos │
  ├────┼───────┼──────────────────────────┼──────┤
  │  1 │ alice │ /data/00001.parquet      │  0   │
  │  2 │ bob   │ /data/00001.parquet      │  1   │  ← matches WHERE
  │  3 │ carol │ /data/00001.parquet      │  2   │
  │  8 │ henry │ /data/00002.parquet      │  0   │  ← matches WHERE
  │ 12 │ liam  │ /data/00003.parquet      │  1   │  ← matches WHERE
  └────┴───────┴──────────────────────────┴──────┘

Step 2: Filter — keep only (_file, _pos) pairs for matching rows
  (/data/00001.parquet, 1)
  (/data/00002.parquet, 0)
  (/data/00003.parquet, 1)

Step 3: PositionDeleteWriter writes these pairs as a sorted Parquet file
  → /data/delete/pos-del-00001.parquet  (sorted by file_path, pos — required by spec)

Step 4: Iceberg atomic commit — new delete file registered in the snapshot
  → original data files untouched
```

Key points:
- `_file` and `_pos` are injected by the Iceberg scan layer at runtime; they are not stored in the Parquet data files
- `_pos` is simply the Parquet reader's sequential row counter — no extra bookkeeping needed
- The DELETE writes only a tiny delete file, leaving data files completely untouched — this is the core MoR advantage over CoW

### Querying virtual metadata columns directly

All Iceberg metadata columns can be read in SQL by naming them explicitly. They do **not** appear in `SELECT *`.

```sql
-- Inspect physical row positions and file paths
SELECT _file, _pos, id, name
FROM local.db.gluten_test
ORDER BY _file, _pos
LIMIT 20;
```

Full set of available metadata columns:

| Column | Type | Description |
|---|---|---|
| `_file` | STRING | Absolute path of the data file containing this row |
| `_pos` | BIGINT | 0-based physical row index within that file |
| `_spec_id` | INT | Partition spec ID used when this row was written |
| `_partition` | STRUCT | Partition values for this row |
| `_deleted` | BOOLEAN | `true` if this row is covered by a position delete file (MoR tables only) |

`_deleted` is particularly useful for verifying that MoR delete files are being applied correctly:

```sql
-- Rows visible to a normal scan (surviving rows only)
SELECT count(*) FROM local.db.gluten_test;

-- All rows in data files including those covered by delete files
-- _deleted=true means the row exists in a data file but is masked by a position delete
SELECT _file, _pos, _deleted, id, name
FROM local.db.gluten_test
WHERE _deleted = true;
-- If position delete files exist, deleted rows appear here
-- After rewrite_data_files these rows are gone and this query returns 0 rows
```

Note: `_deleted` availability depends on the Iceberg version. If it raises an error, use the `record_count` comparison approach described in the [MoR is invisible in EXPLAIN](#mor-is-invisible-in-explain) section instead.

### Position delete file format

A position delete file is a Parquet file with exactly two columns:

```
file_path                                    pos
───────────────────────────────────────────  ───
/tmp/iceberg/data/00001.parquet              2    ← row 2 deleted
/tmp/iceberg/data/00001.parquet              7    ← row 7 deleted
/tmp/iceberg/data/00003.parquet              0    ← row 0 deleted
```

`pos` is the 0-based physical row index within the data file, in write order.

### Velox IcebergSplitReader merge algorithm

At runtime, each Spark task receives a split that bundles the data file path with its associated delete file paths. Velox performs the merge inside `IcebergSplitReader`:

```
Split (delivered at runtime):
  data_file:    /tmp/iceberg/data/00001.parquet
  delete_files: [/tmp/iceberg/delete/pos-del-00001.parquet]

Step 1: read delete file → build deleted_positions set
  deleted_positions = {2, 7}

Step 2: scan data file sequentially, track current_pos counter
  current_pos=0  row(id=1, alice)  → not in set → emit
  current_pos=1  row(id=2, bob)    → not in set → emit
  current_pos=2  row(id=3, carol)  → IN set     → skip ✗
  current_pos=3  row(id=4, dave)   → not in set → emit
  ...
  current_pos=7  row(id=8, henry)  → IN set     → skip ✗

Step 3: pack surviving rows into an Arrow ColumnarBatch and return to Spark
```

Velox caches the deleted-position lookup as a `SelectivityVector` (an in-memory row bitmap), which is passed directly into the Parquet reader so skipped rows are never decoded — the cost is one bitmap lookup per row, not a full column decode.

### Why this is NOT a join (position delete)

It is tempting to think of MoR as a join between the data file and the delete file, but position delete does not need one. The Iceberg spec requires position delete files to be **sorted by `(file_path, pos)`**, and the data file is read sequentially, so the merge can be done with a simple **two-pointer scan** in O(n):

```
data file (sequential):   pos = 0, 1, 2, 3, 4, 5, 6, 7, 8 …
delete file (pre-sorted): pos = 2, 7

two-pointer merge:
  data_pos=0 < delete_pos=2  → emit row 0
  data_pos=1 < delete_pos=2  → emit row 1
  data_pos=2 = delete_pos=2  → skip; delete_ptr++ → delete_pos=7
  data_pos=3 < delete_pos=7  → emit row 3
  …
  data_pos=7 = delete_pos=7  → skip; delete_ptr++ → delete_pos=∞
  data_pos=8                 → emit row 8
```

No hash table, no sort, no join — just two advancing pointers. Position delete is therefore cheap even at large scale.

### Two-pointer algorithm explained

The two-pointer (or merge-scan) technique is a classic algorithm that eliminates nested loops by **advancing two pointers over two sorted sequences simultaneously**. It requires both sequences to be sorted on the same key — which is exactly what Iceberg guarantees.

**Naïve approach — O(n × m):**

Without sorted sequences, every data row must be checked against every delete record:

```
for each data_row (i = 0..n):        ← outer loop, n iterations
    for each delete_pos (j = 0..m):  ← inner loop, m iterations
        if data_row.pos == delete_pos → skip
```

Cost: O(n × m). For 10 M data rows and 100 K deletes that is 10¹² comparisons.

**Two-pointer approach — O(n + m):**

Both sequences are sorted on `pos`. Each pointer only ever moves forward:

```
i = 0   // pointer into data file (pos = 0, 1, 2, 3 …)
j = 0   // pointer into delete file (sorted pos list)

while i < n and j < m:
    if data[i].pos < delete[j].pos:
        emit data[i]          // row is not deleted
        i++
    elif data[i].pos == delete[j].pos:
        skip data[i]          // row is deleted
        i++; j++
    else:                     // data[i].pos > delete[j].pos
        j++                   // advance delete pointer to catch up

while i < n:                  // remaining data rows are all surviving
    emit data[i]; i++
```

Each pointer advances at most n or m steps respectively. Total work: **O(n + m)**.

**Step-by-step trace** (data: 0–9, deletes: {2, 5, 8}):

```
data:    0  1  2  3  4  5  6  7  8  9
delete:        2        5        8

i=0: 0 < 2 → emit 0,  i=1
i=1: 1 < 2 → emit 1,  i=2
i=2: 2 = 2 → skip,    i=3, j=1  (delete ptr → 5)
i=3: 3 < 5 → emit 3,  i=4
i=4: 4 < 5 → emit 4,  i=5
i=5: 5 = 5 → skip,    i=6, j=2  (delete ptr → 8)
i=6: 6 < 8 → emit 6,  i=7
i=7: 7 < 8 → emit 7,  i=8
i=8: 8 = 8 → skip,    i=9, j=3  (delete ptr exhausted)
i=9: j exhausted → emit 9
```

Total steps: 10 (data) + 3 (delete) = 13 = n + m. ✓

**Why this works for Iceberg position deletes:**

| Sorted order | Source |
|---|---|
| Data file rows ordered by `pos` | Parquet reads rows sequentially — order is inherent |
| Delete file rows ordered by `(file_path, pos)` | Iceberg spec **mandates** this sort order at write time |

If the delete file were unsorted, the engine would need a HashSet (O(m) memory pre-load, O(1) per lookup) or fall back to O(n × m) nested loops. By requiring sorted delete files in the spec, Iceberg enables the optimal O(n + m) merge at no extra runtime cost.

### Equality delete IS like a join

Equality delete files record deleted rows by column values (e.g. `id = 5`) rather than physical positions. Since the engine cannot know which file or row holds `id=5` without reading, it must check every row of every data file against the equality predicate — effectively a broadcast join:

```
for each row in data file:
    if row matches any equality-delete predicate → skip
    else → emit
```

This is significantly more expensive than position delete, especially when the equality delete file is large or the predicate touches non-indexed columns. Equality deletes are mainly used in **schema evolution** scenarios (e.g. after a column rename, old delete files written against the old schema are kept as equality deletes). For regular `DELETE`/`UPDATE`/`MERGE` on a MoR table, Iceberg writes position deletes by default.

**Summary: MoR read overhead comes almost entirely from equality deletes, not position deletes.**

### How equality delete files are generated

Unlike position deletes, equality delete files record **column values** of deleted rows rather than their physical positions. The engine therefore does not need `_pos` — it only needs to know *what* was deleted, not *where*.

**When equality deletes are used:**

- **Flink CDC streaming writes** — the most common source. Flink receives a DELETE event from a CDC stream and knows the row values but not which Parquet file or position holds that row (the data may still be in-flight across multiple writer tasks). Flink's Iceberg sink writes equality delete files directly.
- **After partition spec evolution** — when the partition layout changes, old position delete files reference stale file paths. Iceberg may convert them to equality deletes during compaction.

**Generation mechanism:**

```
DELETE WHERE id = 5   (on a table configured with equality delete mode)

Step 1: identify equality fields (configured in table properties, typically the primary key)
  equality_fields = [id]

Step 2: extract equality field values from matching rows
  {id: 5}

Step 3: EqualityDeleteWriter writes a Parquet file containing only equality field columns
  ┌─────┐
  │ id  │   ← only equality-field columns; no _file, no _pos
  ├─────┤
  │  5  │
  └─────┘
  file metadata records: equality_field_ids = [1]  (Iceberg schema field ID)
```

**What happens if the equality delete file is too large to fit in memory?**

Velox loads the entire equality delete file into a HashSet before scanning data files. There is **no spill-to-disk fallback** — if the file exceeds available off-heap memory, the task fails with OOM.

In practice this is rarely a problem because equality delete files only store equality-field columns (typically a single integer primary key). One million delete records × 8 bytes = 8 MB — well within any reasonable memory budget.

It becomes a problem when:
- Equality fields include large string columns
- A Flink pipeline runs for a long time without triggering `rewrite_data_files`, letting delete records accumulate unbounded
- The table has an extremely high delete rate (e.g. short-lived log entries)

**Mitigations:**

```sql
-- Monitor equality delete file sizes (content=2)
SELECT content,
       count(*)                AS file_count,
       sum(file_size_in_bytes) AS total_bytes,
       sum(record_count)       AS total_records
FROM local.db.t.files
GROUP BY content;
-- Alert when content=2 total_bytes exceeds a few hundred MB

-- Materialise equality deletes before they grow large
CALL local.system.rewrite_data_files(table => 'local.db.t');
-- After rewrite, equality delete files are no longer referenced and can be expired
```

**Design guideline:** equality delete files are a temporary state — they must be cleared periodically by `rewrite_data_files`. Unlike position delete files (which are cheap to read), equality delete files should never be allowed to grow unbounded.

### Two "bitmaps" — do not confuse them

A common point of confusion: Iceberg v3 introduced **Puffin files** containing Bloom Filters, which are also called bitmaps. These are completely different:

| | Velox `SelectivityVector` | Iceberg v3 Puffin Bloom Filter |
|---|---|---|
| Lives in | Memory, ephemeral per task | Disk, `.puffin` statistics file |
| Purpose | Mark which rows are deleted; skip them at read time | Quickly determine whether a value exists in a file; skip entire files at planning time |
| Triggered | During data file scan, row by row | At query planning, before any file is opened |
| Iceberg version | Works on v2 (runtime implementation of position delete) | New in v3 |

The `SelectivityVector` is a pure **execution-engine optimisation** inside Velox — it is not an Iceberg format feature and has nothing to do with Puffin.

### What UPDATE does to data files and delete files (MoR)

A MoR UPDATE is **two simultaneous write operations** — it never rewrites the original data files:

```
UPDATE t SET score = score + 10 WHERE region = 'APAC' AND score < 70;
-- matches: dave (60→70), jack (66→76)
-- no match: grace (92), mia (91)  — score already ≥ 70
```

**Step 1 — Scan with virtual metadata columns to find matching rows:**

```
_file                    _pos   id   region  score
data/00001.parquet        3     4    APAC    60.0   ← dave  matches
data/00002.parquet        4     10   APAC    66.0   ← jack  matches
```

**Step 2 — Write a new position delete file marking the OLD rows as deleted:**

```
delete/pos-del-update-001.parquet  (new)
  file_path                 pos
  data/00001.parquet         3    ← old dave position
  data/00002.parquet         4    ← old jack position
```

**Step 3 — Write a new data file containing the UPDATED rows:**

```
data/00004.parquet  (new)
  id=4,  name=dave, score=70.0, region=APAC
  id=10, name=jack, score=76.0, region=APAC
```

**File state after UPDATE:**

```
data/00001.parquet              unchanged  (dave still at pos=3 with score=60)
data/00002.parquet              unchanged  (jack still at pos=4 with score=66)
data/00003.parquet              unchanged
data/00004.parquet              NEW — dave(70), jack(76)
delete/pos-del-delete-001.parquet  unchanged  (from prior DELETE)
delete/pos-del-update-001.parquet  NEW — marks old dave and jack positions deleted
```

**MoR merge at read time:**

```
scan data/00001.parquet:
  pos=3 (dave, score=60) → found in update delete file → SKIP  (old value hidden)

scan data/00004.parquet:
  dave(score=70), jack(score=76) → not in any delete file → EMIT  (new values visible)
```

After the UPDATE, `id=4` exists in **two** data files simultaneously. The old copy is masked by the delete file; only the new copy is visible. This duplication accumulates with every UPDATE and is the primary reason `rewrite_data_files` must be run periodically — it collapses multiple file versions and delete files into a single clean data file per logical row.

### How `write.update.mode` controls UPDATE file layout

The `write.update.mode` table property determines how UPDATE physically reorganises files. The two modes produce completely different file structures.

**`copy-on-write` (default):**

The entire affected data file is rewritten in one pass, baking the updated values directly into the output:

```
old 00001.parquet: alice, bob*, carol, dave(60), eve   (* bob already deleted)
       ↓ rewrite with update applied
new 00001.parquet: alice, carol, dave(70), eve          ← full rewrite, new value inline

net change: 1 file replaces 1 file  |  no new delete file
```

**`merge-on-read` (this table's configuration):**

The affected file is **split** into two parts rather than fully rewritten:

```
old 00001.parquet: alice, bob*, carol, dave(60), eve
       ↓ split
new 00001'.parquet: alice, carol, eve                   ← unchanged rows only (dave excluded)
A.parquet:          dave(70)                            ← updated value in a separate file

net change: 1 file becomes 2 files  |  no new delete file
```

The reader must merge `00001'` and `A` at scan time — this is what "merge **on read**" means for UPDATE: the updated version of a row is spread across two files, and the reader assembles the correct view.

**Why no position delete file for UPDATE under MoR:**

The old dave row is excluded by rewriting `00001` into `00001'` (which simply doesn't contain dave). There is no need to mark dave's position as deleted because the file that held him (`00001`) is removed from the snapshot entirely. Position delete files are only needed when a row is logically deleted from a file that **stays** in the snapshot.

**Trade-off:**

| | CoW UPDATE | MoR UPDATE |
|---|---|---|
| Affected file handling | Full rewrite (all rows + new values) | Split (unchanged rows only) |
| Updated values | Inline in the rewritten file | Separate new file |
| Position delete file generated | No | No |
| Write cost | High (rewrites all rows in affected files) | Low (writes only the touched rows) |
| Read cost | Low (one file per logical group) | Higher (multiple files to merge per scan) |
| Best for | Low update rate, read-heavy workloads | High update rate, write-heavy workloads |

**Observed file counts (verified 2026-05-20):**

After 3 INSERTs → DELETE (id IN 2,8,12) → UPDATE (APAC score < 70):

```
content=0 files=4:
  00001'.parquet  ← rewritten 00001 without dave  (alice, carol, eve)
  00002'.parquet  ← rewritten 00002 without jack  (frank, grace, iris)
  00003.parquet   ← unchanged
  A.parquet       ← new: dave(70), jack(76)

content=1 files=3:
  del-00001  ← marks bob  (ORPHANED: 00001 replaced; references a removed file → no-op)
  del-00002  ← marks henry (ORPHANED: 00002 replaced; references a removed file → no-op)
  del-00003  ← marks liam (ACTIVE: 00003 is unchanged)
```

### Code walkthrough: how write.update.mode is read and dispatched

The mode selection happens in `SparkRowLevelOperationBuilder.java` (Iceberg Spark integration):

```java
// SparkRowLevelOperationBuilder.java — build()
switch (mode) {
  case COPY_ON_WRITE:
    return new SparkCopyOnWriteOperation(...);   // CoW path
  case MERGE_ON_READ:
    return new SparkPositionDeltaOperation(...); // MoR path → WriteDeltaExec
}

// mode() reads the table property for each DML command:
case UPDATE:
  modeName = properties.getOrDefault(UPDATE_MODE, UPDATE_MODE_DEFAULT);
```

`SparkPositionDeltaOperation` (the MoR path) declares:
```java
// rowId() — tells Spark which metadata columns identify a row's position
public NamedReference[] rowId() {
  return new NamedReference[] {
    Expressions.column(MetadataColumns.FILE_PATH.name()),   // _file
    Expressions.column(MetadataColumns.ROW_POSITION.name()) // _pos
  };
}

// UPDATE is represented as DELETE (old pos) + INSERT (new values)
public boolean representUpdateAsDeleteAndInsert() { return true; }
```

In Spark's physical planner (`DataSourceV2Strategy`), this maps to `WriteDeltaExec` — a `V2ExistingTableWriteExec` that handles position-delta writes via `DeltaWrite`. Critically, Gluten's `OffloadIcebergWrite` only offloads `ReplaceDataExec`, `AppendDataExec`, `OverwriteByExpressionExec`, and `OverwritePartitionsDynamicExec` — **`WriteDeltaExec` is not in the offload list**.

**Verified physical plan for MoR UPDATE (EXPLAIN output, 2026-05-20):**

```
WriteDelta org.apache.iceberg.spark.source.SparkPositionDeltaWrite@...   ← vanilla Spark
+- VeloxColumnarToRow
   +- ColumnarExchange hashpartitioning(_spec_id, _partition, _file, 500), REBALANCE_PARTITIONS_BY_COL
      +- VeloxResizeBatches
         +- ^(1) ProjectExecTransformer [... (score + 10.0) AS _pre_2]     ← Velox native
            +- ^(1) ExpandExecTransformer                                  ← Velox native
               [[1, null,null,null,null, _file,_pos,_spec_id,_partition],  ← op=1: DELETE row
                [3, id,name,_pre_2,region, null,null,null,null]]           ← op=3: INSERT row
               +- ^(1) FilterExecTransformer (region=APAC AND score<70)    ← Velox native
                  +- ^(1) InputIteratorTransformer
                     +- RowToVeloxColumnar                                 ← format round-trip
                        +- *(1) ColumnarToRow
                           +- BatchScan [_file, _pos, _spec_id, _partition, ...]  ← NOT IcebergScanTransformer!
```

**Verified physical plan for MoR DELETE (EXPLAIN output, 2026-05-20):**

```
WriteDelta org.apache.iceberg.spark.source.SparkPositionDeltaWrite@...   ← vanilla Spark
+- VeloxColumnarToRow
   +- ColumnarExchange hashpartitioning(_spec_id, _partition, _file, 500), REBALANCE_PARTITIONS_BY_COL
      +- VeloxResizeBatches
         +- ^(1) ProjectExecTransformer [hash(...) AS hash_partition_key,
                                         1 AS __row_operation,            ← op=1 hardcoded (DELETE only)
                                         _file, _pos, _spec_id, _partition]
            +- ^(1) FilterExecTransformer id IN (2, 8, 12)               ← Velox native
               +- ^(1) InputIteratorTransformer
                  +- RowToVeloxColumnar                                   ← format round-trip
                     +- *(1) ColumnarToRow
                        +- BatchScan [id, _file, _pos, _spec_id, _partition]  ← NOT IcebergScanTransformer!
```

DELETE vs UPDATE plan differences:
- No `ExpandExecTransformer` — DELETE generates only `op=1` (DELETE) rows; no INSERT phase
- `ProjectExecTransformer` hardcodes `1 AS __row_operation` (no new values to compute)
- Only data column `id` is read (needed for the filter); no value columns needed

**Key findings from the actual plans (DELETE and UPDATE):**

**1. Scan falls back to `BatchScan` (not `IcebergScanTransformer`) for both DELETE and UPDATE**

Both MoR DELETE and UPDATE must read `_file`, `_pos`, `_spec_id`, `_partition` metadata columns to identify which physical rows to delete. `IcebergScanTransformer` does not support these virtual metadata columns, so Gluten cannot offload the scan for either operation. Vanilla Spark `BatchScan` is used instead.

**2. Inefficient format round-trip at the scan boundary**

```
BatchScan (columnar output)
  → ColumnarToRow     (columnar → row, for Spark vanilla compatibility)
  → RowToVeloxColumnar (row → Velox columnar, to feed Gluten operators)
```

Two conversions that cancel each other out — a known limitation when Gluten cannot replace the scan.

**3. `ExpandExecTransformer` runs in Velox**

Each row matching the WHERE clause is expanded into two rows by Gluten natively:
- `op=1` (DELETE): carries `_file`, `_pos` (position to delete) — data columns set to null
- `op=3` (INSERT): carries new column values — position columns set to null

**4. `WriteDelta` is vanilla Spark — not offloaded by Gluten**

`WriteDeltaExec` extends `V2ExistingTableWriteExec`, a completely different class from `WriteToDataSourceV2Exec`. Gluten's `OffloadIcebergWrite` only handles `ReplaceDataExec`, `AppendDataExec`, `OverwriteByExpressionExec`, and `OverwritePartitionsDynamicExec`. `WriteDeltaExec` is absent from the offload list, so the write runs row-by-row in vanilla Spark via `SparkPositionDeltaWrite` → Iceberg RowDelta commit.

**Gluten offload coverage for MoR UPDATE:**

| Plan node | Offloaded to Velox? | Reason |
|---|---|---|
| `BatchScan` | ❌ vanilla Spark | Needs `_file`, `_pos` metadata columns — `IcebergScanTransformer` does not support them |
| `FilterExecTransformer` | ✅ Velox | Standard columnar filter |
| `ProjectExecTransformer` (score+10) | ✅ Velox | Standard columnar project |
| `ExpandExecTransformer` | ✅ Velox | Gluten native expand for DELETE/INSERT rows |
| `ColumnarExchange` | ✅ Gluten | Columnar shuffle |
| `WriteDelta` | ❌ vanilla Spark | `WriteDeltaExec` not in Gluten offload list |

**Contrast with fully-offloaded operations:**

| Operation | Scan | Write | Gluten coverage |
|---|---|---|---|
| SELECT | `IcebergScanTransformer` ✅ | — | Full |
| INSERT | `IcebergScanTransformer` ✅ | `VeloxIcebergAppendDataExec` ✅ | Full |
| rewrite_data_files | `IcebergScanTransformer` ✅ | `VeloxIcebergReplaceDataExec` ✅ | Full |
| MoR DELETE | `BatchScan` ❌ | `WriteDeltaExec` ❌ | Filter only |
| MoR UPDATE | `BatchScan` ❌ | `WriteDeltaExec` ❌ | Filter/project/expand only |

**Summary of DML → write mode → physical exec → Gluten coverage:**

| DML | write mode | Iceberg operation | Spark exec | Gluten handles? | File effect |
|---|---|---|---|---|---|
| DELETE | MoR | `SparkPositionDeltaOperation` | `WriteDeltaExec` | ❌ scan yes, write no | position delete file added |
| UPDATE | MoR | `SparkPositionDeltaOperation` | `WriteDeltaExec` | ❌ scan no, write no | file split + new data file |
| MERGE | MoR | `SparkPositionDeltaOperation` | `WriteDeltaExec` | partial | mixed |
| rewrite_data_files | — | `SparkWrite.CopyOnWriteOperation` | `ReplaceDataExec` | ✅ full | files compacted, deletes materialised |

### Why `rewrite_data_files` eliminates delete files

`rewrite_data_files` is effectively a manual CoW pass over MoR data:

```
IcebergScanTransformer reads data + delete files → MoR merge in Velox → only surviving rows
  → VeloxIcebergReplaceDataExec writes new Parquet files (no deleted rows baked in)
  → Iceberg atomically commits: old data files + delete files replaced by new compact files
  → delete files are no longer referenced and can be expired
```

### What MERGE INTO does — and why it requires Iceberg

`MERGE INTO` is the SQL standard **upsert** statement: update matched rows, insert unmatched rows, and optionally delete rows — all in a single atomic operation.

```sql
MERGE INTO target t
USING source s ON t.id = s.id
WHEN MATCHED THEN
  UPDATE SET t.score = s.score           -- found → update
WHEN NOT MATCHED THEN
  INSERT (id, name, score) VALUES (...)  -- not found → insert
WHEN MATCHED AND t.score < 0 THEN
  DELETE                                 -- found + condition → delete
```

All three `WHEN` clauses are optional and can be combined freely.

**`MERGE INTO` is not part of vanilla Spark SQL.** The syntax is registered by `IcebergSparkSessionExtensions`. Without the Iceberg JAR and extension, Spark raises a parse error on `MERGE INTO`.

**MoR file changes for MERGE:**

```
MERGE INTO gluten_test t USING src s ON t.id = s.id
  WHEN MATCHED     → same as UPDATE: position delete (old row) + new data file (updated row)
  WHEN NOT MATCHED → same as INSERT: new data file only, no delete file
```

Under MoR, MERGE is implemented as a coordinated DELETE + INSERT — it never rewrites the original data files.

**Comparison with individual DML statements:**

| | `UPDATE` | `DELETE` | `MERGE INTO` |
|---|---|---|---|
| Can update values | yes | no | yes |
| Can delete rows | no | yes | yes |
| Can insert rows | no | no | yes |
| Requires a source table | no | no | yes |
| Vanilla Spark support | no (Iceberg ext) | no (Iceberg ext) | no (Iceberg ext) |
| MoR file effect | pos delete + insert | pos delete (or full-file drop) | pos delete + insert |

**Typical use case — CDC synchronisation:**

```sql
-- source: incremental CDC table (from Kafka landing zone)
-- target: main Iceberg table
MERGE INTO orders t
USING orders_cdc s ON t.order_id = s.order_id
WHEN MATCHED AND s.op = 'U' THEN UPDATE SET t.status = s.status, t.amount = s.amount
WHEN MATCHED AND s.op = 'D' THEN DELETE
WHEN NOT MATCHED AND s.op = 'I' THEN INSERT (order_id, status, amount) VALUES (s.order_id, s.status, s.amount);
```

This is the standard pattern for Flink/Spark real-time data warehouses: Iceberg as the storage layer, `MERGE INTO` as the SQL interface. The accumulated position delete files from repeated MERGE runs are the main driver for scheduling `rewrite_data_files` and `rewrite_position_delete_files` maintenance jobs.

### Why MERGE requires a JOIN but UPDATE does not

**UPDATE touches only one table — a filter is enough:**

```sql
UPDATE gluten_test SET score = score + 10
WHERE region = 'APAC' AND score < 70
-- Only one table. Scan target → apply WHERE filter → write.
-- No second table, no JOIN.
```

**MERGE touches two tables — a JOIN is mandatory to determine which rows match:**

```sql
MERGE INTO gluten_test t       ← target table
USING gluten_test_src s        ← source table (second table)
ON t.id = s.id                 ← match condition
WHEN MATCHED     THEN UPDATE ...
WHEN NOT MATCHED THEN INSERT ...
```

The engine cannot know which target rows match which source rows without scanning both tables and joining them on `t.id = s.id`. The join result drives the three-way split:

```
Scan target (with _file, _pos)  +  Scan source (plain)
           ↓
     JOIN ON t.id = s.id
           ↓
  ┌────────────────────────────────┬─────────────────────────────────┐
  │  MATCHED rows                  │  NOT MATCHED rows               │
  │  t.id=1 (alice) ↔ s.id=1      │  s.id=16 (peter) — no target   │
  │                                │  s.id=17 (quinn) — no target   │
  └────────────────────────────────┴─────────────────────────────────┘
           ↓                                    ↓
  ExpandExecTransformer                  INSERT rows only (op=3)
  DELETE row (op=1, old alice pos)
  INSERT row (op=3, alice-updated)
           ↓
     WriteDelta (SparkPositionDeltaWrite)
     op=1 → position delete file   op=3 → new data file
```

**Analogy:** UPDATE is self-checking — scan your own ledger and fix entries that match a condition. MERGE is cross-referencing — hold up a second ledger (source) against the first (target) row by row; update or delete what matches, insert what does not. The cross-referencing step is the JOIN.

**File changes for the test-plan MERGE (verified 2026-05-20):**

Source has 3 rows: `id=1` (matched → UPDATE alice), `id=16` and `id=17` (unmatched → INSERT).

```
MATCHED UPDATE (alice, id=1):
  Data file containing alice is rewritten without alice → new file 00001'
  New data file A: alice-updated(99.0), peter(85.0), quinn(78.0)

NOT MATCHED INSERT (peter id=16, quinn id=17):
  Written into the same new data file A above

Delete files: delete file for alice's old data file becomes orphaned
              (same pattern as MoR UPDATE — file rewrite eliminates the need
               for a new position delete file)
```

**Gluten offload for MERGE vs UPDATE vs DELETE:**

| Operation | Scan | JOIN | Write | Velox coverage |
|---|---|---|---|---|
| DELETE | `BatchScan` ❌ | none | `WriteDeltaExec` ❌ | Filter only |
| UPDATE | `BatchScan` ❌ | none | `WriteDeltaExec` ❌ | Filter / project / expand |
| MERGE | `BatchScan` ❌ | `ShuffledHashJoinExecTransformer` ✅ (if both sides columnar) | `WriteDeltaExec` ❌ | Join / filter / expand |

The MERGE JOIN can run in Velox if both the target scan (`BatchScan`) and the source scan produce columnar output that Gluten can feed into `ShuffledHashJoinExecTransformer`. Verify with `EXPLAIN MERGE INTO ...`.

### Impact of schema changes on delete files

The two delete file types behave very differently when the table schema evolves.

#### Position delete files — schema-transparent

Position delete files store only `(file_path, pos)` and have no dependency on column definitions:

| Schema change | Effect on position delete files |
|---|---|
| Add column | None — old rows return `null` for the new column; position filtering is unaffected |
| Rename column | None — Iceberg tracks columns by field ID, not name; position deletes don't store column info |
| Drop any column | None — position deletes reference row numbers only, never column values |

#### Equality delete files — tightly coupled to schema

Equality delete files store `field_id → value` pairs. They are safe for additive changes but dangerous when columns they reference are dropped:

**Add column — safe:**
```
equality delete file: {field_id=1 (id): value=5}
After adding column region (field_id=4):
  Apply delete: match on field_id=1 only; new column is ignored ✅
```

**Rename column — safe:**
```
equality delete file: {field_id=1: value=5}
Rename id → user_id: field_id remains 1
  Iceberg matches by field_id, not name ✅
```

**Drop a column referenced by equality delete — dangerous:**
```
equality delete file: {field_id=3 (score): value=80.0}
DROP COLUMN score → field_id=3 removed from schema

At read time: score column does not exist → value match impossible
  → equality delete silently stops applying
  → rows that were deleted become VISIBLE again ⚠️  (silent data corruption)
```

No error is raised. The equality delete file is simply skipped, and previously deleted rows reappear in query results.

#### Safe procedure before dropping a column

Always materialise equality deletes before dropping any column that appears in equality delete field IDs:

```sql
-- Step 1: materialise all equality deletes into data files
CALL local.system.rewrite_data_files(table => 'local.db.t');

-- Step 2: expire old snapshots so delete files are physically removed
CALL local.system.expire_snapshots(table => 'local.db.t', older_than => now(), retain_last => 1);

-- Step 3: now it is safe to drop the column
ALTER TABLE local.db.t DROP COLUMN score;
```

Skipping Steps 1–2 and dropping the column immediately causes silent data correctness failures — no exception, but deleted rows become queryable again.

#### Summary

| Schema change | Position delete | Equality delete |
|---|---|---|
| Add column | ✅ no effect | ✅ no effect |
| Rename column | ✅ no effect | ✅ no effect (matched by field ID) |
| Drop non-equality column | ✅ no effect | ✅ no effect |
| Drop equality-field column | ✅ no effect | ⚠️ delete silently invalidated; rows reappear |

### MoR is invisible in EXPLAIN

The delete file merge happens inside `IcebergScanTransformer` at the split level and is not reflected in the Spark physical plan. `EXPLAIN FORMATTED` shows the Velox native plan as a plain `TableScan` with no delete file paths — those are embedded in the split metadata delivered at runtime, not in the static plan.

To confirm MoR is active, compare data file row counts against actual query row counts:

```sql
-- total rows including deleted ones (sum of data file record_count)
SELECT sum(record_count) FROM local.db.gluten_test.files WHERE content = 0;

-- actual surviving rows after MoR filtering
SELECT count(*) FROM local.db.gluten_test;

-- difference = rows filtered by MoR at read time
```

---

## Observed behaviors and findings

### Finding 1 — MoR DELETE is correct even when no delete files appear

**Symptom:** After `DELETE FROM t WHERE id IN (2, 8, 12)` on a MoR table, the `.files` metadata table shows no `content=1` (position delete) files, and `total-delete-files=0`.

**Root cause — Iceberg file-level delete optimisation:** When `spark.sql.shuffle.partitions` is at its default value, Gluten's columnar write path spreads each INSERT batch across as many shuffle tasks as there are shuffle partitions. With 5 rows and default parallelism, each row lands in a separate task, producing **one data file per row**. The DELETE then finds that every file it touches contains *only* the deleted row — all rows in the file are gone — so Iceberg drops the file reference entirely from the new snapshot rather than writing a position delete file. This is valid, correct MoR behaviour: Iceberg never creates a delete file for a file that has been fully deleted.

**Evidence from snapshot summary:**
```
operation          = delete
deleted-data-files = 3        ← 3 entire files dropped
deleted-records    = 3        ← exactly the 3 rows requested
total-delete-files = 0        ← no delete file needed
```

**How to see position delete files:** Set `spark.sql.shuffle.partitions=1` so each INSERT batch produces a single multi-row file. Deleting a subset of rows from that file forces Iceberg to write a position delete file (`content=1`).

---

### Finding 4 — Why SELECT / INSERT / rewrite_data_files are fully Gluten-offloaded but DELETE / UPDATE are not

The difference comes down to whether the scan needs **virtual position metadata columns**.

**SELECT, INSERT source scan, rewrite_data_files — data columns only:**

```
SELECT * FROM t WHERE score > 80
  → read schema: id, name, score, region  (plain data columns)
  → IcebergScanTransformer validates OK ✅ → full Velox pipeline

rewrite_data_files
  → read schema: data columns only (MoR merge applied internally by Velox)
  → IcebergScanTransformer validates OK ✅ → full Velox pipeline
```

**MoR DELETE / UPDATE — must expose physical row position:**

```
DELETE FROM t WHERE id = 5
  → must produce a position delete file containing (file_path, row_pos) for id=5
  → read schema must include: _file, _pos, _spec_id, _partition
  → IcebergScanTransformer.doValidateInternal() detects these and returns:
       ValidationResult.failed("Read unsupported metadata column")
  → Gluten falls back to vanilla BatchScan ❌
```

**The exact validation code** (`IcebergScanTransformer.scala` line 103):

```scala
val allowedMetadataColumns =
  Set("input_file_name", "input_file_block_start", "input_file_block_length")

val hasUnsupportedMetadata = scan.readSchema().fieldNames.exists { f =>
  MetadataColumns.isMetadataColumn(f) &&
  !allowedMetadataColumns.contains(f.toLowerCase(Locale.ROOT))
}
if (hasUnsupportedMetadata) {
  return ValidationResult.failed("Read unsupported metadata column")  // ← triggers BatchScan fallback
}
```

Only three metadata column names are whitelisted. `_file`, `_pos`, `_spec_id`, and `_partition` are all absent from the whitelist.

**Why these columns cannot be trivially added to IcebergScanTransformer:**

Velox's `IcebergSplitReader` generates `_file` (current file path) and `_pos` (sequential row counter) internally while reading, and uses them to apply position delete files. However, it does **not** expose them as output columns in the Arrow `ColumnarBatch` returned to Spark. Supporting DELETE and UPDATE natively in Velox would require:

1. **Velox C++ layer**: expose `_file` and `_pos` as extra output columns in `IcebergSplitReader`'s returned `RowVector`
2. **Gluten JNI layer**: map those extra columns through the JNI boundary into Arrow format
3. **Gluten Scala layer**: add `_file`/`_pos` to `IcebergScanTransformer`'s output and whitelist
4. **New write executor**: implement `VeloxIcebergWriteDeltaExec` to offload `WriteDeltaExec` (position delete + data file write via Iceberg RowDelta) — currently no Velox equivalent exists

This is a **complete feature gap**, not a small patch. Until it is implemented, MoR DELETE and UPDATE will continue to use `BatchScan` (vanilla scan) and `WriteDeltaExec` (vanilla write), with only the intermediate operators (filter, project, expand) benefiting from Velox.

**Summary:**

| Operation | Needs `_file`/`_pos`? | Scan | Write | Fully Velox? |
|---|---|---|---|---|
| SELECT | No | `IcebergScanTransformer` ✅ | — | ✅ Yes |
| INSERT | No | `LocalTableScanExec` (VALUES) | `VeloxIcebergAppendDataExec` ✅ | ✅ Yes |
| rewrite_data_files | No (MoR applied internally) | `IcebergScanTransformer` ✅ | `VeloxIcebergReplaceDataExec` ✅ | ✅ Yes |
| MoR DELETE | Yes → validation fails | `BatchScan` ❌ | `WriteDeltaExec` ❌ | ❌ Partial |
| MoR UPDATE | Yes → validation fails | `BatchScan` ❌ | `WriteDeltaExec` ❌ | ❌ Partial |
| MERGE source | No | `IcebergScanTransformer` ✅ | — | ✅ Yes (source side) |
| MERGE target | Yes → validation fails | `BatchScan` ❌ | `WriteDeltaExec` ❌ | ❌ Partial |

### Finding 5 — Why MERGE source uses IcebergScanTransformer but UPDATE/DELETE cannot

**MERGE has two tables; UPDATE/DELETE have only one.**

In MERGE, the two responsibilities are split across two separate scan nodes:

```
Target scan — must expose physical row location (to write position delete entries):
  read schema: [id, region, _file, _pos, _spec_id, _partition]
  → _file and _pos present → doValidateInternal() fails → BatchScan ❌

Source scan — only needs to supply new values and drive the ON join condition:
  read schema: [id, name, score, region]   (plain data columns only)
  → no metadata columns → doValidateInternal() passes → IcebergScanTransformer ✅
```

The position delete file entry is `(target._file, target._pos)` — coordinates that come entirely from the **target** scan. The source contributes only column values (for UPDATE new values and INSERT rows). Because the two roles are assigned to two separate scans, the source scan never needs metadata columns.

**UPDATE/DELETE have a single table that must serve both roles simultaneously:**

```
DELETE FROM t WHERE id IN (2, 8, 12)
  One scan, two requirements:
    data columns:     id           ← needed for WHERE filter
    metadata columns: _file, _pos  ← needed to write position delete file
  Combined read schema: [id, _file, _pos, _spec_id, _partition]
  → _file/_pos detected → validation fails → BatchScan ❌
```

There is no second table to offload the "physical location" responsibility to. A single scan cannot satisfy both roles with `IcebergScanTransformer` as it is today.

**Analogy:** MERGE is two workers collaborating — one looks up addresses (target, needs metadata), the other fetches goods (source, needs values only). Gluten accelerates the worker who only fetches goods. UPDATE/DELETE is one worker who must both look up addresses and fetch goods at the same time — Gluten cannot accelerate that worker until `IcebergScanTransformer` supports exposing `_file`/`_pos` as output columns.

**Verified with EXPLAIN (2026-05-20):** Even after setting `write.delete.mode=merge-on-read` on the source table and deleting a row (so the source has a position delete file), the MERGE plan still shows `IcebergScanTransformer` for the source scan. Having delete files on the source does **not** cause a fallback — Velox's `IcebergSplitReader` applies them transparently at the C++ layer without surfacing them to the Spark plan.

### Finding 3 — MoR DELETE and UPDATE are only partially Gluten-offloaded

**Symptom:** `EXPLAIN DELETE` and `EXPLAIN UPDATE` on a MoR table show `BatchScan` (not `IcebergScanTransformer`) at the scan layer and `WriteDelta` (not any Velox exec) at the write layer. Neither operation achieves full Gluten offload.

**Root cause — metadata columns required by position delta:**

MoR DELETE and UPDATE must identify the physical location of each affected row to write position delete files. Iceberg achieves this by injecting virtual metadata columns into the scan:

```
_file      — which Parquet file contains this row
_pos       — row's physical index within that file
_spec_id   — partition spec ID
_partition — partition values
```

`IcebergScanTransformer` (Gluten's native Iceberg scan) does not support these metadata columns — it only handles regular data columns. When the planner detects that `_file` or `_pos` are needed, it falls back to vanilla Spark `BatchScan`.

Additionally, `WriteDeltaExec` (which writes position delete files via Iceberg's `SparkPositionDeltaWrite.RowDelta`) is not in Gluten's `OffloadIcebergWrite` list — the offload rules only cover `ReplaceDataExec`, `AppendDataExec`, `OverwriteByExpressionExec`, and `OverwritePartitionsDynamicExec`.

**Actual plan structure (confirmed with EXPLAIN, 2026-05-20):**

```
DELETE plan:
  WriteDelta (SparkPositionDeltaWrite)          ← vanilla Spark write
    VeloxColumnarToRow
      ColumnarExchange (REBALANCE by _file)     ← Gluten columnar shuffle
        ^(1) ProjectExecTransformer             ← Velox: hardcode op=1, pass _file/_pos
          ^(1) FilterExecTransformer            ← Velox: id IN (2,8,12)
            RowToVeloxColumnar ← ColumnarToRow  ← format round-trip (inefficiency)
              BatchScan [id, _file, _pos, ...]  ← vanilla Spark scan

UPDATE plan:
  WriteDelta (SparkPositionDeltaWrite)          ← vanilla Spark write
    VeloxColumnarToRow
      ColumnarExchange (REBALANCE by _file)     ← Gluten columnar shuffle
        ^(1) ExpandExecTransformer              ← Velox: expand row → DELETE+INSERT pair
          ^(1) ProjectExecTransformer           ← Velox: compute new values (score+10)
            ^(1) FilterExecTransformer          ← Velox: region=APAC AND score<70
              RowToVeloxColumnar ← ColumnarToRow ← format round-trip
                BatchScan [data cols, _file, _pos, ...] ← vanilla Spark scan
```

**Gluten coverage comparison:**

| Operator layer | DELETE | UPDATE | SELECT / INSERT / rewrite |
|---|---|---|---|
| Scan | ❌ `BatchScan` | ❌ `BatchScan` | ✅ `IcebergScanTransformer` |
| Filter / Project | ✅ Velox | ✅ Velox | ✅ Velox |
| Expand (DELETE+INSERT) | n/a | ✅ Velox | n/a |
| Shuffle | ✅ `ColumnarExchange` | ✅ `ColumnarExchange` | ✅ `ColumnarExchange` |
| Write | ❌ `WriteDeltaExec` | ❌ `WriteDeltaExec` | ✅ Velox exec |

**Impact:** MoR DELETE and UPDATE incur vanilla Spark overhead at both ends of the pipeline. The intermediate compute (filter, project, expand) benefits from Velox, but the format round-trip (`BatchScan` → `ColumnarToRow` → `RowToVeloxColumnar`) adds unnecessary conversion cost. For write-heavy MoR workloads, `rewrite_data_files` (which is fully Gluten-accelerated) should be run frequently to limit the accumulation of delete files.

### Finding 2 — One file per row with default parallelism

**Symptom:** Three `INSERT` batches of 5 rows each produce 15 data files total (5 files per batch, 1 row per file) instead of the expected 3 files (1 per batch).

**Root cause:** `INSERT INTO ... VALUES (...)` is executed by Spark's `LocalTableScanExec`, which splits the rows into partitions using:

```
numPartitions = min(numRows, sparkContext.defaultParallelism)
```

In `local[*]` mode, `defaultParallelism` equals the number of CPU cores. On a machine with ≥5 cores, 5 rows → 5 partitions. Gluten's `ColumnarV2TableWriteExec` calls `query.executeColumnar()` and runs one write task per RDD partition, so 5 partitions → 5 files. This has nothing to do with shuffle — `spark.sql.shuffle.partitions` does not affect `LocalTableScanExec`.

**Fix:** Add `--conf spark.default.parallelism=1` to the `spark-sql` launch command. This forces `LocalTableScanExec` to produce a single partition regardless of row count or core count, so a 5-row INSERT produces a single 5-row Parquet file. This setting cannot be changed inside a running session.

**Impact on tests:** Any test that checks file counts, position delete behaviour, or `rewrite_data_files` compaction ratios must be run with `spark.default.parallelism=1`. Tests that only verify query results are not affected.

---

## Reading EXPLAIN output

### `^(N)` — WholeStageCodegenTransformer segment number

`^(N)` marks a **contiguous group of Velox-native operators** that run entirely in C++ without returning to the JVM. Each number is a separate native segment.

```
^(1) IcebergScanTransformer                  ┐
^(1) FlushableHashAggregateTransformer        ├─ Velox native segment 1 (Stage 1)
^(1) ProjectExecTransformer                  ┘
     ColumnarExchange                         ←  JVM layer — Shuffle boundary
     VeloxResizeBatches                       ←  JVM layer — batch size adapter
^(2) InputIteratorTransformer                ┐
^(2) HashAggregateTransformer                ├─ Velox native segment 2 (Stage 2)
     VeloxColumnarToRow                       ←  JVM layer — final Row conversion
```

**`^(N)` vs Spark Stage:**

| | Split by | Relationship |
|---|---|---|
| Stage | Shuffle boundary (DAGScheduler) | 1 Stage ≥ 1 `^(N)` segment |
| `^(N)` | Contiguous Velox operators | Can be multiple per Stage if a JVM operator breaks the chain |

In the query above, Stage 1 and `^(1)` happen to coincide — but they are independent concepts.

**Operators with no `^(N)` prefix run in the JVM layer:**

| Operator | Why it is JVM-side |
|---|---|
| `ColumnarExchange` | Shuffle must go through JVM scheduling (DAGScheduler, BlockManager); Velox has no shuffle implementation |
| `VeloxResizeBatches` | Inserted after Shuffle read to resize Arrow batches before feeding the next Velox segment; acts as a JVM-side adapter |
| `VeloxColumnarToRow` | Final conversion from Arrow columnar to Spark `InternalRow` for the Driver to collect |

**Quick check:** a healthy Gluten plan has `IcebergScanTransformer` inside a `^(1)` block and only one `VeloxColumnarToRow` at the root. Any `BatchScanExec`, `FileScan`, or `RowToColumnarExec` in the plan means Gluten did not offload that operator.

---

## Feature coverage

| Feature | Gluten node | Test |
|---|---|---|
| SELECT / scan | `IcebergScanTransformer` | T-2, T-9 |
| INSERT | `VeloxIcebergAppendDataExec` | T-1 |
| DELETE (MoR) | `VeloxIcebergReplaceDataExec` + `IcebergScanTransformer` | T-3 |
| UPDATE (MoR) | `VeloxIcebergReplaceDataExec` + `IcebergScanTransformer` | T-4 |
| MERGE (MoR) | `VeloxIcebergReplaceDataExec` + `IcebergScanTransformer` | T-5 |
| rewrite_position_delete_files | `IcebergScanTransformer` + native write | T-6 |
| rewrite_data_files | `VeloxIcebergReplaceDataExec` | T-7 |
| expire_snapshots | — (catalog-only) | T-8 |
| Aggregation offload | `FlushableHashAggregateExecTransformer` | T-2 |
| Sort offload | `SortExecTransformer` | T-2, T-9 |
| Join offload | `ShuffledHashJoinExecTransformer` | T-9 |
| Time travel | `IcebergScanTransformer` | T-2 |

---

## T-0: Setup

```sql
DROP TABLE IF EXISTS local.db.gluten_test;
CREATE TABLE local.db.gluten_test (
  id     BIGINT,
  name   STRING,
  score  DOUBLE,
  region STRING
) USING iceberg
TBLPROPERTIES (
  'format-version'    = '2',
  'write.delete.mode' = 'merge-on-read',
  'write.update.mode' = 'merge-on-read',
  'write.merge.mode'  = 'merge-on-read'
);
```

---

## T-1: INSERT — VeloxIcebergAppendDataExec

```sql
-- Three separate inserts → three data files
INSERT INTO local.db.gluten_test VALUES
  (1, 'alice',   95.0, 'US'),
  (2, 'bob',     80.0, 'EU'),
  (3, 'carol',   70.0, 'US'),
  (4, 'dave',    60.0, 'APAC'),
  (5, 'eve',     55.0, 'EU');

INSERT INTO local.db.gluten_test VALUES
  (6, 'frank',   88.0, 'US'),
  (7, 'grace',   92.0, 'APAC'),
  (8, 'henry',   45.0, 'EU'),
  (9, 'iris',    77.0, 'US'),
  (10, 'jack',   66.0, 'APAC');

INSERT INTO local.db.gluten_test VALUES
  (11, 'kate',   83.0, 'US'),
  (12, 'liam',   39.0, 'EU'),
  (13, 'mia',    91.0, 'APAC'),
  (14, 'noah',   58.0, 'US'),
  (15, 'olivia', 74.0, 'EU');

-- V1: row count
SELECT count(*) FROM local.db.gluten_test;
-- expect: 15

-- V2: file layout (3 data files, 0 delete files)
SELECT content, count(*) AS files FROM local.db.gluten_test.files GROUP BY content;
-- expect: content=0  files=3

-- V3: Gluten plan check
EXPLAIN SELECT * FROM local.db.gluten_test WHERE region = 'US';
-- expect: IcebergScanTransformer in plan
```

---

## T-2: SELECT — read path, aggregation, time travel

```sql
-- V1: filter + sort
SELECT id, name, score FROM local.db.gluten_test WHERE score > 80 ORDER BY score DESC;
-- expect (5 rows):
--  1  alice   95.0
--  7  grace   92.0
-- 13  mia     91.0
--  6  frank   88.0
-- 11  kate    83.0

-- V2: aggregation offload
SELECT region, count(*) AS cnt, round(avg(score), 1) AS avg_score
FROM local.db.gluten_test
GROUP BY region
ORDER BY region;
-- expect:
-- APAC  4  72.3
-- EU    5  58.6
-- US    6  80.5

-- V3: time travel — only first batch visible at snapshot 1
-- FOR SYSTEM_VERSION AS OF requires a literal, not a subquery.
-- First fetch the snapshot id:
SELECT snapshot_id, committed_at FROM local.db.gluten_test.snapshots ORDER BY committed_at LIMIT 3;
-- Then substitute the first snapshot_id as a literal (replace 1111111111111111111 with actual id):
SELECT count(*) FROM local.db.gluten_test FOR SYSTEM_VERSION AS OF 1111111111111111111;
-- expect: 5  (only batch 1 visible)

-- V4: Gluten plan on aggregation
EXPLAIN SELECT region, max(score) FROM local.db.gluten_test GROUP BY region;
-- expect: FlushableHashAggregateExecTransformer in plan
```

---

## T-3: DELETE — MoR, position delete file creation

```sql
-- Deletes 3 rows across different data files → one position delete file per touched file
DELETE FROM local.db.gluten_test WHERE id IN (2, 8, 12);

-- V1: row count
SELECT count(*) FROM local.db.gluten_test;
-- expect: 12

-- V2: delete files created (content=1)
SELECT content, count(*) AS files FROM local.db.gluten_test.files GROUP BY content;
-- expect: content=0  files=3 | content=1  files=3

-- V3: deleted rows absent
SELECT id FROM local.db.gluten_test WHERE id IN (2, 8, 12);
-- expect: 0 rows

-- V4: MoR scan — delete files applied at native Velox layer
EXPLAIN SELECT * FROM local.db.gluten_test ORDER BY id;
-- expect: IcebergScanTransformer (no RowToColumnar, no filter in JVM)
```

---

## T-4: UPDATE — MoR, additional delete files

```sql
-- MoR UPDATE: position-deletes old rows, inserts updated rows as new data
UPDATE local.db.gluten_test SET score = score + 10 WHERE region = 'APAC' AND score < 70;
-- Affected: dave(60→70), jack(66→76)

-- V1: APAC rows
SELECT id, name, score FROM local.db.gluten_test WHERE region = 'APAC' ORDER BY id;
-- expect:
--  4  dave   70.0
--  7  grace  92.0
-- 10  jack   76.0
-- 13  mia    91.0
-- actual (verified 2026-05-20): 4 dave 70.0 | 7 grace 92.0 | 10 jack 76.0 | 13 mia 91.0 ✅

-- V2: total row count unchanged
SELECT count(*) FROM local.db.gluten_test;
-- expect: 12
-- actual: 12 ✅

-- V3: file layout after UPDATE
SELECT content, count(*) AS files FROM local.db.gluten_test.files GROUP BY content;
-- expect: content=0 files=4 | content=1 files=3
-- actual: content=0 files=4 | content=1 files=3 ✅
--
-- Why content=1 is still 3 (UPDATE added 0 new delete files):
--
-- UPDATE does NOT use "position delete + insert" (pure RowDelta/MoR).
-- Instead it uses "file rewrite + insert":
--
--   Old 00001.parquet: alice, bob*, carol, dave, eve   (* already masked by del-00001)
--   Old 00002.parquet: frank, grace, henry*, iris, jack
--
--   UPDATE rewrites those two files EXCLUDING the rows being updated:
--     00001'.parquet → alice, carol, eve          (dave excluded; bob already excluded)
--     00002'.parquet → frank, grace, iris          (jack excluded; henry already excluded)
--     A.parquet      → dave(70), jack(76)          (the new values)
--
--   Old 00001 and 00002 removed from snapshot; old dave/jack simply no longer
--   exist in any active data file — no position delete file needed.
--
--   The 3 delete files are all from the prior DELETE:
--     del-00001 → marks bob  (NOW ORPHANED: 00001 was replaced; no-op at read time)
--     del-00002 → marks henry (NOW ORPHANED: 00002 was replaced; no-op at read time)
--     del-00003 → marks liam  (still active; 00003 is unchanged)
--
-- content=0 files=4: 00001' + 00002' + 00003 + A  ✅
-- content=1 files=3: del-00001(orphan) + del-00002(orphan) + del-00003(active) ✅
```

---

## T-5: MERGE — upsert, mixed matched/not-matched

```sql
CREATE TABLE local.db.gluten_test_src (
  id     BIGINT,
  name   STRING,
  score  DOUBLE,
  region STRING
) USING iceberg
TBLPROPERTIES ('format-version' = '2');

INSERT INTO local.db.gluten_test_src VALUES
  (1,  'alice-updated', 99.0, 'US'),   -- matched → UPDATE
  (16, 'peter',         85.0, 'EU'),   -- new     → INSERT
  (17, 'quinn',         78.0, 'APAC'); -- new     → INSERT

MERGE INTO local.db.gluten_test t
USING local.db.gluten_test_src s ON t.id = s.id
WHEN MATCHED THEN
  UPDATE SET t.name = s.name, t.score = s.score
WHEN NOT MATCHED THEN
  INSERT (id, name, score, region) VALUES (s.id, s.name, s.score, s.region);

-- V1: updated + inserted rows
SELECT id, name, score FROM local.db.gluten_test WHERE id IN (1, 16, 17) ORDER BY id;
-- expect:
--  1  alice-updated  99.0
-- 16  peter          85.0
-- 17  quinn          78.0

-- V2: total count
SELECT count(*) FROM local.db.gluten_test;
-- expect: 14
```

---

## T-6: rewrite_position_delete_files

```sql
-- V1: file state before compaction
SELECT content, count(*) AS files, sum(file_size_in_bytes) AS bytes
FROM local.db.gluten_test.files
GROUP BY content;
-- expect: content=1  files > 1  (multiple small delete files)

-- Compact position delete files
CALL local.system.rewrite_position_delete_files(
  table => 'local.db.gluten_test'
);
-- columns: (rewritten_bytes, added_bytes, rewritten_delete_files, added_delete_files)
-- expect: rewritten_delete_files > 0,  added_delete_files < rewritten_delete_files

-- V2: fewer delete files after compaction
SELECT content, count(*) AS files FROM local.db.gluten_test.files GROUP BY content;
-- expect: content=1  files=1  (compacted into one)

-- V3: data unchanged
SELECT count(*) FROM local.db.gluten_test;
-- expect: 14

-- V4: deletes still in effect
SELECT id FROM local.db.gluten_test WHERE id IN (2, 8, 12);
-- expect: 0 rows
```

---

## T-7: rewrite_data_files — file compaction + delete materialisation

```sql
-- V1: file state before compaction
SELECT content, count(*) AS files FROM local.db.gluten_test.files GROUP BY content;
-- expect: content=0  multiple data files | content=1  delete files present

-- Compact data files (binpack strategy)
CALL local.system.rewrite_data_files(
  table    => 'local.db.gluten_test',
  strategy => 'binpack',
  options  => map(
    'target-file-size-bytes', '134217728',
    'min-input-files',        '2'
  )
);
-- columns: (rewritten_bytes, added_bytes, rewritten_files, added_files, failed_data_files)
-- expect: rewritten_files >= 2,  added_files < rewritten_files,  failed_data_files=0

-- V2: delete files gone (deletes materialised into new data files)
SELECT content, count(*) AS files FROM local.db.gluten_test.files GROUP BY content;
-- expect: content=0  files=1,  content=1  absent

-- V3: deleted rows still absent after materialisation
SELECT id FROM local.db.gluten_test WHERE id IN (2, 8, 12);
-- expect: 0 rows

-- V4: total row count unchanged
SELECT count(*) FROM local.db.gluten_test;
-- expect: 14

-- V5: full correctness check
SELECT id, name, score FROM local.db.gluten_test ORDER BY id;
-- expect: 14 rows
--   id=1   alice-updated  99.0
--   ids 2, 8, 12 absent
--   id=16  peter          85.0
--   id=17  quinn          78.0

-- V6: Gluten plan on post-rewrite scan (no delete files to join)
EXPLAIN SELECT * FROM local.db.gluten_test WHERE score > 70 ORDER BY score DESC;
-- expect: IcebergScanTransformer, no delete file paths in native plan
```

---

## T-8: expire_snapshots

```sql
-- V1: snapshot history
SELECT snapshot_id, operation, committed_at
FROM local.db.gluten_test.snapshots
ORDER BY committed_at;
-- expect: multiple snapshots (append, delete, overwrite, replace, ...)

-- Expire all but the latest
CALL local.system.expire_snapshots(
  table       => 'local.db.gluten_test',
  older_than  => now(),
  retain_last => 1
);

-- V2: only 1 snapshot remains
SELECT count(*) FROM local.db.gluten_test.snapshots;
-- expect: 1

-- V3: data still readable after expiry
SELECT count(*) FROM local.db.gluten_test;
-- expect: 14
```

---

## T-9: Gluten offload verification (EXPLAIN checks)

Every plan below must contain `IcebergScanTransformer` and `VeloxColumnarToRow` at the root.
None should contain `BatchScanExec`, `FileScan parquet`, or `RowToColumnar`.

```sql
-- Scan with filter
EXPLAIN SELECT * FROM local.db.gluten_test WHERE score BETWEEN 70 AND 90 ORDER BY score;

-- Aggregation
EXPLAIN SELECT region, max(score), min(score) FROM local.db.gluten_test GROUP BY region;

-- Join (both sides go through IcebergScanTransformer)
EXPLAIN
SELECT a.id, a.name, b.score AS src_score
FROM local.db.gluten_test a
JOIN local.db.gluten_test_src b ON a.id = b.id;
-- expect: ShuffledHashJoinExecTransformer (forceShuffledHashJoin=true)

-- Window function
EXPLAIN
SELECT id, name, score,
       rank() OVER (PARTITION BY region ORDER BY score DESC) AS rnk
FROM local.db.gluten_test;
```

---

## T-10: Teardown

```sql
DROP TABLE IF EXISTS local.db.gluten_test;
DROP TABLE IF EXISTS local.db.gluten_test_src;
```
