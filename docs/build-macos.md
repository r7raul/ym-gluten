# Building Gluten on macOS (Apple Silicon) — eBay Environment

This guide covers building the full Gluten + Velox stack on macOS arm64 with the eBay internal Maven repository (`raptor2`). It documents all prerequisites, build steps, and known fixes required as of May 2026.

---

## Prerequisites

### System Tools

```bash
brew install ccache ninja cmake pkg-config
```

### Java

Java 17 is required (Spark 3.5 target).

```bash
# Check available JDKs
/usr/libexec/java_home -V

# Set Java 17 for all build steps
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
```

### Maven Local Repository

All eBay builds use `~/.m2/raptor2` as the local Maven repository, configured in `~/.m2/settings.xml` with the `ebaycentral.qa.ebay.com` mirror.

---

## Build Order

The full build requires the following steps in order:

1. Build Velox (C++ engine)
2. Build Arrow `15.0.0-gluten` (Java JARs)
3. Build eBay Spark `3.5.0-ebay.0-SNAPSHOT` (catalyst + sql modules)
4. Build Gluten C++ native library (`libgluten.dylib`)
5. Build Gluten Java/Scala modules

---

## Step 1: Build Velox

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
export INSTALL_PREFIX=/Users/jijtang/IdeaProjects/ym-gluten/ep/build-velox/build/velox_ep/scripts/deps-install

cd /Users/jijtang/IdeaProjects/ym-gluten/ep/build-velox/src
./build-velox.sh \
  --velox_home=/Users/jijtang/IdeaProjects/ym-gluten/ep/build-velox/build/velox_ep \
  --build_type=release
```

### Known Fixes Applied to Velox Build

**`build-velox.sh` — `CXX_FLAGS`** (add deprecated literal operator suppression):
```bash
CXX_FLAGS='... -Wno-deprecated-literal-operator'
```

**`fix-ep-policy.py`** — Arrow patches for macOS:
- Fix 1: Add `CMAKE_POLICY_VERSION_MINIMUM=3.5` to Arrow EP cmake args
- Fix 2: Skip `BOOST_PROCESS_NEED_SOURCE` for Boost >= 1.86
- Fix 3: Replace `BOOST_PROCESS_V2_ASIO_NAMESPACE` with `boost::asio` in `cpp/src/arrow/testing/process.cc`

**`cmake-arrow-patch-cmd.patch`** — Add `ARROW_FILESYSTEM=ON` to Arrow EP cmake args so filesystem headers are installed.

**`cpp/CMake/ConfigArrow.cmake`** — Make `libarrow_bundled_dependencies.a` optional (not generated on macOS with system deps):
```cmake
function(FIND_ARROW_LIB LIB_NAME)
  cmake_parse_arguments(ARG "OPTIONAL" "" "" ${ARGN})
  # ... OPTIONAL keyword skips FATAL_ERROR if not found
```

**`cpp/core/CMakeLists.txt`** — Conditional link for bundled deps:
```cmake
if(TARGET Arrow::arrow_bundled_dependencies)
  target_link_libraries(gluten PUBLIC Arrow::arrow Arrow::arrow_bundled_dependencies)
else()
  target_link_libraries(gluten PUBLIC Arrow::arrow)
endif()
```

---

## Step 2: Build Arrow 15.0.0-gluten

Arrow `15.0.0-gluten` JARs are Gluten-specific and not in the eBay Maven repository. They must be built from source.

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
export INSTALL_PREFIX=/usr/local
export SUDO="sudo"

cd /Users/jijtang/IdeaProjects/ym-gluten/dev
bash build-arrow.sh
```

The script downloads Arrow 15.0.0, applies Gluten patches, builds C++ and Java, and installs `arrow-*:15.0.0-gluten` JARs to `~/.m2/raptor2`.

### Key Fixes in `dev/build-arrow.sh`

1. **Enable Parquet** for the C++ build — `jni_wrapper.cc` uses `ParquetFileFormat` unconditionally:
   ```bash
   -DARROW_PARQUET=ON  # changed from OFF
   ```

2. **eBay Maven repo** for the Java build — prevents SSL failures downloading from external repos:
   ```bash
   MVN_CMD="${CURRENT_DIR}/../build/mvn -Dmaven.repo.local=${HOME}/.m2/raptor2 -P ebay"
   ```

3. **`fix-ep-policy.py`** — patches `cpp/src/arrow/testing/process.cc` in the downloaded Arrow source:
   ```python
   # Replace BOOST_PROCESS_V2_ASIO_NAMESPACE (undefined in Boost >= 1.80 integrated)
   old3 = 'namespace asio = BOOST_PROCESS_V2_ASIO_NAMESPACE;'
   new3 = 'namespace asio = boost::asio;'
   ```

---

## Step 3: Build eBay Spark (sql/catalyst + sql/core)

The eBay Spark `3.5.0-ebay.0-SNAPSHOT` published JARs in raptor2 do not contain `KeyGroupedPartitioning` in `org.apache.spark.sql.catalyst.plans.physical`. Build from source:

```bash
cd /Users/jijtang/IdeaProjects/ebay-spark

# Fix: sun.security.action not accessible in Java 17
# Edit core/src/main/scala/org/apache/spark/serializer/SerializationDebugger.scala:
# Replace:
#   !AccessController.doPrivileged(new sun.security.action.GetBooleanAction(...))
# With:
#   !java.lang.Boolean.getBoolean("sun.io.serialization.extendedDebugInfo")

./build/mvn \
  -Dmaven.repo.local=/Users/jijtang/.m2/raptor2 \
  -pl sql/catalyst,sql/core -am \
  -DskipTests \
  -rf :spark-core_2.12 \
  install
```

This installs `spark-catalyst_2.12:3.5.0-ebay.0-SNAPSHOT` and `spark-sql_2.12:3.5.0-ebay.0-SNAPSHOT` with SPJ (Storage Partition Join) APIs into raptor2.

---

## Step 4: Build Gluten Java/Scala (full reactor install)

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 17)

cd /Users/jijtang/IdeaProjects/ym-gluten
./build/mvn \
  -Dmaven.repo.local=/Users/jijtang/.m2/raptor2 \
  -P spark-3.5,backends-velox,scala-2.12 \
  -DskipTests=true \
  install
```

> **Note:** The `iceberg` and `iceberg-test` profiles require `org.apache.iceberg:iceberg-open-api:1.6.1.1.9.0` which is not in any accessible repository. Use dummy JARs for test-scoped dependencies if needed:
> ```bash
> for classifier in tests test-fixtures; do
>   mvn install:install-file \
>     -Dmaven.repo.local=/Users/jijtang/.m2/raptor2 \
>     -Dfile=empty.jar \
>     -DgroupId=org.apache.iceberg \
>     -DartifactId=iceberg-open-api \
>     -Dversion=1.6.1.1.9.0 \
>     -Dpackaging=jar \
>     -Dclassifier=$classifier
> done
> ```

---

## Step 5: Build Gluten C++ Native Library

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
export INSTALL_PREFIX=/Users/jijtang/IdeaProjects/ym-gluten/ep/build-velox/build/velox_ep/scripts/deps-install

cd /Users/jijtang/IdeaProjects/ym-gluten/dev
./buildbundle-veloxbe.sh \
  --run_setup_script=OFF \
  --build_arrow=OFF \
  --spark_version=3.5 \
  --velox_home=/Users/jijtang/IdeaProjects/ym-gluten/ep/build-velox/build/velox_ep \
  build_gluten_cpp
```

This produces:
- `cpp/build/releases/libgluten.dylib` (~15MB)
- `cpp/build/releases/libvelox.dylib` (~138MB)

### Known Fixes in Gluten C++ CMake

**`cpp/core/CMakeLists.txt`** — Link zlib (required by Arrow GZip codec):
```cmake
find_package(ZLIB)
if(ZLIB_FOUND)
  target_link_libraries(gluten PUBLIC ZLIB::ZLIB)
endif()
```

Without this, the linker fails with undefined symbols `_deflate`, `_inflate`, etc. from `libarrow.a`.

**fmt v11/v12 mismatch after Homebrew upgrade** — If `brew upgrade` updated `fmt` to v12 after the velox_ep deps were originally built, the velox .o files pick up the v12 headers (Homebrew's `/opt/homebrew/include` is first in the include path) but `deps-install/lib/libfmt.a` is still v11. `libfolly.a` was also built against v11. The link fails with `fmt::v12::*` or `fmt::v11::*` undefined symbols. Fix by merging both versions into a single archive:

```bash
DEPS=/Users/jijtang/IdeaProjects/ym-gluten/ep/build-velox/build/velox_ep/scripts/deps-install/lib

# Back up old v11 copy
cp "${DEPS}/libfmt.a" "${DEPS}/libfmt.a.v11.bak"

# Merge v11 + v12 objects (different namespace prefixes → no symbol conflicts)
mkdir -p /tmp/fmt-merge-v11 /tmp/fmt-merge-v12
(cd /tmp/fmt-merge-v11 && ar x "${DEPS}/libfmt.a.v11.bak" && for f in *.o; do mv "$f" "v11_$f"; done)
(cd /tmp/fmt-merge-v12 && ar x /opt/homebrew/lib/libfmt.a   && for f in *.o; do mv "$f" "v12_$f"; done)
ar rcs "${DEPS}/libfmt.a" /tmp/fmt-merge-v11/v11_*.o /tmp/fmt-merge-v12/v12_*.o
```

After merging, relink as usual: `rm cpp/build/releases/libvelox.dylib && ninja -C cpp/build releases/libvelox.dylib`.

**StringIdMap patches (macOS aarch64)** — Two functions in `velox/common/caching/StringIdMap.cpp` need defensive fixes on macOS aarch64 because the two internal maps (`stringToId_` and `idToEntry_`) can drift out of sync, causing crashes or runtime errors. After applying either or both fixes, use the surgical patch workflow below to avoid a full velox rebuild.

> **Background — why `-O3` matters here:**
> Velox release builds use `-O3` (the highest GCC/Clang optimization level). At `-O3` the compiler performs aggressive optimizations that are safe under the C++ standard but interact badly with macOS aarch64 in these two cases:
>
> - **Register allocation** — the compiler keeps local variables (including iterator structs like `F14ItemIter`) in CPU registers rather than spilling them to the stack, minimising memory traffic.
> - **Cold-path separation** — `FOLLY_UNLIKELY(cond)` is a branch-prediction hint that tells the compiler "this branch is almost never taken." The compiler moves the cold-path code to the end of the function and, critically, **does not bother to save caller-saved registers before the preceding function call** when it believes those registers are only needed on the cold path.
>
> On ARM64, registers x0–x18 are *caller-saved*: the callee (e.g. `idToEntry_.find()`) may freely overwrite them. The `it` iterator returned by `stringToId_.find()` is held in some of these registers. After `idToEntry_.find()` returns, those registers have been overwritten. On the `FOLLY_UNLIKELY` cold path, `it.chunk_` is now null — passing it to `eraseUnderlying` dereferences null at offset `+0x20` → SIGSEGV.
>
> The same bug does **not** appear at `-O0` or `-O1` because the compiler spills `it` to the stack before every call, so its value survives regardless of what the callee does to the registers.

Patch 1 — `release()`: replace `find()+erase(iterator)` with erase-by-key so a missing reverse-mapping is silently skipped instead of crashing:
```cpp
// Replace this block in release():
//   auto strIter = stringToId_.find(it->second.string);
//   VELOX_DCHECK(strIter != stringToId_.end());
//   stringToId_.erase(strIter);
// With:
stringToId_.erase(it->second.string);  // safe no-op if key absent
```

Patch 2 — `makeId()`: on the stale-entry branch, erase `stringToId_` by key (`std::string(string)`) instead of by iterator (`it`). At `-O3`, `FOLLY_UNLIKELY` causes the ARM64 compiler to not preserve the `it` registers across the `idToEntry_.find()` call (ARM64 caller-saved registers x0–x18 can be trashed by the callee). On that cold path `it.chunk_` is null, crashing `eraseUnderlying+0x20`. The function-parameter `string` (a `std::string_view`) is stack-resident and always valid:
```cpp
// In makeId(), replace:
//   VELOX_CHECK(entry != idToEntry_.end());
// With:
if (FOLLY_UNLIKELY(entry == idToEntry_.end())) {
  // Do NOT use 'it' here — ARM64 caller-saved regs trashed by idToEntry_.find() at -O3.
  stringToId_.erase(std::string(string));
} else {
  VELOX_CHECK_GE(entry->second.numInUse, 1);
  ++entry->second.numInUse;
  return it->second;
}
```

**Surgical patch workflow** — recompile one `.o`, update the static archive, relink the dylib, and inject it into the Gluten JAR (Gluten loads from the JAR, not from `releases/`):
```bash
VELOX_BUILD=/Users/jijtang/IdeaProjects/ym-gluten/ep/build-velox/build/velox_ep/_build/release
STRINGIDMAP_O="${VELOX_BUILD}/velox/buffer/CMakeFiles/velox.dir/__/common/caching/StringIdMap.cpp.o"
LIBVELOX_A="${VELOX_BUILD}/lib/libvelox.a"
LIBVELOX_DYLIB=/Users/jijtang/IdeaProjects/ym-gluten/cpp/build/releases/libvelox.dylib
JAR=/Users/jijtang/IdeaProjects/ym-gluten/package/target/gluten-package_2.12-1.7.0-SNAPSHOT.jar

# 1. Force recompile (ninja won't detect the edit without a touch)
touch /Users/jijtang/IdeaProjects/ym-gluten/ep/build-velox/build/velox_ep/velox/common/caching/StringIdMap.cpp
ninja -C "${VELOX_BUILD}" "${STRINGIDMAP_O#${VELOX_BUILD}/}"

# 2. Update static archive
ar r "${LIBVELOX_A}" "${STRINGIDMAP_O}"

# 3. Relink shared library
rm -f "${LIBVELOX_DYLIB}"
ninja -C /Users/jijtang/IdeaProjects/ym-gluten/cpp/build releases/libvelox.dylib

# 4. Inject into Gluten JAR (critical — Spark loads from JAR, not releases/)
TMPDIR=$(mktemp -d)
mkdir -p "${TMPDIR}/darwin/aarch64"
cp "${LIBVELOX_DYLIB}" "${TMPDIR}/darwin/aarch64/libvelox.dylib"
(cd "${TMPDIR}" && jar uf "${JAR}" darwin/aarch64/libvelox.dylib)
rm -rf "${TMPDIR}"
echo "Done"
```

---

## IntelliJ IDEA Build Configuration

IntelliJ runs Maven for individual modules from their own directories. Several path fixes are needed so style checkers resolve config files correctly.

### Profiles

Use these profiles in IntelliJ Maven run configuration:
```
spark-3.5,iceberg-test,iceberg,backends-velox,scala-2.12
```

With:
```
-DskipTests=true
-Dmaven.repo.local=/Users/jijtang/.m2/raptor2
```

### `.scalafmt.conf` Resolution Fix

The root `pom.xml` uses a `scalafmt.conf.path` property so each module can override the path:

**Root `pom.xml`** (default for reactor builds from project root):
```xml
<properties>
   <scalafmt.conf.path>${project.basedir}/.scalafmt.conf</scalafmt.conf.path>
</properties>
```

```xml
<scalafmt>
   <file>${scalafmt.conf.path}</file>
</scalafmt>
```

**Each top-level module** (`gluten-core/pom.xml`, `gluten-arrow/pom.xml`, etc.) overrides for standalone IntelliJ builds:
```xml
<properties>
   <scalafmt.conf.path>${project.basedir}/../.scalafmt.conf</scalafmt.conf.path>
</properties>
```

**Nested modules** (`shims/common/pom.xml`, `shims/spark35/pom.xml`) use `../../`:
```xml
<properties>
   <scalafmt.conf.path>${project.basedir}/../../.scalafmt.conf</scalafmt.conf.path>
</properties>
```

### Checkstyle Suppression Fix

The suppression file path in `dev/checkstyle.xml` is resolved via Maven's `checkstyle.suppressions.file` property:

```xml
<module name="SuppressionFilter">
   <property name="file" value="${checkstyle.suppressions.file}"
             default="dev/checkstyle-suppressions.xml"/>
</module>
```

Each module that runs standalone overrides:
```xml
<plugin>
   <groupId>org.apache.maven.plugins</groupId>
   <artifactId>maven-checkstyle-plugin</artifactId>
   <configuration>
      <configLocation>${project.basedir}/../dev/checkstyle.xml</configLocation>
      <suppressionsLocation>${project.basedir}/../dev/checkstyle-suppressions.xml</suppressionsLocation>
   </configuration>
</plugin>
```

### Scalastyle Config Fix

Similarly for modules run standalone by IntelliJ:
```xml
<plugin>
   <groupId>org.scalastyle</groupId>
   <artifactId>scalastyle-maven-plugin</artifactId>
   <configuration>
      <configLocation>${project.basedir}/../dev/scalastyle-config.xml</configLocation>
   </configuration>
</plugin>
```

---

## Quick Reference: Full Build Commands

```bash
# 0. Prerequisites
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
export INSTALL_PREFIX=/Users/jijtang/IdeaProjects/ym-gluten/ep/build-velox/build/velox_ep/scripts/deps-install

# 1. Velox
cd /Users/jijtang/IdeaProjects/ym-gluten/ep/build-velox/src
./build-velox.sh --velox_home=../build/velox_ep --build_type=release

# 2. Arrow 15.0.0-gluten
export SUDO="sudo"
cd /Users/jijtang/IdeaProjects/ym-gluten/dev
bash build-arrow.sh

# 3. eBay Spark (only needed once; skip if raptor2 already has SPJ APIs)
cd /Users/jijtang/IdeaProjects/ebay-spark
./build/mvn -Dmaven.repo.local=/Users/jijtang/.m2/raptor2 \
  -pl sql/catalyst,sql/core -am -DskipTests \
  -rf :spark-core_2.12 install

# 4. Gluten Java/Scala  (iceberg profile is mandatory — omitting it crashes GlutenSessionExtensions at runtime)
cd /Users/jijtang/IdeaProjects/ym-gluten
./build/mvn -Dmaven.repo.local=/Users/jijtang/.m2/raptor2 \
  -P spark-3.5,backends-velox,scala-2.12,iceberg -DskipTests=true install

# 5. Gluten C++
cd /Users/jijtang/IdeaProjects/ym-gluten/dev
./buildbundle-veloxbe.sh \
  --run_setup_script=OFF --build_arrow=OFF \
  --spark_version=3.5 \
  --velox_home=/Users/jijtang/IdeaProjects/ym-gluten/ep/build-velox/build/velox_ep \
  build_gluten_cpp
```

**Expected outputs:**
- `cpp/build/releases/libgluten.dylib` — Gluten JNI native library
- `cpp/build/releases/libvelox.dylib` — Velox native library
- All Gluten JARs installed in `~/.m2/raptor2`

---

## Step 6: Run Spark SQL with Gluten + Iceberg

```bash
  GLUTEN_JAR=/Users/jijtang/IdeaProjects/ym-gluten/package/target/gluten-package_2.12-1.7.0-SNAPSHOT.jar
  ICEBERG_JAR=/Users/jijtang/IdeaProjects/Iceberg-APACHE-ebay/spark/v3.5/spark-runtime/build/libs/iceberg-spark-runtime-3.5_2.12-16ad665.jar

  export DYLD_LIBRARY_PATH=${RELEASES}:${DEPS}

  /Users/jijtang/Downloads/spark/bin/spark-sql \
    --jars "${GLUTEN_JAR},${ICEBERG_JAR}" \
    --conf "spark.driver.extraClassPath=${GLUTEN_JAR}:${ICEBERG_JAR}" \
    --conf spark.plugins=org.apache.gluten.GlutenPlugin \
    --conf spark.sql.extensions="org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.apache.gluten.extension.GlutenSessionExtensions" \
    --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.local.type=hadoop \
    --conf spark.sql.catalog.local.warehouse=/tmp/iceberg-warehouse \
	  --conf spark.memory.offHeap.enabled=true \
	  --conf spark.memory.offHeap.size=20g \
	  --conf spark.shuffle.manager=org.apache.spark.shuffle.sort.ColumnarShuffleManager \
    --conf spark.sql.session.timeZone=UTC \
    --conf spark.sql.adaptive.enabled=false \
    --conf spark.gluten.sql.columnar.forceShuffledHashJoin=true \
    --conf spark.gluten.sql.injectNativePlanStringToExplain=true \
    --conf spark.gluten.sql.columnar.replaceData=true \
    --conf spark.gluten.sql.enable.enhancedFeatures=true \
    --conf spark.sql.storeAssignmentPolicy=ANSI \
    --conf "spark.driver.extraJavaOptions=-Xlog:disable" \
    --conf "spark.executor.extraJavaOptions=-Xlog:disable" \
    --conf spark.sql.autoBroadcastJoinThreshold=-1
```

### Configuration Reference

#### Classpath & JAR Setup

| Config | Value | Explanation |
|---|---|---|
| `--jars` | `${GLUTEN_JAR},${ICEBERG_JAR}` | Adds Gluten and Iceberg JARs to both driver and executor classpaths |
| `spark.driver.extraClassPath` | `${GLUTEN_JAR}:${ICEBERG_JAR}` | Explicitly puts JARs on the driver's JVM classpath at startup — needed for classes loaded during early driver initialization (e.g., plugin registration), which `--jars` alone may miss |

#### Plugin & Extension Registration

| Config | Value | Explanation |
|---|---|---|
| `spark.plugins` | `org.apache.gluten.GlutenPlugin` | Registers Gluten as a Spark plugin — the entry point where Gluten hooks into Spark's lifecycle to replace row-based executors with columnar/native ones |
| `spark.sql.extensions` | `IcebergSparkSessionExtensions,` `GlutenSessionExtensions` | Registers two SQL extension sets: Iceberg adds syntax (`CALL`, time travel, etc.) and optimizer rules; Gluten injects its columnar rule appliers into Spark's query planning pipeline |

#### Iceberg Catalog

| Config | Value | Explanation |
|---|---|---|
| `spark.sql.catalog.local` | `org.apache.iceberg.spark.SparkCatalog` | Registers a catalog named `local` backed by Iceberg. Tables accessed as `local.db.table` are managed by Iceberg |
| `spark.sql.catalog.local.type` | `hadoop` | Uses Hadoop (filesystem-based) catalog — Iceberg stores metadata directly on disk, no Hive Metastore required |
| `spark.sql.catalog.local.warehouse` | `/tmp/iceberg-warehouse` | Root directory where Iceberg stores all table data and metadata files for the `local` catalog |

#### Memory

| Config | Value | Explanation |
|---|---|---|
| `spark.memory.offHeap.enabled` | `true` | Enables off-heap memory allocation. **Required for Gluten** — Velox allocates native memory outside the JVM heap |
| `spark.memory.offHeap.size` | `20g` | Off-heap memory budget per executor. Velox draws from this pool for columnar batch buffers, hash tables, sort buffers, etc. |

#### Shuffle

| Config | Value | Explanation |
|---|---|---|
| `spark.shuffle.manager` | `ColumnarShuffleManager` | Replaces Spark's default shuffle manager with Gluten's columnar shuffle manager, keeping data in Arrow columnar format across stage boundaries and avoiding costly row↔columnar conversions |

#### SQL Behavior

| Config | Value | Explanation |
|---|---|---|
| `spark.sql.session.timeZone` | `UTC` | Sets session timezone to UTC. Recommended with Gluten to avoid timestamp inconsistencies between JVM and native (Velox) sides |
| `spark.sql.adaptive.enabled` | `false` | Disables AQE to get a stable, predictable physical plan. AQE's runtime plan rewrites can complicate debugging of Gluten offloading behavior |
| `spark.sql.autoBroadcastJoinThreshold` | `-1` | Disables broadcast joins entirely — all joins use shuffle-based strategies. Combined with `forceShuffledHashJoin=true`, forces all joins through Gluten's native SHJ path |

#### Gluten-specific

| Config | Value | Explanation |
|---|---|---|
| `spark.gluten.sql.columnar.forceShuffledHashJoin` | `true` | Forces Shuffled Hash Join (SHJ) over Sort Merge Join wherever possible. SHJ is generally faster in Velox as it avoids a full sort phase |
| `spark.gluten.sql.injectNativePlanStringToExplain` | `true` | Appends the Velox-level native plan string to `EXPLAIN FORMATTED` output, useful for verifying which operators were offloaded to native execution |
| `spark.gluten.sql.enable.enhancedFeatures` | `true` | Master switch for Gluten's Iceberg native write group: `AppendDataExec`, `ReplaceDataExec`, `OverwriteByExpressionExec`, `OverwritePartitionsDynamicExec`, `WriteToDataSourceV2Exec`. Must be `true` for the config below to take effect |
| `spark.gluten.sql.columnar.replaceData` | `true` | Enables Gluten's columnar implementation of `ReplaceDataExec` — the V2 write node used by Iceberg's `rewrite_data_files` and row-level operations (MERGE/UPDATE/DELETE on COW tables) |

---

### Verify Gluten is offloading to Velox

```sql
CREATE TABLE local.db.orders (
                                 order_id BIGINT, customer_id BIGINT, amount DOUBLE, order_date STRING
) USING iceberg;

INSERT INTO local.db.orders VALUES
                                (1, 100, 250.0, '2024-01-01'), (2, 101, 180.5, '2024-01-02'),
                                (3, 100, 320.0, '2024-01-03'), (4, 102, 95.0,  '2024-01-04'),
                                (5, 101, 450.0, '2024-01-05');

EXPLAIN SELECT customer_id, SUM(amount) AS total
        FROM local.db.orders GROUP BY customer_id ORDER BY total DESC;
```

Look for `VeloxColumnarToRowExec` or `FlushableHashAggregateExecTransformer` in the plan output.

### Understanding Spark Job / Stage / Query relationships

A single SQL statement maps to one `QueryExecution` (one entry in the Web UI SQL tab), but may trigger **multiple Jobs**:

```
SQL
 └─ Query (1 QueryExecution)
      └─ Job(s)   — one per Action (collect, write, …)
           └─ Stage(s)  — split at every Shuffle boundary
                └─ Task(s)  — one per partition, run in parallel
```

**Why `ORDER BY` produces 2 Jobs**

`ORDER BY` uses `rangepartitioning` to guarantee a globally ordered result. Spark cannot determine the range boundaries without knowing the data distribution, so it runs a lightweight **sampling Job first**:

```
Job 0  genShuffleDependency (VeloxSparkPlanExecApi.scala)
  └─ Scan data, random-sample values, compute N-1 split points
       e.g. for spark.sql.shuffle.partitions=500 → 499 boundaries

Job 1  main execution
  └─ Stage 0: IcebergScan → write Shuffle (data routed by range boundaries)
  └─ Stage 1: read Shuffle → Sort → return results
```

Job 0 is triggered inside `genShuffleDependency` when Gluten creates the columnar shuffle dependency — the sampling still goes through Gluten's columnar path.

**By contrast**, queries that use `hashpartitioning` (e.g. shuffle joins without `ORDER BY`) produce only **1 Job** — hash partitioning requires no knowledge of data distribution so no sampling step is needed.

**Stage count = number of Shuffles + 1.** Each `ColumnarExchange` (or `Exchange`) node in the physical plan is a Shuffle boundary; stages on either side cannot overlap in execution — the upstream stage must finish completely before the downstream stage starts.

### How `rewrite_data_files` and `rewrite_position_delete_files` work with Gluten

#### Background: Iceberg MoR (Merge-on-Read) delete model

Iceberg v2 tables support row-level deletes without rewriting data files immediately. Instead, deletes are recorded as separate **delete files**:

- **Position delete files** — record which rows are deleted by `(data_file_path, row_position)` pairs (written by `DELETE`, `UPDATE`, `MERGE` on MoR tables)
- **Equality delete files** — record deleted rows by column value predicates

On every read, the engine must join data files with their associated delete files and filter out deleted rows. As delete files accumulate, read performance degrades. The two maintenance procedures compact files to restore performance.

#### `rewrite_data_files`

Compacts many small data files into fewer large ones, and **materialises pending deletes into the new files** (deleted rows are simply omitted from the output).

Gluten execution flow:

```
CALL local.system.rewrite_data_files(table => '...')
  │
  ├─ Iceberg planner: identify candidate data files + their delete files
  │
  └─ Spark physical plan (all native via Gluten):
       VeloxIcebergReplaceDataExec          ← write new compacted Parquet files
         └─ SortExecTransformer (optional)  ← sort within output file if needed
              └─ IcebergScanTransformer     ← read data files AND apply delete files
                                               natively in Velox (MoR merge in C++)
```

Key points:
- `IcebergScanTransformer` reads both data and delete files and merges them in native C++ — deleted rows never surface to Spark
- `VeloxIcebergReplaceDataExec` writes the surviving rows as new Parquet files in the columnar path — enabled by `spark.gluten.sql.columnar.replaceData=true`
- After commit, Iceberg atomically swaps old data files + their delete files for the new compacted files; the delete files are no longer referenced and can be expired

Return value: `(rewritten_bytes, added_bytes, rewritten_files, added_files, failed_data_files)`

#### `rewrite_position_delete_files`

Compacts many small position delete files into fewer large ones. The data files are **not** touched. This is useful when many small `DELETE` statements have produced many tiny delete files.

Gluten execution flow:

```
CALL local.system.rewrite_position_delete_files(table => '...')
  │
  ├─ Iceberg planner: identify candidate position delete files
  │
  └─ Spark physical plan:
       Write new position delete Parquet files
         └─ SortExecTransformer            ← sort by (file_path, pos) — required format
              └─ IcebergScanTransformer     ← read position delete files as plain data
                                               (columns: file_path STRING, pos BIGINT)
```

Key points:
- Position delete files are themselves Parquet files with two columns (`file_path`, `pos`); reading them goes through `IcebergScanTransformer` just like normal data
- The output must be sorted by `(file_path, pos)` — Velox's `SortExecTransformer` handles this natively
- If no delete files exist (table has never had deletes, or all were already materialised by a prior `rewrite_data_files`), the procedure returns `0 0 0 0` immediately — this is expected, not an error
- Equality delete files are **not** handled by this procedure; use `rewrite_data_files` to materialise them

Return value: `(rewritten_bytes, added_bytes, rewritten_delete_files, added_delete_files)`

#### Recommended maintenance sequence

```sql
-- 1. Compact position delete files first (cheaper — no data rewrite)
CALL local.system.rewrite_position_delete_files(table => 'local.db.t');

-- 2. Then compact data files (reads fewer, smaller delete files after step 1)
CALL local.system.rewrite_data_files(
  table => 'local.db.t',
  strategy => 'binpack',
  options => map('target-file-size-bytes', '134217728', 'min-input-files', '2')
);

-- 3. Expire old snapshots to reclaim storage
CALL local.system.expire_snapshots(table => 'local.db.t', older_than => now());
```

#### Required Gluten configs for write path

| Config | Required value | Purpose |
|---|---|---|
| `spark.gluten.sql.enable.enhancedFeatures` | `true` | Enables all Iceberg native write executors |
| `spark.gluten.sql.columnar.replaceData` | `true` | Enables `VeloxIcebergReplaceDataExec` used by `rewrite_data_files` |
| `spark.sql.storeAssignmentPolicy` | `ANSI` | Iceberg DataSource V2 writer rejects the default `LEGACY` policy |

#### `VeloxIcebergReplaceDataExec` source code map

The implementation is split across three layers:

**Layer 1 — Scala: Spark plan replacement**

| File | Role |
|---|---|
| `backends-velox/src-iceberg/main/scala/org/apache/gluten/extension/OffloadIcebergWrite.scala` | Rule entry point. `OffloadIcebergReplaceData` pattern-matches `ReplaceDataExec` and swaps it for `VeloxIcebergReplaceDataExec`. Also registers rules for `AppendDataExec`, `OverwriteByExpressionExec`, etc. |
| `backends-velox/src-iceberg/main/scala/org/apache/gluten/execution/VeloxIcebergReplaceDataExec.scala` | Thin wrapper — holds `query`, `refreshCache`, `write`; delegates everything to the parent class |
| `backends-velox/src-iceberg/main/scala/org/apache/gluten/execution/AbstractIcebergWriteExec.scala` | Constructs `IcebergDataWriteFactory` with file format, compression, partition spec, sort order, and nested-field metadata; passes them to the native layer |

**Layer 2 — C++: Gluten native writer (JNI boundary)**

| File | Role |
|---|---|
| `cpp/velox/compute/iceberg/IcebergWriter.h` | Class declaration: `write(VeloxColumnarBatch&)`, `commit()`, `WriteStats` |
| `cpp/velox/compute/iceberg/IcebergWriter.cc` | `write()` — forwards Arrow columnar batches to Velox `IcebergDataSink`; `commit()` — closes files and returns written file paths to Spark; `GlutenIcebergFileNameGenerator` — produces Iceberg-format filenames `{partitionId:05d}-{taskId}-{operationId}-{fileCount:05d}.parquet` |

**Layer 3 — Velox: IcebergDataSink (upstream Velox code)**

| File | Role |
|---|---|
| `velox/connectors/hive/iceberg/IcebergDataSink.h/.cc` | Actual Parquet writing, partition routing, and `DataFile` metadata generation — this is Velox upstream code, not Gluten-owned |

**Full call chain**

```
Spark
VeloxIcebergReplaceDataExec
  └─ AbstractIcebergWriteExec.createBatchWriterFactory()
       └─ IcebergDataWriteFactory          (Scala — serialises params across JNI)
            └─ JNI ↓
                 IcebergWriter::write(VeloxColumnarBatch&)   (cpp/velox/compute/iceberg/)
                   └─ IcebergDataSink::appendData(RowVector) (Velox — writes Parquet)
                 IcebergWriter::commit()                     (returns file path list)
            └─ JNI ↑
  └─ Iceberg catalog: atomic commit — old files replaced by new files
```

**Read vs. write path comparison**

| | Read (Scan) | Write (ReplaceData) |
|---|---|---|
| Spark node | `IcebergScanTransformer` | `VeloxIcebergReplaceDataExec` |
| C++ class | `IcebergSplitReader` | `IcebergWriter` |
| Velox class | `IcebergDataSource` / `IcebergDeleteFile` | `IcebergDataSink` |
| Data format | Arrow columnar (read from Parquet) | Arrow columnar (written to Parquet) |

### Known Fixes for Spark Launch

**Remove stale Gluten JARs from Spark's `jars/` directory.**

If a previous Gluten version was installed into `$SPARK_HOME/jars/`, its old component registration files are scanned by Gluten's component discovery at startup and clash with the new build. Remove them:

```bash
rm -f /Users/jijtang/Downloads/spark/jars/gluten-package_2.12-*.jar
rm -f /Users/jijtang/Downloads/spark/jars/gluten-velox-bundle-*.jar
```

**Do not use `-rf` when running the full Maven install.**

Using `-rf :backends-velox` skips rebuilding `gluten-core`, leaving stale `.class` files for `GlutenInjector`. Always run the full reactor:

```bash
./build/mvn -Dmaven.repo.local=/Users/jijtang/.m2/raptor2 \
  -P spark-3.5,backends-velox,scala-2.12 -DskipTests=true install
```

**Arrow undefined symbols in `libvelox.dylib`.**

Velox's parquet code is compiled against `/usr/local/include/arrow` (installed by `build-arrow.sh`, const-ref API) but Velox EP's bundled `libarrow.a` has by-value signatures. Fix in `cpp/velox/CMakeLists.txt` — link `/usr/local/lib/libarrow.a` after `facebook::velox`:

```cmake
target_link_libraries(velox PUBLIC facebook::velox)

if(EXISTS /usr/local/lib/libarrow.a)
  target_link_libraries(velox PUBLIC /usr/local/lib/libarrow.a)
endif()
if(EXISTS /usr/local/lib/libarrow_bundled_dependencies.a)
  target_link_libraries(velox PUBLIC /usr/local/lib/libarrow_bundled_dependencies.a)
endif()
```


**`VeloxUserError: session 'session_timezone' set with invalid value 'GMT'`**

The plan validator (`cpp/velox/substrait/SubstraitToVeloxPlanValidator.h`) hardcodes `"GMT"` as the session timezone. Velox's ICU timezone library does not recognise `GMT` — it requires `UTC` or an IANA identifier. This causes every query to fail during native plan validation even when `spark.sql.session.timeZone=UTC` is set.

Fix — change the hardcoded value from `"GMT"` to `"UTC"`:

```cpp
// cpp/velox/substrait/SubstraitToVeloxPlanValidator.h  line ~34
{velox::core::QueryConfig::kSparkPartitionId, "0"},
{velox::core::QueryConfig::kSessionTimezone, "UTC"}  // was "GMT"
```

After this fix, also add the following conf to all `spark-sql` / `spark-submit` invocations so the runtime session timezone matches:

```bash
--conf spark.sql.session.timeZone=UTC
```

---

## Troubleshooting

| Error | Cause | Fix |
|---|---|---|
| `deprecated-literal-operator` in GEOS/json.hpp | Clang 17+ stricter warnings | Add `-Wno-deprecated-literal-operator` to `CXX_FLAGS` in `build-velox.sh` |
| `BOOST_PROCESS_V2_ASIO_NAMESPACE` undefined | Boost 1.80+ integrated, macro removed | Patch `process.cc`: use `boost::asio` directly |
| `arrow/filesystem/filesystem.h` not found | Arrow built without filesystem | Add `-DARROW_FILESYSTEM=ON` to Arrow EP cmake args |
| `libarrow_bundled_dependencies.a` not found | macOS uses dynamic system deps | Make optional in `ConfigArrow.cmake` |
| `ccache: command not found` | ccache not installed | `brew install ccache` |
| `Could NOT find JNI` | `JAVA_HOME` not set | `export JAVA_HOME=$(/usr/libexec/java_home -v 17)` |
| `Could NOT find glog` | `INSTALL_PREFIX` wrong | Set to `velox_ep/scripts/deps-install` |
| `KeyGroupedPartitioning` not in catalyst | eBay Spark raptor2 JARs outdated | Build `sql/catalyst,sql/core` from `ebay-spark` source |
| `sun.security.action` compile error | Java 17 module restrictions | Replace `sun.security.action.GetBooleanAction` with `java.lang.Boolean.getBoolean` |
| Arrow `15.0.0-gluten` JARs missing | Custom build not in any repo | Run `dev/build-arrow.sh` |
| `_deflate` / `_inflate` undefined symbols | Arrow GZip codec needs zlib | Add `find_package(ZLIB)` + `ZLIB::ZLIB` to `core/CMakeLists.txt` |
| `iceberg-open-api:1.6.1.1.9.0` missing | eBay Iceberg not in repo | Install dummy JARs for test-scoped classifiers |
| `scalafmt.conf` not found (IntelliJ) | Module run from its own dir | Add `scalafmt.conf.path` property override in each module pom |
| Arrow undefined symbols in `libvelox.dylib` (`large_list` etc.) | Velox compiled against `/usr/local/include/arrow` (const-ref) but linked against Velox EP's `libarrow.a` (by-value) | Link `/usr/local/lib/libarrow.a` into velox target in `cpp/velox/CMakeLists.txt` |
| `NoSuchMethodError: GlutenInjector.ras()` at startup | Old gluten JAR in `$SPARK_HOME/jars/` registers stale `VeloxDeltaComponent` that calls removed `ras()` API | Remove old `gluten-package_2.12-*.jar` and `gluten-velox-bundle-*.jar` from `spark/jars/` |
| `NoSuchMethodError: GlutenInjector.ras()` after full rebuild | Partial Maven reactor (`-rf :backends-velox`) left stale `gluten-core` classes | Always run full `install` without `-rf` |
| `VeloxUserError: session_timezone invalid value 'GMT'` | `SubstraitToVeloxPlanValidator.h` hardcodes `"GMT"` which ICU doesn't recognise | Change to `"UTC"` in `cpp/velox/substrait/SubstraitToVeloxPlanValidator.h` line ~34; add `--conf spark.sql.session.timeZone=UTC` to Spark launch |
| All queries fall back to vanilla Spark silently; Web UI shows "Queries: 0" | `VeloxIcebergComponent` is registered in `META-INF/gluten-components/` but `OffloadIcebergScan$` class is missing — `GlutenSessionExtensions.apply` throws `NoClassDefFoundError` and Spark logs `Cannot use GlutenSessionExtensions` at WARN, silently registering zero columnar rules | Always build with `-P iceberg` profile so `gluten-iceberg` module is included in the package JAR. Check `/Users/jijtang/Downloads/spark/logs/spark.log` for `Cannot use GlutenSessionExtensions` to diagnose this class. |
| `iceberg-open-api:1.6.1.1.9.0` missing (blocks iceberg profile build) | eBay Iceberg artifact not in any accessible repo | Install a dummy main JAR: `mvn install:install-file -Dfile=<any-jar> -DgroupId=org.apache.iceberg -DartifactId=iceberg-open-api -Dversion=1.6.1.1.9.0 -Dpackaging=jar` |
| `fmt::v12::*` undefined when relinking `libvelox.dylib` (or `fmt::v11::*` undefined with homebrew fmt) | Homebrew updated `fmt` v11→v12 after velox_ep deps were built. velox_ep .o files pick up `/opt/homebrew/include/fmt` (v12 headers) but `deps-install/lib/libfmt.a` is v11; `libfolly.a` was also built with v11 and still needs those symbols. | Create a merged `libfmt.a` with both namespaces. Back up the v11 copy first (`cp deps-install/lib/libfmt.a deps-install/lib/libfmt.a.v11.bak`), then extract each into a temp dir, prefix objects (e.g. `v11_*.o`, `v12_*.o`), and `ar rcs` them together. See the fmt-merge script in [Step 5 notes](#step-5-build-gluten-c-native-library). |
| JVM SIGSEGV in `StringIdMap::release` → `eraseUnderlying+0x20` on `SELECT` from Iceberg table | macOS aarch64: `idToEntry_` has an entry but `stringToId_` lacks the reverse mapping. `erase(end())` crashes because F14's `end()` iterator has a null chunk pointer; at `-O3` the null-guard is elided. | Apply Patch 1 + surgical patch workflow in [Step 5 notes](#step-5-build-gluten-c-native-library). |
| JVM SIGSEGV in `StringIdMap::makeId` → `eraseUnderlying+0x20` on `SELECT` from Iceberg table (stack: `makeId+0x300`, registers show `x1=0`) | macOS aarch64: `FOLLY_UNLIKELY` on the stale-entry branch causes the ARM64 compiler at `-O3` to not preserve the `it` iterator registers across `idToEntry_.find()`. On the cold path `it.chunk_` is null, crashing `eraseUnderlying+0x20`. The original `stringToId_.erase(it)` is unsafe here even though `it != end()` — the register holding the chunk pointer was trashed. | Apply Patch 2 (erase by `std::string(string)`, not by iterator) + surgical patch workflow. Apply both Patch 1 and Patch 2 together. |
