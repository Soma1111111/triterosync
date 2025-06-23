# TriteroSync: Multi-Platform Data Synchronization

## Overview

**TriteroSync** is a framework for synchronizing and integrating data across heterogeneous systems using only operation logs (oplogs). The implementation in this repository demonstrates synchronization across **Hive**, **PostgreSQL**, and **MongoDB**, providing robust merge strategies and conflict resolution mechanisms.

## Features

- **Multiple Databases Supported:** 
  - Hive
  - PostgreSQL
  - MongoDB

- **Operation Logging:** 
  - Each system maintains its own append-only, auditable operation log (oplog) for all `SET` and `GET` operations.

- **Abstracted CRUD Operations:** 
  - Each system exposes methods for reading (`get()`) and updating (`set()`) data.

- **Merge Functionality:** 
  - Each system implements a `merge_from(source)` method, synchronizing its state with another system using only oplogs.
  - Systems exchange only logs, never data directly (loose coupling).

## Merge Logics

There are **three merge strategies** implemented, each building on the Last-Writer-Wins (LWW) protocol:

| Variant            | Conflict Resolution Strategy                                                                 |
|--------------------|---------------------------------------------------------------------------------------------|
| `merge_from`       | **LWW**: Retain the value with the latest timestamp for each key.                           |
| `merge_from2`      | **Grade-based**: In case of conflict, the higher grade is retained (e.g., F < D < C < B < A).|
| `merge_from3`      | **Server-priority**: Each system is assigned a priority (MongoDB: 1, PostgreSQL: 2, Hive: 3). On conflict, the value from the highest-priority system is retained. |

### General Merge Workflow

1. **Read Source Oplog:** Load and sort the source system’s oplog by timestamp (oldest first).
2. **Read Target Oplog:** Load the target system’s oplog.
3. **Build “Latest” Map:** For each key (e.g., (id1, id2)), record the latest timestamp seen in the target oplog.
4. **Compare & Apply:** For each source entry:
    - If its timestamp is greater than the stored timestamp for that key (or if the key is new), apply the update, log it with the original timestamp, and update the map.
    - Otherwise, skip the entry.
5. **Finalize:** The target’s database and oplog now reflect the latest updates.

**Properties:**  
- Ensures Last-Writer-Wins (LWW) semantics per key.
- Append-only, auditable logs.
- Conflict resolution is strategy-dependent (timestamp, grade, or server priority).

## Sample Operation Log Format

```
1, SET((KEY1,KEY2), VALUE)
2, GET(KEY1,KEY2)
```

## Example Test Case

```
1, HIVE.SET((KEY1,KEY2), VALUE)
2, POSTGRE.GET(KEY1,KEY2)
1, MONGO.SET((KEY3,KEY4), VALUE)
HIVE.MERGE(POSTGRE)
POSTGRE.MERGE(MONGO)
```
- MERGE commands synchronize states using oplogs only.

## Directory Structure

```
├── .gitignore
├── .~lock.student_course_grades.csv#
├── Associativity.in
├── Commutativity.in
├── Idempotency.in
├── README.md
├── Sync_databases.py
├── example_testcase2.in
├── example_testcase_max.in
├── example_testcase_priority.in
├── example_testcase_timestamp.in
├── hive_bulk_upload_crud.py
├── hive_crud.py
├── mongo_crud.py
├── mongodb_database.py
├── oplog.hiveql
├── oplog.mongo
├── oplog.sql
├── postgre_bulk_upload.py
├── postgre_crud.py
├── student_course_grades.csv
├── sync_max.py                # Server-priority based merge
├── sync_priority.py           # Grade-based merge
├── sync_timestamp.py          # LWW (timestamp) based merge
├── sync_timestamp_global.py
├── test_case_generator.py
```

## Usage

1. **Setup:** Ensure data is loaded redundantly across Hive, PostgreSQL, and MongoDB.
2. **Operation Logging:** Each CRUD operation is logged in the system’s oplog.
3. **Merging:** Use one of the provided merge scripts (`sync_timestamp.py`, `sync_priority.py`, `sync_max.py`) to synchronize data across systems according to your desired conflict resolution strategy.
4. **Testing:** Use provided test case files to validate merge correctness and properties (associativity, commutativity, idempotency).

## Contribution

- Contributions are welcome! Please document any changes and ensure clear, maintainable code.

---

*For more implementation details, refer to the source code and comments.*
