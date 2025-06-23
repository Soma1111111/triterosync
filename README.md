# TriteroSync: Multi-Platform Data Synchronization

## Overview

**TriteroSync** is a project that demonstrates data integration and synchronization across heterogeneous systems. The repository provides a framework for synchronizing data across multiple platforms, including MongoDB, and any two or more from Pig, Hive, PostgreSQL, or MySQL.

## Features

- **Data Synchronization:** Synchronize redundant data loaded onto three or more heterogeneous data systems.
- **Abstracted Operations:** Each system implements independent methods for reading (`get()`) and updating (`set()`) data.
- **Operation Logging:** Every operation is logged in an operation log (oplog) for each system, making the logs generic and applicable to any table.
- **Merge Functionality:** Each system defines an abstract `merge()` method that takes the name of another system as an argument and uses only that system's operation log to synchronize data (without direct access to its data).
- **Test Case Driven:** Synchronization logic can be tested using a sequence of operations and merge commands provided via a test case file.
- **Emphasizes Properties:** The design considers mathematical properties of merge operations such as associativity, commutativity, idempotency, and system convergence.

## Sample Operation Log Format

```
1, SET((KEY1,KEY2), VALUE)
2, GET(KEY1,KEY2)
```

Each operation is timestamped and recorded for replay and synchronization.

## Example Test Case

```
1, SYSTEM1.SET((KEY1,KEY2), VALUE)
2, SYSTEM2.GET(KEY1,KEY2)
3, SYSTEM3.SET((KEY3,KEY4), VALUE)
SYSTEM1.MERGE(SYSTEM2)
SYSTEM2.MERGE(SYSTEM3)
```
- SET and GET operations are ordered per system by timestamp/counter.
- MERGE commands synchronize states using oplogs only and are executed in the given order.

## Usage

1. Prepare your heterogeneous systems (MongoDB and two or more of Pig, Hive, PostgreSQL/MySQL).
2. Load redundant data into each system.
3. Ensure each system maintains its own oplog of all SET/GET operations.
4. Use the abstract `merge()` method to synchronize data between systems using only their oplogs.
5. Provide a test case input file with a sequence of SET, GET, and MERGE commands to drive the main program.

## Design Guidelines

- Focus on simple, maintainable solutions.
- No need for multithreading or parallelism.
- GUI is optional; command-line is sufficient.
- The codebase should include:
  - Source code for each system and the main synchronization logic
  - Sample oplogs and test cases
  - Documentation/comments explaining the merge logic and oplog design

## Sample Directory Structure

```
├── src/
│   ├── mongo.py
│   ├── hive.py
│   ├── sql.py
│   ├── pig.py
│   └── common/
├── logs/
│   ├── oplog.mongo
│   ├── oplog.hive
│   └── oplog.sql
├── testcase.in
├── README.md
```

## Contribution

- Contributions are welcome. Please document your changes and ensure your code is clear and maintainable.

---

*For more details, see the source code and comments.*
