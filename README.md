# Distributed Systems Labs (MIT 6.5840/6.824)

This repository contains implementations of Labs 1–4 from MIT's 6.5840 (formerly 6.824) Distributed Systems course. The course and labs are described in detail on the [official course website](https://pdos.csail.mit.edu/6.5840/schedule.html).

---

## What is a Distributed System?

A **distributed system** is a collection of independent computers that appear to the users of the system as a single computer. Such systems must handle challenges like:
- Network partitions and unreliable nodes
- Fault tolerance (handling crashed or slow servers)
- Data consistency across machines
- Scalability as demand grows

Modern cloud services (Google, Amazon, Facebook) are all built atop distributed systems.

---

## Key Concepts Used in This Repository

### 1. MapReduce

[MapReduce](https://research.google.com/archive/mapreduce.html) is a programming model developed by Google for processing large data sets with a parallel, distributed algorithm.  
- **Map** step: Processes input data into key/value pairs.
- **Reduce** step: Merges all values associated with the same key.

**Lab 1** implements a simplified MapReduce framework and some sample applications.

### 2. Raft Consensus Algorithm

[Raft](https://raft.github.io/raft.pdf) is a consensus algorithm for managing a replicated log across a cluster of servers.  
Consensus is needed to ensure that all servers in a distributed system agree on the same data, even in the presence of failures.

Raft makes it easier to understand and implement consensus by decomposing the problem into:
- **Leader election** (choosing a coordinator)
- **Log replication** (copying data reliably)
- **Safety** (ensuring servers don't diverge)

**Lab 2** involves building a basic Raft implementation.

### 3. Replicated Key/Value Store

A **key/value store** is a database that uses a simple key-value method to store data. By layering Raft under a key/value API, we get a system that is:
- **Fault tolerant:** Keeps working even if some servers fail.
- **Consistent:** All servers agree on the order of operations.

**Lab 3** requires building such a store using your Raft code.

### 4. Sharding

**Sharding** is a method for distributing data across multiple servers (shards), allowing the system to scale horizontally while maintaining high availability.

**Lab 4** is about implementing a sharded key/value service.

---

## Folder Structure

```
src/
  mr/          # MapReduce framework implementation (Lab 1)
  mrapps/      # MapReduce application plugins
  main/        # Entry points, input samples, and runners for labs
  kvraft1/     # Raft-based key/value service (Lab 3)
  shardkv1/    # Sharded key/value service (Lab 4)
  labgob/      # Serialization helpers
  models1/     # Model checking and helpers
  ...
```
**See [GitHub repository](https://github.com/Aquib-Nawaz/6.5840) for the full list.**

---

## Labs Implemented

| Lab   | Description                                  | Reference Link                                           |
|-------|----------------------------------------------|----------------------------------------------------------|
| 1     | MapReduce framework and applications         | [Lab 1](https://pdos.csail.mit.edu/6.5840/labs/lab-mr.html)   |
| 2     | Fault-tolerant Raft consensus implementation | [Lab 2](https://pdos.csail.mit.edu/6.5840/labs/lab-raft.html) |
| 3     | Key/value store on Raft                      | [Lab 3](https://pdos.csail.mit.edu/6.5840/labs/lab-kvraft.html) |
| 4     | Sharded key/value service                    | [Lab 4](https://pdos.csail.mit.edu/6.5840/labs/lab-shard.html) |

---

## How to Use This Code

### Prerequisites

- Go 1.20+ (recommended)
- Unix/Linux or macOS

### Building and Running

Each lab has its own entry point in `src/main/`. For example, to run the MapReduce coordinator:
```bash
cd src/main
go run mrcoordinator.go pg-being_ernest.txt
```

To run a worker:
```bash
go run mrworker.go wc.go
```
(Replace arguments as appropriate for each lab.)

### Testing

Each lab directory includes Go tests that you can run, for example:
```bash
cd src/mr
go test -v
```
Refer to the README or instructions in each lab’s directory for more details.

---

## Further Reading

- [MIT 6.5840/6.824 Course Website](https://pdos.csail.mit.edu/6.5840/schedule.html)
- [Raft Paper (consensus algorithm)](https://raft.github.io/raft.pdf)
- [MapReduce Paper (parallel data processing)](https://research.google.com/archive/mapreduce.html)

---

**Note:** This repository is for educational use. If you are a current 6.5840/6.824 student, respect the course’s collaboration and academic honesty policies.
