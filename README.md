# Distributed Systems Labs (MIT 6.5840/6.824)

This repository contains my implementations of Labs 1–4 from the MIT 6.5840 (Distributed Systems) course ([Course Website](https://pdos.csail.mit.edu/6.5840/schedule.html)). These labs cover key concepts in distributed systems, focusing on fault tolerance, replication, and consensus.

## Table of Contents

- [Overview](#overview)
- [Labs Implemented](#labs-implemented)
- [Getting Started](#getting-started)
- [Running Tests](#running-tests)
- [Code Structure](#code-structure)
- [References](#references)

---

## Overview

The MIT 6.5840/6.824 course is a graduate-level introduction to distributed systems, covering techniques for building fault-tolerant and scalable systems. The labs are designed to reinforce core concepts through hands-on implementation.

This repository includes my solutions for the following labs:
- **Lab 1: MapReduce** – Implementing a simplified MapReduce framework.
- **Lab 2: Raft (Leader Election & Log Replication)** – Building a fault-tolerant replicated state machine using the Raft consensus algorithm.
- **Lab 3: Fault-tolerant Key/Value Service** – Using Raft to implement a robust key/value storage system.
- **Lab 4: Sharded Key/Value Service** – Extending the key/value service to support dynamic sharding and configuration changes.

## Labs Implemented

| Lab   | Description                                      | Reference Link                                           |
|-------|--------------------------------------------------|----------------------------------------------------------|
| Lab 1 | MapReduce framework                              | [Lab 1 on course site](https://pdos.csail.mit.edu/6.5840/labs/lab-mr.html)   |
| Lab 2 | Fault-tolerant Raft consensus implementation     | [Lab 2 on course site](https://pdos.csail.mit.edu/6.5840/labs/lab-raft.html) |
| Lab 3 | Key/value store on Raft                          | [Lab 3 on course site](https://pdos.csail.mit.edu/6.5840/labs/lab-kvraft.html) |
| Lab 4 | Sharded key/value service                        | [Lab 4 on course site](https://pdos.csail.mit.edu/6.5840/labs/lab-shard.html) |

## Getting Started

### Prerequisites

- Go (>=1.20 recommended)
- Unix/Linux or macOS (Windows is not officially supported)
- Git

### Setup

Clone the repository:

```bash
git clone https://github.com/Aquib-Nawaz/6.5840.git
cd 6.5840
```

Install Go dependencies if needed.

## Running Tests

Each lab includes a set of tests provided by the course. To run the tests for a specific lab, navigate to the lab directory and execute:

```bash
cd labX  # Replace X with 1, 2, 3, or 4
go test -v
```

Some labs may require running the test script from the root directory or using provided test scripts. Refer to the README in each lab’s directory for more details.

## Code Structure

```
├── lab1/            # MapReduce lab code
├── lab2/            # Raft consensus implementation
├── lab3/            # Key/value service using Raft
├── lab4/            # Sharded key/value service
├── scripts/         # Helper scripts (if any)
└── README.md
```

## References

- [MIT 6.5840/6.824 Course Website](https://pdos.csail.mit.edu/6.5840/schedule.html)
- [Lab instructions and handouts](https://pdos.csail.mit.edu/6.5840/labs/)
- [Raft Paper](https://raft.github.io/raft.pdf)
- [MapReduce Paper](https://research.google.com/archive/mapreduce.html)

---

**Note:** This repository is for educational purposes. If you are a current student in 6.5840/6.824, please abide by the course’s collaboration and academic honesty policies.
