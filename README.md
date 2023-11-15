
# SDDMS
Simple Distributed Database Management System

## Overview
SDDMS is a simple distributed database management system written in Rust. It was written as a class project, so it is
entirely a learning exercise.

It has the following features

- **Full Replication** - The database is fully replicated across sites
- **Serialization** - The database uses two-phase locking to ensure serializability across sites
- **Deadlock resilient** - The database detects deadlocks and aborts invalid transactions that would cause deadlocks