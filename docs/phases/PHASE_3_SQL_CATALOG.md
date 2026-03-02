# Phase 3: SQL & Catalog

## Overview
Phase 3 enabled the engine to understand SQL syntax and manage dynamic database schemas through a persistent catalog.

## Key Components

### 1. SQL Parser (`parser/parser.cpp`, `lexer.cpp`)
Implemented a custom recursive descent parser.
- **Lexer**: Tokenizes SQL strings with support for keywords, identifiers, and literals.
- **Parser**: Constructs Abstract Syntax Trees (AST) for:
    - **DDL**: `CREATE TABLE`, `DROP TABLE`.
    - **DML**: `SELECT`, `INSERT`, `UPDATE`, `DELETE`.
- **Expression Support**: Parsing of complex boolean and arithmetic expressions in `WHERE` and `SET` clauses.

### 2. Global Catalog (`catalog/catalog.cpp`)
Introduced a centralized authority for metadata.
- **Schema Management**: Tracks table definitions, column types, and constraints.
- **Thread Safety**: Uses readers-writer locks to allow concurrent metadata lookups while ensuring atomic updates.
- **Object IDs (OID)**: System-wide unique identifiers for tables and indexes.

### 3. System Tables
Implemented persistence for metadata.
- **Storage**: Catalog state is stored in internal heap tables (`pg_class`, `pg_attribute`).
- **Bootstrap**: Logic to initialize a fresh data directory with core system tables.

## Lessons Learned
- Decoupling the AST from the execution plan allows for easier query optimization in later stages.
- A robust catalog is essential for multi-node consistency.
