pq
===============

Parquet query tool

# Objetive

Create a tool similar to jq but for parquet files.

# Usage

Print number of rows in file:

```sh
pq count <file>
```

Print avro schema:

```sh
pq schema <file>
```

Print file content:

```sh
pq parse <file>
```