pq
===============

Parquet query tool

# Objetive

Create a tool similar to jq but for parquet files.

# Installation

The preferred instalation process is download one of the precompiled binary files that can be downloaded from releases page in github. You will find these different files:

- [pq-linux-amd64](https://github.com/tonivade/pq/releases/download/0.4/pq-linux-amd64): native image for linux x86_64.
- [pq-darwin-amd64](https://github.com/tonivade/pq/releases/download/0.4/pq-darwin-amd64): native image for mac x86_64.
- [pq-windows-amd64.exe](https://github.com/tonivade/pq/releases/download/0.4/pq-windows-amd64.exe): native image for windows x86_64.
- [pq.jar](https://github.com/tonivade/pq/releases/download/0.4/pq.jar): fat jar with all the needed classes

Native images has been generated using graalvm-ce 21.0.2.

Current version is 0.4.

# Usage

You will see all the available subcomands using help subcommand:

```sh
$ ./pq help
Usage: pq [-v] [COMMAND]
parquet query tool
  -v, --verbose   enable debug logs
Commands:
  count     print total number of rows in parquet file
  schema    print schema of parquet file
  read      print content of parquet file in json format
  metadata  print metadata of parquet file
  write     create a parquet file from a jsonl stream and a schema
  help      Display help information about the specified command.
Copyright(c) 2023 by @tonivade
```

## count

Print number of rows in file:

```sh
$ ./pq help count
Usage: pq count [-v] [--filter=PREDICATE] FILE
print total number of rows in parquet file
      FILE                 parquet file
      --filter=PREDICATE   predicate to apply to the rows
  -v, --verbose            enable debug logs
```

Example

```sh
$ ./pq count <file>
1000
```

## schema

Print parquet schema.

```sh
$ ./pq help schema
Usage: pq schema [-v] [--select=COLUMN[,COLUMN...]]... FILE
print schema of parquet file
      FILE        parquet file
      --select=COLUMN[,COLUMN...]
                  list of columns to select
  -v, --verbose   enable debug logs
```

Example:

```sh
$ ./pq schema <file>
message spark_schema {
  optional int32 id;
  optional binary first_name (STRING);
  optional binary last_name (STRING);
  optional binary email (STRING);
  optional binary gender (STRING);
  optional binary ip_address (STRING);
  optional binary cc (STRING);
  optional binary country (STRING);
  optional binary birthdate (STRING);
  optional double salary;
  optional binary title (STRING);
  optional binary comments (STRING);
}
```

## read

Print file content.

```sh
$ ./pq help read
Usage: pq read [-v] [--index] [--filter=PREDICATE] [--format=JSON|CSV]
               [--get=ROW] [--head=ROWS] [--skip=ROWS] [--tail=ROWS]
               [--select=COLUMN[,COLUMN...]]... FILE
print content of parquet file in json format
      FILE                 parquet file
      --filter=PREDICATE   predicate to apply to the rows
      --format=JSON|CSV    output format, json or csv
      --get=ROW            print just the row with given index
      --head=ROWS          get the first N number of rows
      --index              print row index
      --select=COLUMN[,COLUMN...]
                           list of columns to select
      --skip=ROWS          skip a number N of rows
      --tail=ROWS          get the last N number of rows
  -v, --verbose            enable debug logs
```

Example:

```sh
$ ./pq read <file>
{"id":1,"first_name":"Amanda","last_name":"Jordan","email":"ajordan0@com.com","gender":"Female","ip_address":null,"cc":"6759521864920116","country":"Indonesia","birthdate":"3/8/1971","salary":49756.53,"title":"Internal Auditor","comments":"1E+02"}
...
```

## metadata

Print file metadata.

```sh
$ ./pq help metadata
Usage: pq metadata [-v] [--show-blocks] FILE
print metadata of parquet file
      FILE            parquet file
      --show-blocks   show block metadata info
  -v, --verbose       enable debug logs
```

Example:

```sh
$ ./pq metadata <file>
"createdBy":parquet-mr version 1.8.3 (build aef7230e114214b7cc962a8f3fc5aeed6ce80828)
"count":1000
```

## write

Creates a parquet file from a jsonl/csv file and a shema

```
$ ./pq help write
Usage: pq write [-v] [--format=JSON|CSV] [--schema=FILE] FILE
create a parquet file from a jsonl stream and a schema
      FILE                destination parquet file
      --format=JSON|CSV   input format, json or csv
      --schema=FILE       file with schema definition
  -v, --verbose           enable debug logs
```

# License

This project is released under MIT License
