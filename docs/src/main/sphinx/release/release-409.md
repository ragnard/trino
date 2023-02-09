# Release 409 (3 Mar 2023)

## General

* Add support for dropping nested fields with a `DROP COLUMN` statement. ({issue}`15975`)
* Add queue, analysis, planning, and execution time to the output of
  `EXPLAIN ANALYZE`. ({issue}`16329`)
* Add support for executing table functions with any number of table arguments. ({issue}`1839`)
* Improve output of `EXPLAIN` queries to show statistics when a query contains
  aggregations. ({issue}`16201`)
* Improve performance of queries with aggregations containing a `DISTINCT`
  clause using table statistics. This can be configured with the
  `optimizer.mark-distinct-strategy`configuration property, and the
  `optimizer.use-mark-distinct` configuration property is now deprecated in
  favor of it. ({issue}`15927`)
* Improve performance of LIKE expressions with patterns constructed dynamically. ({issue}`15999`)
* Remove the `distributed-index-joins-enabled` configuration property and
  related functionality. ({issue}`15375`)
* Fix failure when using non-comparable and non-sortable values as part of a
  `VALUES` expression. ({issue}`16242`)

## BigQuery connector

* Add support for using default values when inserting data. ({issue}`16327`)
* Fix failure when non-lowercase column names exist in the `query` table
  function. ({issue}`16075`)

## Cassandra connector

* Add support for `TIME` type. ({issue}`13063`)

## ClickHouse connector

* Remove support for the `ru.yandex.clickhouse.ClickHouseDriver` legacy JDBC
  driver in the `clickhouse.legacy-driver` configuration property. ({issue}`16188`)
* Remove support for specifying expressions in the `sample_by` table property to
  prevent SQL injection. ({issue}`16261`)

## Delta Lake connector

* Avoid query failure by inferring required Delta Lake version when creating new
  tables or configuring table features. ({issue}`16310`)
* Fix query failure when reading Parquet files generated by Kafka Connect. ({issue}`16264`)

## Hive connector

* Add support for the Hadoop `DefaultCodec` to Hive formats. ({issue}`16250`)
* Add a native CSV file format reader and writer. These can be disabled with the
  `csv_native_reader_enabled` and `csv_native_writer_enabled` session properties
  or the `csv.native-reader.enabled` and `csv.native-writer.enabled`
  configuration properties. ({issue}`15918`)
* Add a native JSON file format reader and writer. These can be disabled with
  the `json_native_reader_enabled` and `json_native_writer_enabled` session
  properties or the `json.native-reader.enabled` and
  `json.native-writer.enabled` configuration properties. ({issue}`15918`)
* Add a native text file format reader and writer. These can be disabled with
  the `text_file_native_reader_enabled` and `text_file_native_writer_enabled`
  session properties or the `text-file.native-reader.enabled` and
  `text-file.native-writer.enabled` configuration properties. ({issue}`15918`)
* Add a native sequence file format reader and writer. These can be disabled
  with the `sequence_file_native_reader_enabled` and
  `sequence_file_native_writer_enabled` session properties or the
  `sequence-file.native-reader.enabled` and
  `sequence-file.native-writer.enabled` configuration properties. ({issue}`15918`)
* Add a native regex file format reader. The reader can be disabled with the
  `regex_native_reader_enabled` session property or the
  `regex.native-reader.enabled` configuration property. ({issue}`15918`)
* Add `regex` and `regex_case_insensitive` table properties for the `REGEX`
  format. ({issue}`16271`)
* Improve performance of queries which read from partitioned Hive tables and
  write to partitioned tables when statistics are not available for the source
  tables. ({issue}`16229`)
* Improve query performance when only table statistics generated by Apache Spark
  are available. This can be disabled via the
  `hive.metastore.thrift.use-spark-table-statistics-fallback` configuration
  property. ({issue}`16120`)
* Fix incorrectly ignoring computed table statistics in `ANALYZE`. ({issue}`15995`)
* Fix query failure when reading Parquet files generated by Kafka Connect. ({issue}`16264`)

## Hudi connector

* Fix query failure when reading Parquet files generated by Kafka Connect. ({issue}`16264`)

## Iceberg connector

* Add support for dropping nested fields with a `DROP COLUMN` statement. ({issue}`15975`)
* Add support for Iceberg table sort orders. Tables can have a list of
  `sorted_by` columns which are used to order files written to the table. ({issue}`14891`)
* Fix query failure when reading nested columns on a table with
  [equality delete files](https://iceberg.apache.org/spec/#equality-delete-files). ({issue}`14836`)
* Fix query failure when reading Parquet files generated by Kafka Connect. ({issue}`16264`)

## SQL Server connector

* Add support for pushing down joins using `=` and `!=`  predicates over text
  columns if the column uses a case-sensitive collation within SQL Server. ({issue}`16185`)