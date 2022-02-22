USE WideWorldImportersDW;
GO

-- Row group metadata
SELECT
	tables.name AS table_name,
	indexes.name AS index_name,
	column_store_row_groups.*
FROM sys.column_store_row_groups
INNER JOIN sys.indexes
ON indexes.index_id = column_store_row_groups.index_id
AND indexes.object_id = column_store_row_groups.object_id
INNER JOIN sys.tables
ON tables.object_id = indexes.object_id
WHERE tables.name = 'Sale_CCI'
ORDER BY tables.object_id, indexes.index_id, column_store_row_groups.row_group_id;

-- Segment metadata joined back to the parent table, column, and index.
SELECT
	tables.name AS table_name,
	indexes.name AS index_name,
	columns.name AS column_name,
	partitions.partition_number,
	column_store_segments.encoding_type,
	column_store_segments.row_count,
	column_store_segments.has_nulls,
	column_store_segments.base_id,
	column_store_segments.magnitude,
	column_store_segments.min_data_id,
	column_store_segments.max_data_id,
	column_store_segments.null_value,
	column_store_segments.on_disk_size
FROM sys.column_store_segments
INNER JOIN sys.partitions
ON column_store_segments.hobt_id = partitions.hobt_id
INNER JOIN sys.indexes
ON indexes.index_id = partitions.index_id
AND indexes.object_id = partitions.object_id
INNER JOIN sys.tables
ON tables.object_id = indexes.object_id
INNER JOIN sys.columns
ON tables.object_id = columns.object_id
AND column_store_segments.column_id = columns.column_id
WHERE tables.name = 'Sale_CCI'
ORDER BY columns.name, column_store_segments.segment_id;

SELECT
	objects.name AS table_name,
	indexes.name AS index_name,
	dm_db_column_store_row_group_physical_stats.partition_number,
	dm_db_column_store_row_group_physical_stats.row_group_id,
	dm_db_column_store_row_group_physical_stats.state_desc,
	dm_db_column_store_row_group_physical_stats.total_rows,
	dm_db_column_store_row_group_physical_stats.deleted_rows,
	dm_db_column_store_row_group_physical_stats.size_in_bytes,
	dm_db_column_store_row_group_physical_stats.trim_reason_desc,
	dm_db_column_store_row_group_physical_stats.transition_to_compressed_state_desc,
	dm_db_column_store_row_group_physical_stats.has_vertipaq_optimization,
	dm_db_column_store_row_group_physical_stats.created_time
FROM sys.dm_db_column_store_row_group_physical_stats
INNER JOIN sys.objects
ON objects.object_id = dm_db_column_store_row_group_physical_stats.object_id
INNER JOIN sys.indexes
ON indexes.object_id = dm_db_column_store_row_group_physical_stats.object_id
AND indexes.index_id = dm_db_column_store_row_group_physical_stats.index_id
WHERE objects.name = 'Sale_CCI';

SELECT
	objects.name AS table_name,
	indexes.name AS index_name,
	dm_db_column_store_row_group_operational_stats.row_group_id,
	dm_db_column_store_row_group_operational_stats.index_scan_count,
	dm_db_column_store_row_group_operational_stats.scan_count,
	dm_db_column_store_row_group_operational_stats.delete_buffer_scan_count,
	dm_db_column_store_row_group_operational_stats.row_group_lock_count,
	dm_db_column_store_row_group_operational_stats.row_group_lock_wait_count,
	dm_db_column_store_row_group_operational_stats.row_group_lock_wait_in_ms,
	dm_db_column_store_row_group_operational_stats.returned_row_count
FROM sys.dm_db_column_store_row_group_operational_stats
INNER JOIN sys.objects
ON objects.object_id = dm_db_column_store_row_group_operational_stats.object_id
INNER JOIN sys.indexes
ON indexes.object_id = dm_db_column_store_row_group_operational_stats.object_id
AND indexes.index_id = dm_db_column_store_row_group_operational_stats.index_id
WHERE objects.name = 'Sale_CCI';

SELECT
	objects.name AS table_name,
	indexes.name AS index_name,
	dm_db_column_store_row_group_operational_stats.row_group_id,
	dm_db_column_store_row_group_operational_stats.scan_count,
	dm_db_column_store_row_group_operational_stats.row_group_lock_wait_count,
	dm_db_column_store_row_group_operational_stats.row_group_lock_wait_in_ms,
	CASE
		WHEN dm_db_column_store_row_group_operational_stats.row_group_lock_wait_count = 0 THEN 0
		ELSE CAST(CAST(dm_db_column_store_row_group_operational_stats.row_group_lock_wait_in_ms AS DECIMAL(16,2)) /
			 CAST(dm_db_column_store_row_group_operational_stats.row_group_lock_wait_count AS DECIMAL(16,2)) AS DECIMAL(16,2))
	END AS lock_wait_ms_per_wait_incidence
FROM sys.dm_db_column_store_row_group_operational_stats
INNER JOIN sys.objects
ON objects.object_id = dm_db_column_store_row_group_operational_stats.object_id
INNER JOIN sys.indexes
ON indexes.object_id = dm_db_column_store_row_group_operational_stats.object_id
AND indexes.index_id = dm_db_column_store_row_group_operational_stats.index_id
WHERE objects.name = 'Sale_CCI';

-- Run a query to get some data into memory for view in the object pool query below.
SELECT
	COUNT(*),
	SUM(Quantity)
FROM fact.Sale_CCI
WHERE Sale_CCI.[Invoice Date Key] >= '1/1/2016'
AND Sale_CCI.[Invoice Date Key] < '1/1/2017'

SELECT
	databases.name,
	objects.name,
	indexes.name,
	columns.name,
	dm_column_store_object_pool.row_group_id,
	dm_column_store_object_pool.object_type_desc,
	dm_column_store_object_pool.access_count,
	dm_column_store_object_pool.memory_used_in_bytes,
	dm_column_store_object_pool.object_load_time
FROM sys.dm_column_store_object_pool
INNER JOIN sys.objects
ON objects.object_id = dm_column_store_object_pool.object_id
INNER JOIN sys.indexes
ON indexes.object_id = dm_column_store_object_pool.object_id
AND indexes.index_id = dm_column_store_object_pool.index_id
INNER JOIN sys.databases
ON databases.database_id = dm_column_store_object_pool.database_id
LEFT JOIN sys.columns
ON columns.column_id = dm_column_store_object_pool.column_id
AND columns.object_id = dm_column_store_object_pool.object_id
WHERE objects.name = 'Sale_CCI'
AND databases.name = DB_NAME()
ORDER BY dm_column_store_object_pool.row_group_id, columns.name;

SELECT
	databases.name,
	objects.name,
	indexes.name,
	dm_column_store_object_pool.row_group_id,
	SUM(dm_column_store_object_pool.access_count) AS access_count,
	SUM(dm_column_store_object_pool.memory_used_in_bytes) AS memory_used_in_bytes
FROM sys.dm_column_store_object_pool
INNER JOIN sys.objects
ON objects.object_id = dm_column_store_object_pool.object_id
INNER JOIN sys.indexes
ON indexes.object_id = dm_column_store_object_pool.object_id
AND indexes.index_id = dm_column_store_object_pool.index_id
INNER JOIN sys.databases
ON databases.database_id = dm_column_store_object_pool.database_id
LEFT JOIN sys.columns
ON columns.column_id = dm_column_store_object_pool.column_id
AND columns.object_id = dm_column_store_object_pool.object_id
WHERE objects.name = 'Sale_CCI'
AND databases.name = DB_NAME()
GROUP BY databases.name, objects.name, indexes.name, dm_column_store_object_pool.row_group_id
ORDER BY dm_column_store_object_pool.row_group_id;

SELECT * FROM sys.internal_partitions


 SELECT
	tables.name AS table_name,
	indexes.name AS index_name,
	partitions.partition_number,
	column_store_row_groups.row_group_id,
	column_store_row_groups.state_description,
	column_store_row_groups.total_rows,
	column_store_row_groups.size_in_bytes,
	column_store_row_groups.deleted_rows,
	internal_partitions.internal_object_type_desc,
	internal_partitions.rows,
	internal_partitions.data_compression_desc
FROM sys.column_store_row_groups
INNER JOIN sys.indexes
ON indexes.index_id = column_store_row_groups.index_id
AND indexes.object_id = column_store_row_groups.object_id
INNER JOIN sys.tables
ON tables.object_id = indexes.object_id
INNER JOIN sys.partitions
ON partitions.partition_number = column_store_row_groups.partition_number
AND partitions.index_id = indexes.index_id
AND partitions.object_id = tables.object_id
LEFT JOIN sys.internal_partitions
ON internal_partitions.object_id = tables.object_id
WHERE tables.name = 'Sale_CCI'
ORDER BY indexes.index_id, column_store_row_groups.row_group_id;