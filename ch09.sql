USE WideWorldImportersDW;
SET STATISTICS IO ON;
SET NOCOUNT OFF;

DELETE
FROM Fact.Sale_CCI
WHERE [Invoice Date Key] = '1/1/2016';

SELECT
	tables.name AS table_name,
	indexes.name AS index_name,
	partitions.partition_number,
	column_store_row_groups.state_description,
	column_store_row_groups.total_rows,
	column_store_row_groups.size_in_bytes,
	column_store_row_groups.deleted_rows
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
WHERE tables.name = 'Sale_CCI'
ORDER BY indexes.index_id, column_store_row_groups.row_group_id;

DELETE
FROM Fact.Sale_CCI
WHERE [Invoice Date Key] = '1/1/2016';

SELECT
	tables.name AS table_name,
	indexes.name AS index_name,
	partitions.partition_number,
	column_store_row_groups.state_description,
	column_store_row_groups.total_rows,
	column_store_row_groups.size_in_bytes,
	column_store_row_groups.deleted_rows
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
WHERE tables.name = 'Sale_CCI'
ORDER BY indexes.index_id, column_store_row_groups.row_group_id;

ALTER INDEX CCI_fact_sale_CCI ON Fact.Sale_CCI REBUILD;

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
	internal_partitions.rows
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

SELECT
	*
FROM Fact.Sale_CCI
WHERE [Invoice Date Key] = '1/2/2016';

UPDATE Sale_CCI
	SET [Total Dry Items] = [Total Dry Items] - 1,
		[Total Chiller Items] = [Total Chiller Items] + 1
FROM Fact.Sale_CCI
WHERE [Invoice Date Key] = '1/2/2016';

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
	internal_partitions.rows
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

SELECT
	COUNT(*)
FROM Fact.Sale_CCI
WHERE [Invoice Date Key] >= '1/3/2016'
AND [Invoice Date Key] < '1/8/2016'

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
	internal_partitions.rows
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

ALTER INDEX CCI_fact_sale_CCI ON Fact.Sale_CCI REBUILD;
GO

SELECT
	COUNT(*)
FROM Fact.Sale_CCI
WHERE [Invoice Date Key] >= '1/3/2016'
AND [Invoice Date Key] < '1/8/2016'

UPDATE Sale_CCI
	SET [Total Dry Items] = [Total Dry Items] - 1,
		[Total Chiller Items] = [Total Chiller Items] + 1
FROM Fact.Sale_CCI
WHERE [Invoice Date Key] >= '1/3/2016'
AND [Invoice Date Key] < '1/8/2016';

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
	internal_partitions.rows
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

SELECT
	COUNT(*)
FROM Fact.Sale_CCI
WHERE [Invoice Date Key] >= '1/8/2016'
AND [Invoice Date Key] < '3/5/2016';

ALTER INDEX CCI_fact_sale_CCI ON Fact.Sale_CCI REBUILD;
GO

UPDATE Sale_CCI
	SET [Total Dry Items] = [Total Dry Items] - 1,
		[Total Chiller Items] = [Total Chiller Items] + 1
FROM Fact.Sale_CCI
WHERE [Invoice Date Key] >= '1/8/2016'
AND [Invoice Date Key] < '3/5/2016';

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
	internal_partitions.rows
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
