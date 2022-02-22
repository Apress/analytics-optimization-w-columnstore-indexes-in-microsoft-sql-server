USE WideWorldImportersDW;
SET STATISTICS IO ON;
GO

SELECT
	tables.name AS table_name,
	indexes.name AS index_name,
	partitions.partition_number,
	column_store_row_groups.row_group_id,
	column_store_row_groups.total_rows,
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

SELECT
	tables.name AS table_name,
	indexes.name AS index_name,
	columns.name AS column_name,
	partitions.partition_number,
	column_store_segments.segment_id,
	column_store_segments.min_data_id,
	column_store_segments.max_data_id,
	column_store_segments.row_count
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
AND columns.name = 'Invoice Date Key'
ORDER BY tables.name, columns.name, column_store_segments.segment_id;

SELECT
	tables.name AS table_name,
	indexes.name AS index_name,
	columns.name AS column_name,
	partitions.partition_number,
	column_store_segments.segment_id,
	column_store_segments.min_data_id,
	column_store_segments.max_data_id,
	column_store_segments.row_count
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
WHERE tables.name = 'Sale_CCI_ORDERED'
AND columns.name = 'Invoice Date Key'
ORDER BY tables.name, columns.name, column_store_segments.segment_id;

WITH CTE_SEGMENTS AS (
	SELECT
		tables.name AS table_name,
		indexes.name AS index_name,
		columns.name AS column_name,
		partitions.partition_number,
		column_store_segments.segment_id,
		column_store_segments.min_data_id,
		column_store_segments.max_data_id,
		column_store_segments.row_count
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
	WHERE tables.name = 'Sale_CCI_ORDERED'
	AND columns.name = 'Invoice Date Key')
SELECT
	CTE_SEGMENTS.table_name,
	CTE_SEGMENTS.index_name,
	CTE_SEGMENTS.column_name,
	CTE_SEGMENTS.partition_number,
	CTE_SEGMENTS.segment_id,
	CTE_SEGMENTS.min_data_id,
	CTE_SEGMENTS.max_data_id,
	CTE_SEGMENTS.row_count,
	OVERLAPPING_SEGMENT.partition_number AS overlapping_partition_number,
	OVERLAPPING_SEGMENT.segment_id AS overlapping_segment_id,
	OVERLAPPING_SEGMENT.min_data_id AS overlapping_min_data_id,
	OVERLAPPING_SEGMENT.max_data_id AS overlapping_max_data_id
FROM CTE_SEGMENTS
INNER JOIN CTE_SEGMENTS OVERLAPPING_SEGMENT
ON (OVERLAPPING_SEGMENT.min_data_id > CTE_SEGMENTS.min_data_id
AND OVERLAPPING_SEGMENT.min_data_id < CTE_SEGMENTS.max_data_id)
OR (OVERLAPPING_SEGMENT.max_data_id > CTE_SEGMENTS.min_data_id
AND OVERLAPPING_SEGMENT.max_data_id < CTE_SEGMENTS.max_data_id)
OR (OVERLAPPING_SEGMENT.min_data_id < CTE_SEGMENTS.min_data_id
AND OVERLAPPING_SEGMENT.max_data_id > CTE_SEGMENTS.max_data_id)
ORDER BY CTE_SEGMENTS.partition_number, CTE_SEGMENTS.segment_id;

SELECT
	[Invoice Date Key],
	COUNT(*) AS Sale_Count
FROM Fact.Sale_CCI_ORDERED
WHERE [Invoice Date Key] IN ('5/1/2013', '9/5/2013', '1/17/2014', '6/30/2014', '3/14/2015', '12/12/2015', '1/1/2016', '2/29/2016')
GROUP BY [Invoice Date Key]
ORDER BY [Invoice Date Key]

SET NOCOUNT ON;
SELECT COUNT(*) FROM Fact.Sale_CCI_ORDERED WHERE [Invoice Date Key] = '5/1/2013';
SELECT COUNT(*) FROM Fact.Sale_CCI_ORDERED WHERE [Invoice Date Key] = '9/5/2013';
SELECT COUNT(*) FROM Fact.Sale_CCI_ORDERED WHERE [Invoice Date Key] = '1/17/2014';
SELECT COUNT(*) FROM Fact.Sale_CCI_ORDERED WHERE [Invoice Date Key] = '6/30/2014';
SELECT COUNT(*) FROM Fact.Sale_CCI_ORDERED WHERE [Invoice Date Key] = '3/14/2015';
SELECT COUNT(*) FROM Fact.Sale_CCI_ORDERED WHERE [Invoice Date Key] = '12/12/2015';
SELECT COUNT(*) FROM Fact.Sale_CCI_ORDERED WHERE [Invoice Date Key] = '1/1/2016';
SELECT COUNT(*) FROM Fact.Sale_CCI_ORDERED WHERE [Invoice Date Key] = '2/29/2016';

SET NOCOUNT OFF;

DELETE
FROM Fact.Sale_CCI_ORDERED
WHERE [Invoice Date Key] <= '1/17/2013';

SELECT
	tables.name AS table_name,
	indexes.name AS index_name,
	partitions.partition_number,
	column_store_row_groups.row_group_id,
	column_store_row_groups.total_rows,
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
WHERE tables.name = 'Sale_CCI_ORDERED'
ORDER BY indexes.index_id, column_store_row_groups.row_group_id;

ALTER INDEX CCI_fact_Sale_CCI_ORDERED ON Fact.Sale_CCI_ORDERED REORGANIZE;

SELECT
	tables.name AS table_name,
	indexes.name AS index_name,
	partitions.partition_number,
	column_store_row_groups.row_group_id,
	column_store_row_groups.total_rows,
	column_store_row_groups.deleted_rows,
	column_store_row_groups.state_description
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
WHERE tables.name = 'Sale_CCI_ORDERED'
ORDER BY indexes.index_id, column_store_row_groups.row_group_id;

ALTER INDEX CCI_fact_Sale_CCI_ORDERED ON Fact.Sale_CCI_ORDERED REORGANIZE;

SELECT
	objects.name AS table_name,
	indexes.name AS index_name,
	dm_db_column_store_row_group_physical_stats.partition_number,
	dm_db_column_store_row_group_physical_stats.row_group_id,
	dm_db_column_store_row_group_physical_stats.state_desc,
	dm_db_column_store_row_group_physical_stats.total_rows,
	dm_db_column_store_row_group_physical_stats.deleted_rows,
	dm_db_column_store_row_group_physical_stats.size_in_bytes,
	dm_db_column_store_row_group_physical_stats.trim_reason_desc
FROM sys.dm_db_column_store_row_group_physical_stats
INNER JOIN sys.objects
ON objects.object_id = dm_db_column_store_row_group_physical_stats.object_id
INNER JOIN sys.indexes
ON indexes.object_id = dm_db_column_store_row_group_physical_stats.object_id
AND indexes.index_id = dm_db_column_store_row_group_physical_stats.index_id
WHERE objects.name = 'Sale_CCI_ORDERED';

INSERT INTO Fact.Sale_CCI_ORDERED
	([Sale Key], [City Key], [Customer Key], [Bill To Customer Key], [Stock Item Key], [Invoice Date Key], [Delivery Date Key],
     [Salesperson Key], [WWI Invoice ID], Description, Package, Quantity, [Unit Price], [Tax Rate], [Total Excluding Tax], [Tax Amount],
     Profit, [Total Including Tax], [Total Dry Items], [Total Chiller Items], [Lineage Key])
VALUES
(   6769, 69490, 0, 0, 26, '2013-02-10', '2013-02-11', 36, 2081, 'Coffee Mug', 'Each', 17, 12.42, 8.00, 211.14, 16.89, 75.00, 228.03,
	17, 0, 11);
   
SELECT
	tables.name AS table_name,
	indexes.name AS index_name,
	partitions.partition_number,
	column_store_row_groups.row_group_id,
	column_store_row_groups.total_rows,
	column_store_row_groups.deleted_rows,
	column_store_row_groups.state_description
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
WHERE tables.name = 'Sale_CCI_ORDERED'
ORDER BY indexes.index_id, column_store_row_groups.row_group_id;

ALTER INDEX CCI_fact_Sale_CCI_ORDERED ON Fact.Sale_CCI_ORDERED REORGANIZE WITH (COMPRESS_ALL_ROW_GROUPS = ON);

SELECT
	objects.name AS table_name,
	indexes.name AS index_name,
	dm_db_column_store_row_group_physical_stats.partition_number,
	dm_db_column_store_row_group_physical_stats.row_group_id,
	dm_db_column_store_row_group_physical_stats.state_desc,
	dm_db_column_store_row_group_physical_stats.total_rows,
	dm_db_column_store_row_group_physical_stats.deleted_rows,
	dm_db_column_store_row_group_physical_stats.size_in_bytes,
	dm_db_column_store_row_group_physical_stats.trim_reason_desc
FROM sys.dm_db_column_store_row_group_physical_stats
INNER JOIN sys.objects
ON objects.object_id = dm_db_column_store_row_group_physical_stats.object_id
INNER JOIN sys.indexes
ON indexes.object_id = dm_db_column_store_row_group_physical_stats.object_id
AND indexes.index_id = dm_db_column_store_row_group_physical_stats.index_id
WHERE objects.name = 'Sale_CCI_ORDERED';

ALTER INDEX CCI_fact_Sale_CCI_ORDERED ON Fact.Sale_CCI_ORDERED REORGANIZE;

ALTER INDEX CCI_fact_Sale_CCI_ORDERED ON Fact.Sale_CCI_ORDERED REBUILD;

SELECT
	objects.name AS table_name,
	indexes.name AS index_name,
	dm_db_column_store_row_group_physical_stats.partition_number,
	dm_db_column_store_row_group_physical_stats.row_group_id,
	dm_db_column_store_row_group_physical_stats.state_desc,
	dm_db_column_store_row_group_physical_stats.total_rows,
	dm_db_column_store_row_group_physical_stats.deleted_rows,
	dm_db_column_store_row_group_physical_stats.size_in_bytes,
	dm_db_column_store_row_group_physical_stats.trim_reason_desc
FROM sys.dm_db_column_store_row_group_physical_stats
INNER JOIN sys.objects
ON objects.object_id = dm_db_column_store_row_group_physical_stats.object_id
INNER JOIN sys.indexes
ON indexes.object_id = dm_db_column_store_row_group_physical_stats.object_id
AND indexes.index_id = dm_db_column_store_row_group_physical_stats.index_id
WHERE objects.name = 'Sale_CCI_ORDERED';

/*	Step 1	*/
DROP INDEX CCI_fact_Sale_CCI_ORDERED ON Fact.Sale_CCI_ORDERED;
/*	Step 2	*/
CREATE CLUSTERED INDEX CCI_fact_Sale_CCI_ORDERED ON Fact.Sale_CCI_ORDERED ([Invoice Date Key]);
/*	Step 3	*/
CREATE CLUSTERED COLUMNSTORE INDEX CCI_fact_Sale_CCI_ORDERED ON Fact.Sale_CCI_ORDERED WITH (DROP_EXISTING=ON, MAXDOP=1);

SELECT
	objects.name AS table_name,
	indexes.name AS index_name,
	dm_db_column_store_row_group_physical_stats.partition_number,
	dm_db_column_store_row_group_physical_stats.row_group_id,
	dm_db_column_store_row_group_physical_stats.state_desc,
	dm_db_column_store_row_group_physical_stats.total_rows,
	dm_db_column_store_row_group_physical_stats.deleted_rows,
	dm_db_column_store_row_group_physical_stats.size_in_bytes,
	dm_db_column_store_row_group_physical_stats.trim_reason_desc
FROM sys.dm_db_column_store_row_group_physical_stats
INNER JOIN sys.objects
ON objects.object_id = dm_db_column_store_row_group_physical_stats.object_id
INNER JOIN sys.indexes
ON indexes.object_id = dm_db_column_store_row_group_physical_stats.object_id
AND indexes.index_id = dm_db_column_store_row_group_physical_stats.index_id
WHERE objects.name = 'Sale_CCI_PARTITIONED';

ALTER INDEX Sale_CCI_PARTITIONED ON Fact.Sale_CCI_ORDERED REBUILD PARTITION = 6;
