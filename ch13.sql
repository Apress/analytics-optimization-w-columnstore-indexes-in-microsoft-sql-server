USE WideWorldImportersDW;
SET STATISTICS IO ON;
GO

SELECT
	COUNT(*),
	SUM(Quantity) AS total_quantity
FROM Fact.Sale_CCI_ORDERED
WHERE [Invoice Date Key] >= '11/1/2015'
AND [Invoice Date Key] < '1/1/2016';

SELECT
	COUNT(*),
	SUM(Quantity) AS total_quantity
FROM Fact.Sale_CCI_ORDERED
WHERE [Stock Item Key] = 186;

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
AND columns.name = 'Stock Item Key'
ORDER BY tables.name, columns.name, column_store_segments.segment_id;

CREATE NONCLUSTERED INDEX IX_Sale_CCI_ORDERED
ON Fact.Sale_CCI_ORDERED ([Stock Item Key]) INCLUDE (Quantity);
GO

SELECT
	COUNT(*),
	SUM(Quantity) AS total_quantity
FROM Fact.Sale_CCI_ORDERED
WHERE [Stock Item Key] = 186;

DROP INDEX IX_Sale_CCI_ORDERED
ON Fact.Sale_CCI_ORDERED;

ALTER TABLE Fact.Sale ADD CONSTRAINT PK_Fact_Sale PRIMARY KEY NONCLUSTERED 
(	[Sale Key] ASC,
	[Invoice Date Key] ASC);
GO

SELECT
    indexes.name AS Index_Name,
    SUM(dm_db_partition_stats.used_page_count) * 8 Index_Size_KB,
    SUM(dm_db_partition_stats.row_count) AS Row_Count
FROM sys.dm_db_partition_stats
INNER JOIN sys.indexes
ON dm_db_partition_stats.object_id = indexes.object_id
AND dm_db_partition_stats.index_id = indexes.index_id
INNER JOIN sys.tables
ON tables.object_id = dm_db_partition_stats.object_id
INNER JOIN sys.schemas
ON schemas.schema_id = tables.schema_id
WHERE schemas.name = 'Fact'
AND tables.name = 'Sale'
GROUP BY indexes.name
ORDER BY indexes.name;

CREATE NONCLUSTERED INDEX IX_Sale_CCI_ORDERED
ON Fact.Sale_CCI_ORDERED ([Stock Item Key], [Invoice Date Key]) INCLUDE (Quantity)
WHERE [Invoice Date Key] <= '1/1/2016';
GO

SELECT
	COUNT(*),
	SUM(Quantity) AS total_quantity
FROM Fact.Sale_CCI_ORDERED
WHERE [Invoice Date Key] >= '5/1/2014'
AND [Invoice Date Key] < '6/1/2014'
AND [Stock Item Key] = 186;

DROP INDEX IX_Sale_CCI_ORDERED
ON Fact.Sale_CCI_ORDERED;

SELECT DISTINCT
	objects.name,
	partitions.partition_number,
	dm_db_column_store_row_group_physical_stats.row_group_id,
	dm_db_column_store_row_group_physical_stats.has_vertipaq_optimization
FROM sys.dm_db_column_store_row_group_physical_stats
INNER JOIN sys.objects
ON objects.object_id = dm_db_column_store_row_group_physical_stats.object_id
INNER JOIN sys.partitions
ON partitions.object_id = objects.object_id
AND partitions.partition_number = dm_db_column_store_row_group_physical_stats.partition_number
WHERE objects.name = 'Sale'
AND dm_db_column_store_row_group_physical_stats.has_vertipaq_optimization IS NOT NULL
AND dm_db_column_store_row_group_physical_stats.has_vertipaq_optimization = 0
ORDER BY dm_db_column_store_row_group_physical_stats.row_group_id;
GO

ALTER INDEX [CCX_Fact_Sale] ON fact.sale REBUILD;
GO

CREATE VIEW Fact.v_Sale
WITH SCHEMABINDING
AS
SELECT
	[Sale Key],
    [City Key],
    [Customer Key],
    [Bill To Customer Key],
    [Stock Item Key],
    [Invoice Date Key],
    [Delivery Date Key],
    [Salesperson Key],
    [WWI Invoice ID],
    Description,
    Package,
    Quantity,
    [Unit Price],
    [Tax Rate],
    [Total Excluding Tax],
    [Tax Amount],
    Profit,
    [Total Including Tax],
    [Total Dry Items],
    [Total Chiller Items],
    [Lineage Key]
FROM Fact.Sale;
GO

CREATE UNIQUE CLUSTERED INDEX CI_v_sale
ON Fact.v_Sale ([Sale Key], [Invoice Date Key]);
GO

CREATE NONCLUSTERED INDEX IX_v_Sale
ON Fact.v_Sale ([Stock Item Key], Quantity)
GO

SELECT
	COUNT(*),
	SUM(Quantity) AS total_quantity
FROM Fact.v_Sale
WHERE [Stock Item Key] = 186;

SELECT DISTINCT
	objects.name,
	partitions.partition_number,
	dm_db_column_store_row_group_physical_stats.row_group_id,
	dm_db_column_store_row_group_physical_stats.has_vertipaq_optimization
FROM sys.dm_db_column_store_row_group_physical_stats
INNER JOIN sys.objects
ON objects.object_id = dm_db_column_store_row_group_physical_stats.object_id
INNER JOIN sys.partitions
ON partitions.object_id = objects.object_id
AND partitions.partition_number = dm_db_column_store_row_group_physical_stats.partition_number
WHERE objects.name = 'Sale'
AND dm_db_column_store_row_group_physical_stats.has_vertipaq_optimization IS NOT NULL
AND dm_db_column_store_row_group_physical_stats.has_vertipaq_optimization = 0
ORDER BY dm_db_column_store_row_group_physical_stats.row_group_id;
GO

INSERT INTO Fact.Sale
	([City Key], [Customer Key], [Bill To Customer Key], [Stock Item Key], [Invoice Date Key], [Delivery Date Key], [Salesperson Key],
     [WWI Invoice ID], Description, Package, Quantity, [Unit Price], [Tax Rate], [Total Excluding Tax], [Tax Amount], Profit,
     [Total Including Tax], [Total Dry Items], [Total Chiller Items], [Lineage Key])
VALUES
(   41568,
    0,
    0,
    186,
    '1/16/2016',
    '1/18/2016',
    9,
    187,
    N'Giant coffee mug',
    N'Each',
    100,
    15.00,
    8.000,
    1500.00,
    120.00,
    800.00,
    1620.00,
    100,
    0,
    11);

ALTER INDEX [CCX_Fact_Sale] ON fact.sale REORGANIZE WITH (COMPRESS_ALL_ROW_GROUPS = ON);
GO

SELECT DISTINCT
	objects.name,
	partitions.partition_number,
	dm_db_column_store_row_group_physical_stats.row_group_id,
	dm_db_column_store_row_group_physical_stats.has_vertipaq_optimization
FROM sys.dm_db_column_store_row_group_physical_stats
INNER JOIN sys.objects
ON objects.object_id = dm_db_column_store_row_group_physical_stats.object_id
INNER JOIN sys.partitions
ON partitions.object_id = objects.object_id
AND partitions.partition_number = dm_db_column_store_row_group_physical_stats.partition_number
WHERE objects.name = 'Sale'
AND dm_db_column_store_row_group_physical_stats.has_vertipaq_optimization IS NOT NULL
AND dm_db_column_store_row_group_physical_stats.has_vertipaq_optimization = 0
ORDER BY dm_db_column_store_row_group_physical_stats.row_group_id;
GO

DROP INDEX IX_v_Sale ON Fact.v_Sale;
GO

DROP INDEX CI_v_sale ON Fact.v_Sale;
GO

DROP VIEW Fact.v_Sale;
GO

-- If needed
ALTER TABLE Fact.Sale ADD CONSTRAINT PK_Sale PRIMARY KEY NONCLUSTERED ([Sale Key], [Invoice Date Key]);
GO

SELECT
    indexes.name AS Index_Name,
    SUM(dm_db_partition_stats.used_page_count) * 8 Index_Size_KB,
    SUM(dm_db_partition_stats.row_count) AS Row_Count
FROM sys.dm_db_partition_stats
INNER JOIN sys.indexes
ON dm_db_partition_stats.object_id = indexes.object_id
AND dm_db_partition_stats.index_id = indexes.index_id
INNER JOIN sys.tables
ON tables.object_id = dm_db_partition_stats.object_id
INNER JOIN sys.schemas
ON schemas.schema_id = tables.schema_id
WHERE indexes.name = 'PK_Sale'
GROUP BY indexes.name;

ALTER TABLE Fact.Sale DROP CONSTRAINT PK_Sale;
ALTER TABLE Fact.Sale ADD CONSTRAINT PK_Sale PRIMARY KEY NONCLUSTERED ([Sale Key], [Invoice Date Key])
WITH (DATA_COMPRESSION = PAGE);
GO

SELECT
    indexes.name AS Index_Name,
    SUM(dm_db_partition_stats.used_page_count) * 8 Index_Size_KB,
    SUM(dm_db_partition_stats.row_count) AS Row_Count
FROM sys.dm_db_partition_stats
INNER JOIN sys.indexes
ON dm_db_partition_stats.object_id = indexes.object_id
AND dm_db_partition_stats.index_id = indexes.index_id
INNER JOIN sys.tables
ON tables.object_id = dm_db_partition_stats.object_id
INNER JOIN sys.schemas
ON schemas.schema_id = tables.schema_id
WHERE indexes.name = 'PK_Sale'
GROUP BY indexes.name;