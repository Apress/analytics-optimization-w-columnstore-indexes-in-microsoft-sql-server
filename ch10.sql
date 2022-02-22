USE WideWorldImportersDW;
SET STATISTICS IO ON;
SET NOCOUNT OFF;

SELECT
	[City Key],
	COUNT(*)
FROM fact.Sale_CCI
WHERE [Invoice Date Key] >= '1/1/2016'
GROUP BY [City Key]
ORDER BY COUNT(*) DESC;

SELECT
	*
FROM fact.Sale_CCI
WHERE [Invoice Date Key] = '2/17/2016';

SELECT
	[Sale Key],
	[City Key],
	[Invoice Date Key]
FROM fact.Sale_CCI
WHERE [Invoice Date Key] = '2/17/2016';

SELECT
	*
FROM Dimension.Employee

INSERT INTO Dimension.Employee
	([Employee Key], [WWI Employee ID], Employee, [Preferred Name], [Is Salesperson], Photo, [Valid From], [Valid To], [Lineage Key])
VALUES
(   -1, -- Clustered Index
    289, N'Ebenezer Scrooge', N'Scrooge', 0, NULL, GETUTCDATE(), '9999-12-31 23:59:59.9999999', 3),
(   213, -- Clustered Index
    400, N'Captain Ahab', N'Captain', 0, NULL, GETUTCDATE(), '9999-12-31 23:59:59.9999999', 3),
(   1017, -- Clustered Index
    501, N'Holden Caulfield', N'Phony', 0, NULL, GETUTCDATE(), '9999-12-31 23:59:59.9999999', 3);

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
	SUM([Quantity])
FROM Fact.Sale_CCI
WHERE [Invoice Date Key] >= '1/1/2016'
AND [Invoice Date Key] < '2/1/2016';

CREATE TABLE Fact.Sale_CCI_ORDERED
(	[Sale Key] [bigint] NOT NULL,
	[City Key] [int] NOT NULL,
	[Customer Key] [int] NOT NULL,
	[Bill To Customer Key] [int] NOT NULL,
	[Stock Item Key] [int] NOT NULL,
	[Invoice Date Key] [date] NOT NULL,
	[Delivery Date Key] [date] NULL,
	[Salesperson Key] [int] NOT NULL,
	[WWI Invoice ID] [int] NOT NULL,
	[Description] [nvarchar](100) NOT NULL,
	[Package] [nvarchar](50) NOT NULL,
	[Quantity] [int] NOT NULL,
	[Unit Price] [decimal](18, 2) NOT NULL,
	[Tax Rate] [decimal](18, 3) NOT NULL,
	[Total Excluding Tax] [decimal](18, 2) NOT NULL,
	[Tax Amount] [decimal](18, 2) NOT NULL,
	[Profit] [decimal](18, 2) NOT NULL,
	[Total Including Tax] [decimal](18, 2) NOT NULL,
	[Total Dry Items] [int] NOT NULL,
	[Total Chiller Items] [int] NOT NULL,
	[Lineage Key] [int] NOT NULL);

CREATE CLUSTERED INDEX CCI_fact_Sale_CCI_ORDERED ON Fact.Sale_CCI_ORDERED ([Invoice Date Key]);

INSERT INTO Fact.Sale_CCI_ORDERED
	([Sale Key], [City Key], [Customer Key], [Bill To Customer Key], [Stock Item Key], [Invoice Date Key], [Delivery Date Key],
	 [Salesperson Key], [WWI Invoice ID], Description, Package, Quantity, [Unit Price], [Tax Rate],
	 [Total Excluding Tax], [Tax Amount], Profit, [Total Including Tax], [Total Dry Items],
	 [Total Chiller Items], [Lineage Key])
SELECT
	Sale.[Sale Key], Sale.[City Key], Sale.[Customer Key], Sale.[Bill To Customer Key], Sale.[Stock Item Key], Sale.[Invoice Date Key], Sale.[Delivery Date Key],
	Sale.[Salesperson Key], Sale.[WWI Invoice ID], Sale.Description, Sale.Package, Sale.Quantity, Sale.[Unit Price], Sale.[Tax Rate],
	Sale.[Total Excluding Tax], Sale.[Tax Amount], Sale.Profit, Sale.[Total Including Tax], Sale.[Total Dry Items],
	Sale.[Total Chiller Items], Sale.[Lineage Key]
FROM fact.Sale
CROSS JOIN
Dimension.City
WHERE City.[City Key] >= 1 AND City.[City Key] <= 110;

-- Create a clustered columnstore index on the table, removing the existing clustered rowstore index.
CREATE CLUSTERED COLUMNSTORE INDEX CCI_fact_Sale_CCI_ORDERED ON Fact.Sale_CCI_ORDERED WITH (MAXDOP = 1, DROP_EXISTING = ON);
GO

SELECT
	SUM([Quantity])
FROM Fact.Sale_CCI_ORDERED
WHERE [Invoice Date Key] >= '1/1/2016'
AND [Invoice Date Key] < '2/1/2016';

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

-- Compare table sizes
CREATE TABLE #storage_data
(	table_name VARCHAR(MAX),
	rows_used BIGINT,
	reserved VARCHAR(50),
	data VARCHAR(50),
	index_size VARCHAR(50),
	unused VARCHAR(50));

INSERT INTO #storage_data
	(table_name, rows_used, reserved, data, index_size, unused)
EXEC sp_MSforeachtable "EXEC sp_spaceused '?'";

UPDATE #storage_data
	SET reserved = LEFT(reserved, LEN(reserved) - 3),
		data = LEFT(data, LEN(data) - 3),
		index_size = LEFT(index_size, LEN(index_size) - 3),
		unused = LEFT(unused, LEN(unused) - 3);
SELECT
	table_name,
	rows_used,
	reserved / 1024 AS data_space_reserved_mb,
	data / 1024 AS data_space_used_mb,
	index_size / 1024 AS index_size_mb,
	unused AS free_space_kb,
	CAST(CAST(data AS DECIMAL(24,2)) / CAST(rows_used AS DECIMAL(24,2)) AS DECIMAL(24,4)) AS kb_per_row
FROM #storage_data
WHERE rows_used > 0
AND table_name IN ('Sale_CCI', 'Sale_CCI_ORDERED')
ORDER BY CAST(reserved AS INT) DESC;

DROP TABLE #storage_data;
GO

-- Test deletion of a single date from an ordered and unordered table.
SELECT COUNT(*) AS row_count FROM Fact.Sale_CCI WHERE [Invoice Date Key] = '1/1/2015';
SELECT COUNT(*) AS row_count FROM Fact.Sale_CCI_ORDERED WHERE [Invoice Date Key] = '1/1/2015';

UPDATE Sale_CCI
	SET [Total Dry Items] = [Total Dry Items] - 1,
		[Total Chiller Items] = [Total Chiller Items] + 1
FROM Fact.Sale_CCI -- Unordered
WHERE [Invoice Date Key] = '1/1/2015';
UPDATE Sale_CCI_ORDERED
	SET [Total Dry Items] = [Total Dry Items] - 1,
		[Total Chiller Items] = [Total Chiller Items] + 1
FROM Fact.Sale_CCI_ORDERED -- Ordered
WHERE [Invoice Date Key] = '1/1/2015';
