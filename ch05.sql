USE WideWorldImportersDW;
GO

-- Dictionary Metadata in sys.column_store_dictionaries.
SELECT * FROM sys.column_store_dictionaries;

SELECT
	partitions.partition_number,
	objects.name AS table_name,
	columns.name AS column_name,
	types.name AS data_type,
	types.max_length,
	types.precision,
	types.scale,
	CASE
		WHEN column_store_dictionaries.dictionary_id = 0 THEN 'Global Dictionary'
		ELSE 'Local Dictionary'
	END AS dictionary_scope,
	CASE WHEN column_store_dictionaries.type = 1 THEN 'Hash dictionary containing int values'
		 WHEN column_store_dictionaries.type = 2 THEN 'Not used' -- Included for completeness
		 WHEN column_store_dictionaries.type = 3 THEN 'Hash dictionary containing string values'
		 WHEN column_store_dictionaries.type = 4 THEN 'Hash dictionary containing float values'
	END AS dictionary_type,
	column_store_dictionaries.entry_count,
	column_store_dictionaries.on_disk_size
FROM sys.column_store_dictionaries
INNER JOIN sys.partitions
ON column_store_dictionaries.hobt_id = partitions.hobt_id
INNER JOIN sys.objects
ON objects.object_id = partitions.object_id
INNER JOIN sys.columns
ON columns.column_id = column_store_dictionaries.column_id
AND columns.object_id = objects.object_id
INNER JOIN sys.types
ON types.user_type_id = columns.user_type_id
WHERE objects.name = 'Sale_CCI';

SELECT
	column_store_segments.segment_id,
	types.name AS data_type,
	types.max_length,
	types.precision,
	types.scale,
	CASE
		WHEN PRIMARY_DICTIONARY.dictionary_id IS NOT NULL THEN 1
		ELSE 0
	END AS does_global_dictionary_exist,
	PRIMARY_DICTIONARY.entry_count AS global_dictionary_entry_count,
	PRIMARY_DICTIONARY.on_disk_size AS global_dictionary_on_disk_size,
	CASE
		WHEN SECONDARY_DICTIONARY.dictionary_id IS NOT NULL THEN 1
		ELSE 0
	END AS does_local_dictionary_exist,
	SECONDARY_DICTIONARY.entry_count AS local_dictionary_entry_count,
	SECONDARY_DICTIONARY.on_disk_size AS local_dictionary_on_disk_size
FROM sys.column_store_segments
INNER JOIN sys.partitions
ON column_store_segments.hobt_id = partitions.hobt_id
INNER JOIN sys.objects
ON objects.object_id = partitions.object_id
INNER JOIN sys.columns
ON columns.object_id = objects.object_id
AND column_store_segments.column_id = columns.column_id
INNER JOIN sys.types
ON types.user_type_id = columns.user_type_id
LEFT JOIN sys.column_store_dictionaries PRIMARY_DICTIONARY
ON column_store_segments.primary_dictionary_id = PRIMARY_DICTIONARY.dictionary_id
AND column_store_segments.primary_dictionary_id <> -1
AND PRIMARY_DICTIONARY.column_id = columns.column_id
AND PRIMARY_DICTIONARY.hobt_id = partitions.hobt_id
LEFT JOIN sys.column_store_dictionaries SECONDARY_DICTIONARY
ON column_store_segments.secondary_dictionary_id = SECONDARY_DICTIONARY.dictionary_id
AND column_store_segments.secondary_dictionary_id <> -1
AND SECONDARY_DICTIONARY.column_id = columns.column_id
AND SECONDARY_DICTIONARY.hobt_id = partitions.hobt_id
WHERE objects.name = 'Sale_CCI'
AND columns.name = 'Bill To Customer Key';

-- New version of Sale_CCI with a normalized Description column
CREATE TABLE Dimension.Sale_Description
(	Description_Key SMALLINT NOT NULL IDENTITY(1,1) PRIMARY KEY CLUSTERED,
	[Description] NVARCHAR(100) NOT NULL);

CREATE TABLE Fact.Sale_CCI_Normalized
(	[Sale Key] [bigint] NOT NULL,
	[City Key] [int] NOT NULL,
	[Customer Key] [int] NOT NULL,
	[Bill To Customer Key] [int] NOT NULL,
	[Stock Item Key] [int] NOT NULL,
	[Invoice Date Key] [date] NOT NULL,
	[Delivery Date Key] [date] NULL,
	[Salesperson Key] [int] NOT NULL,
	[WWI Invoice ID] [int] NOT NULL,
	Description_Key SMALLINT NOT NULL,
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

INSERT INTO Dimension.Sale_Description
	(Description)
SELECT DISTINCT
	Description
FROM fact.Sale;

SELECT * FROM Dimension.Sale_Description;

INSERT INTO Fact.Sale_CCI_Normalized
	([Sale Key], [City Key], [Customer Key], [Bill To Customer Key], [Stock Item Key], [Invoice Date Key], [Delivery Date Key],
	 [Salesperson Key], [WWI Invoice ID], Description_Key, Package, Quantity, [Unit Price], [Tax Rate],
	 [Total Excluding Tax], [Tax Amount], Profit, [Total Including Tax], [Total Dry Items],
	 [Total Chiller Items], [Lineage Key])
SELECT
	Sale.[Sale Key], Sale.[City Key], Sale.[Customer Key], Sale.[Bill To Customer Key], Sale.[Stock Item Key], Sale.[Invoice Date Key], Sale.[Delivery Date Key],
	Sale.[Salesperson Key], Sale.[WWI Invoice ID], Sale_Description.Description_Key, Sale.Package, Sale.Quantity, Sale.[Unit Price], Sale.[Tax Rate],
	Sale.[Total Excluding Tax], Sale.[Tax Amount], Sale.Profit, Sale.[Total Including Tax], Sale.[Total Dry Items],
	Sale.[Total Chiller Items], Sale.[Lineage Key]
FROM fact.Sale
INNER JOIN Dimension.Sale_Description
ON Sale_Description.Description = Sale.Description
CROSS JOIN
Dimension.City
WHERE City.[City Key] >= 1 AND City.[City Key] <= 110;

-- Create a columnstore index on the table.
CREATE CLUSTERED COLUMNSTORE INDEX CCI_Sale_CCI_Normalized ON fact.Sale_CCI_Normalized;
GO

-- Compare table sizes between normalized and denormalized table
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
AND table_name IN ('Sale_CCI', 'Sale_CCI_Normalized')
ORDER BY CAST(reserved AS INT) DESC;

DROP TABLE #storage_data;
GO

SELECT
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
WHERE objects.name = 'Sale_CCI'
ORDER BY dm_db_column_store_row_group_physical_stats.row_group_id;

SET STATISTICS IO ON;
SELECT
	SUM(Quantity)
FROM fact.Sale_CCI
WHERE [Invoice Date Key] >= '1/1/2016'
AND [Invoice Date Key] < '2/1/2016';
GO

-- Create, populate, and test with a columnstore index using archive compression
CREATE TABLE Fact.Sale_CCI_Archive
(	[Sale Key] [bigint] NOT NULL,
	[City Key] [int] NOT NULL,
	[Customer Key] [int] NOT NULL,
	[Bill To Customer Key] [int] NOT NULL,
	[Stock Item Key] [int] NOT NULL,
	[Invoice Date Key] [date] NOT NULL,
	[Delivery Date Key] [date] NULL,
	[Salesperson Key] [int] NOT NULL,
	[WWI Invoice ID] [int] NOT NULL,
	[Description] NVARCHAR(100) NOT NULL,
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

INSERT INTO Fact.Sale_CCI_Archive
	([Sale Key], [City Key], [Customer Key], [Bill To Customer Key], [Stock Item Key], [Invoice Date Key], [Delivery Date Key],
	 [Salesperson Key], [WWI Invoice ID], Description, Package, Quantity, [Unit Price], [Tax Rate],
	 [Total Excluding Tax], [Tax Amount], Profit, [Total Including Tax], [Total Dry Items],
	 [Total Chiller Items], [Lineage Key])
SELECT
	Sale.[Sale Key], Sale.[City Key], Sale.[Customer Key], Sale.[Bill To Customer Key], Sale.[Stock Item Key], Sale.[Invoice Date Key], Sale.[Delivery Date Key],
	Sale.[Salesperson Key], Sale.[WWI Invoice ID], Sale_Description.Description_Key, Sale.Package, Sale.Quantity, Sale.[Unit Price], Sale.[Tax Rate],
	Sale.[Total Excluding Tax], Sale.[Tax Amount], Sale.Profit, Sale.[Total Including Tax], Sale.[Total Dry Items],
	Sale.[Total Chiller Items], Sale.[Lineage Key]
FROM fact.Sale
INNER JOIN Dimension.Sale_Description
ON Sale_Description.Description = Sale.Description
CROSS JOIN
Dimension.City
WHERE City.[City Key] >= 1 AND City.[City Key] <= 110;

-- Create a columnstore index on the table.
CREATE CLUSTERED COLUMNSTORE INDEX CCI_Sale_CCI_Archive ON fact.Sale_CCI_Archive WITH (DATA_COMPRESSION=COLUMNSTORE_ARCHIVE);
GO

