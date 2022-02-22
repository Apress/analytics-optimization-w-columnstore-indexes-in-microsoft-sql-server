USE WideWorldImportersDW;
SET NOCOUNT ON;
SET STATISTICS IO ON;
GO

-- Query that generates 25,109,150 rows spanning the date range of 1/1/2013 to 5/31/2016 that will be
-- used throughout this book to demonstrate columnstore index-related topics.
SELECT
	Sale.[Sale Key], Sale.[City Key], Sale.[Customer Key], Sale.[Bill To Customer Key], Sale.[Stock Item Key], Sale.[Invoice Date Key], Sale.[Delivery Date Key],
	Sale.[Salesperson Key], Sale.[WWI Invoice ID], Sale.Description, Sale.Package, Sale.Quantity, Sale.[Unit Price], Sale.[Tax Rate],
	Sale.[Total Excluding Tax], Sale.[Tax Amount], Sale.Profit, Sale.[Total Including Tax], Sale.[Total Dry Items],
	Sale.[Total Chiller Items], Sale.[Lineage Key]
FROM fact.Sale
CROSS JOIN
Dimension.City
WHERE City.[City Key] >= 1 AND City.[City Key] <= 110;


-- Columnstore index table used in demonstrations.  This is an unordered columnstore index
-- based off of the Fact.Sale table

CREATE TABLE Fact.Sale_CCI
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

INSERT INTO Fact.Sale_CCI
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

-- Create a columnstore index on the table.
CREATE CLUSTERED COLUMNSTORE INDEX CCI_fact_sale_CCI ON fact.Sale_CCI;
GO

-- Check the size of this data.
-- The table contains 25109150 rows spanning the date range of 1/1/2013 to 5/31/2016.
SELECT
	COUNT(*),
	MIN([Invoice Date Key]),
	MAX([Invoice Date Key])
FROM fact.Sale_CCI;

-- Row group metadata
SELECT * FROM sys.column_store_row_groups;

-- Row group metadata joined back to the parent table and index.
SELECT
	tables.name AS table_name,
	indexes.name AS index_name,
	column_store_row_groups.partition_number,
	column_store_row_groups.row_group_id,
	column_store_row_groups.state_description,
	column_store_row_groups.total_rows,
	column_store_row_groups.deleted_rows,
	column_store_row_groups.size_in_bytes
FROM sys.column_store_row_groups
INNER JOIN sys.indexes
ON indexes.index_id = column_store_row_groups.index_id
AND indexes.object_id = column_store_row_groups.object_id
INNER JOIN sys.tables
ON tables.object_id = indexes.object_id
WHERE tables.name = 'Sale_CCI'
ORDER BY tables.object_id, indexes.index_id, column_store_row_groups.row_group_id;

-- Segment metadata
SELECT * FROM sys.column_store_segments;

-- Segment metadata joined back to the parent table, column, and index.
SELECT
	tables.name AS table_name,
	indexes.name AS index_name,
	columns.name AS column_name,
	partitions.partition_number,
	column_store_segments.row_count,
	column_store_segments.has_nulls,
	column_store_segments.min_data_id,
	column_store_segments.max_data_id,
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

