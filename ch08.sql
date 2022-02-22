USE WideWorldImportersDW;
SET STATISTICS IO ON;
GO

CREATE TABLE Fact.Sale_Transactional (
	[Sale Key] [bigint] NOT NULL,
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
	[Lineage Key] [int] NOT NULL,
 CONSTRAINT PK_Fact_Sale_Transactional PRIMARY KEY NONCLUSTERED 
(	[Sale Key] ASC,
	[Invoice Date Key] ASC))
WITH (DATA_COMPRESSION = PAGE);
	
INSERT INTO fact.Sale_Transactional
	([Sale Key], [City Key],[Customer Key], [Bill To Customer Key], [Stock Item Key], [Invoice Date Key], [Delivery Date Key], [Salesperson Key], [WWI Invoice ID],
     Description, Package, Quantity, [Unit Price], [Tax Rate], [Total Excluding Tax], [Tax Amount], Profit, [Total Including Tax], [Total Dry Items],
     [Total Chiller Items], [Lineage Key])
SELECT TOP 102400
	*
FROM Fact.Sale;

SELECT
	fn_dblog.allocunitname,
	SUM(fn_dblog.[log record length]) AS log_size
FROM sys.fn_dblog (NULL, NULL)
WHERE fn_dblog.allocunitname = ('Fact.Sale_Transactional.PK_Fact_Sale_Transactional')
GROUP BY fn_dblog.allocunitname;

CREATE TABLE Fact.Sale_CCI_Clean_Test (
	[Sale Key] [bigint] NOT NULL,
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
	
CREATE CLUSTERED COLUMNSTORE INDEX CCI_Sale_CCI_Clean_Test ON Fact.Sale_CCI_Clean_Test;

INSERT INTO fact.Sale_CCI_Clean_Test
	([Sale Key], [City Key],[Customer Key], [Bill To Customer Key], [Stock Item Key], [Invoice Date Key], [Delivery Date Key], [Salesperson Key], [WWI Invoice ID],
     Description, Package, Quantity, [Unit Price], [Tax Rate], [Total Excluding Tax], [Tax Amount], Profit, [Total Including Tax], [Total Dry Items],
     [Total Chiller Items], [Lineage Key])
SELECT TOP 102400
	*
FROM Fact.Sale;

SELECT
	fn_dblog.allocunitname,
	SUM(fn_dblog.[log record length]) AS log_size
FROM sys.fn_dblog (NULL, NULL)
WHERE fn_dblog.allocunitname = ('Fact.Sale_CCI_Clean_Test.CCI_Sale_CCI_Clean_Test')
GROUP BY fn_dblog.allocunitname;

DROP TABLE Fact.Sale_CCI_Clean_Test;

CREATE TABLE Fact.Sale_CCI_Clean_Test_2 (
	[Sale Key] [bigint] NOT NULL,
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

CREATE CLUSTERED COLUMNSTORE INDEX CCI_Sale_CCI_Clean_Test_2 ON Fact.Sale_CCI_Clean_Test_2;

INSERT INTO fact.Sale_CCI_Clean_Test_2
	([Sale Key], [City Key],[Customer Key], [Bill To Customer Key], [Stock Item Key], [Invoice Date Key], [Delivery Date Key], [Salesperson Key], [WWI Invoice ID],
     Description, Package, Quantity, [Unit Price], [Tax Rate], [Total Excluding Tax], [Tax Amount], Profit, [Total Including Tax], [Total Dry Items],
     [Total Chiller Items], [Lineage Key])
SELECT TOP 102399
	*
FROM Fact.Sale;

SELECT
	fn_dblog.allocunitname,
	SUM(fn_dblog.[log record length]) AS log_size
FROM sys.fn_dblog (NULL, NULL)
WHERE fn_dblog.allocunitname IN ('Fact.Sale_CCI_Clean_Test_2.CCI_Sale_CCI_Clean_Test_2', 'Fact.Sale_CCI_Clean_Test_2.CCI_Sale_CCI_Clean_Test_2(Delta)')
GROUP BY fn_dblog.allocunitname;

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
WHERE tables.name = 'Sale_CCI_Clean_Test_2'
ORDER BY indexes.index_id, column_store_row_groups.row_group_id;

ALTER INDEX CCI_Sale_CCI_Clean_Test_2 ON Fact.Sale_CCI_Clean_Test_2 REORGANIZE WITH (COMPRESS_ALL_ROW_GROUPS = ON);

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
WHERE tables.name = 'Sale_CCI_Clean_Test_2'
ORDER BY indexes.index_id, column_store_row_groups.row_group_id;

ALTER INDEX CCI_Sale_CCI_Clean_Test_2 ON Fact.Sale_CCI_Clean_Test_2 REORGANIZE WITH (COMPRESS_ALL_ROW_GROUPS = ON);

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
WHERE tables.name = 'Sale_CCI_Clean_Test_2'
ORDER BY indexes.index_id, column_store_row_groups.row_group_id;

DROP TABLE Fact.Sale_CCI_Clean_Test_2;