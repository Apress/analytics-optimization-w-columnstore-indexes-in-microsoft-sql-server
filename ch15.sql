USE WideWorldImportersDW;
SET STATISTICS IO ON;
GO

SELECT
	SUM(Quantity) AS Total_Quantity,
	SUM([Total Excluding Tax]) AS Total_Excluding_Tax
FROM Fact.Sale_CCI_PARTITIONED
WHERE [Invoice Date Key] = '7/17/2015';

CREATE PARTITION FUNCTION fact_Sale_CCI_years_function (DATE) AS RANGE RIGHT FOR VALUES 
('1/1/2014', '1/1/2015', '1/1/2016', '1/1/2017');

CREATE PARTITION SCHEME fact_Sale_CCI_years_scheme AS PARTITION fact_Sale_CCI_years_function
TO (WideWorldImportersDW_2013_fg, WideWorldImportersDW_2014_fg, WideWorldImportersDW_2015_fg,
WideWorldImportersDW_2016_fg, WideWorldImportersDW_2017_fg)

-- Segment metadata joined back to the parent table, column, and index.
SELECT
	indexes.name AS index_name,
	partitions.partition_number,
	column_store_segments.segment_id,
	column_store_segments.encoding_type,
	column_store_segments.row_count,
	column_store_segments.min_data_id,
	column_store_segments.max_data_id,
	column_store_segments.primary_dictionary_id
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
WHERE tables.name = 'Sale_CCI_PARTITIONED'
AND columns.name = 'Invoice Date Key'
ORDER BY columns.name, partitions.partition_number, column_store_segments.segment_id;

SELECT
	partitions.partition_number,
	objects.name,
	columns.name,
	column_store_dictionaries.type,
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
WHERE objects.name = 'Sale_CCI_PARTITIONED'
AND columns.name = 'Invoice Date Key'
AND column_store_dictionaries.dictionary_id = 0;


-- Segment metadata joined back to the parent table, column, and index.
SELECT
	indexes.name AS index_name,
	partitions.partition_number,
	column_store_segments.segment_id,
	column_store_segments.encoding_type,
	column_store_segments.row_count,
	column_store_segments.min_data_id,
	column_store_segments.max_data_id,
	column_store_segments.primary_dictionary_id
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
WHERE tables.name = 'Sale_CCI_PARTITIONED'
AND columns.name IN ('Quantity', 'Total Excluding Tax')
ORDER BY columns.name, partitions.partition_number, column_store_segments.segment_id;

SELECT
	SUM(Quantity) AS Total_Quantity,
	SUM([Total Excluding Tax]) AS Total_Excluding_Tax
FROM Fact.Sale_CCI_PARTITIONED
WHERE [Invoice Date Key] = '7/17/2015';

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

SELECT TOP 25
	column_store_dictionaries.dictionary_id,
	objects.name AS table_name,
	columns.name AS column_name,
	types.name AS data_type,
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
ORDER BY on_disk_size DESC;

CREATE TABLE #Sales_Temp_Data
(	[Sale Key] BIGINT NOT NULL,
	[Customer Key] INT NOT NULL,
	[Invoice Date Key] DATE NOT NULL,
	Quantity INT NOT NULL,
	[Total Excluding Tax] DECIMAL(18,2) NOT NULL);

INSERT INTO #Sales_Temp_Data
	([Sale Key], [Customer Key], [Invoice Date Key], Quantity, [Total Excluding Tax])
SELECT
	[Sale Key], [Customer Key], [Invoice Date Key], Quantity, [Total Excluding Tax]
FROM Fact.Sale_CCI_PARTITIONED;

CREATE CLUSTERED COLUMNSTORE INDEX CCI_Sales_Temp_Data ON #Sales_Temp_Data;

SELECT
	COUNT(*)
FROM #Sales_Temp_Data;

SELECT
	SUM(Quantity) * SUM([Total Excluding Tax])
FROM #Sales_Temp_Data
WHERE [Invoice Date Key] >= '1/1/2015'
AND [Invoice Date Key] < '1/1/2016';

DROP TABLE #Sales_Temp_Data
GO

CREATE TABLE #Sales_Temp_Data
(	[Sale Key] BIGINT NOT NULL,
	[Customer Key] INT NOT NULL,
	[Invoice Date Key] DATE NOT NULL,
	Quantity INT NOT NULL,
	[Total Excluding Tax] DECIMAL(18,2) NOT NULL);

INSERT INTO #Sales_Temp_Data
	([Sale Key], [Customer Key], [Invoice Date Key], Quantity, [Total Excluding Tax])
SELECT
	[Sale Key], [Customer Key], [Invoice Date Key], Quantity, [Total Excluding Tax]
FROM Fact.Sale_CCI_PARTITIONED;

CREATE CLUSTERED INDEX CI_Sales_Temp_Data ON #Sales_Temp_Data ([Sale Key]);

CREATE NONCLUSTERED COLUMNSTORE INDEX NCCI_Sales_Temp_Data ON #Sales_Temp_Data ([Invoice Date Key], Quantity, [Total Excluding Tax]);

DROP TABLE #Sales_Temp_Data

DECLARE @Sales_Temp_Data TABLE
(	[Sale Key] BIGINT NOT NULL,
	[Customer Key] INT NOT NULL,
	[Invoice Date Key] DATE NOT NULL,
	Quantity INT NOT NULL,
	[Total Excluding Tax] DECIMAL(18,2) NOT NULL);

INSERT INTO @Sales_Temp_Data
	([Sale Key], [Customer Key], [Invoice Date Key], Quantity, [Total Excluding Tax])
SELECT
	[Sale Key], [Customer Key], [Invoice Date Key], Quantity, [Total Excluding Tax]
FROM Fact.Sale_CCI_PARTITIONED;

CREATE CLUSTERED COLUMNSTORE INDEX CCI_Sales_Temp_Data ON @Sales_Temp_Data;
GO

USE master
GO
ALTER DATABASE WideWorldImporters ADD FILEGROUP WideWorldImporters_moltp CONTAINS MEMORY_OPTIMIZED_DATA;
ALTER DATABASE WideWorldImporters ADD FILE (name='WideWorldImporters_moltp', filename='C:\SQLData\WideWorldImporters_moltp') TO FILEGROUP WideWorldImporters_moltp;
ALTER DATABASE WideWorldImporters SET MEMORY_OPTIMIZED_ELEVATE_TO_SNAPSHOT = ON;
GO

USE WideWorldImporters;
GO

CREATE TABLE Sales.Orders_MOLTP
(	OrderID INT NOT NULL CONSTRAINT PK_Orders_MOLTP PRIMARY KEY NONCLUSTERED HASH WITH (BUCKET_COUNT = 150000),
	CustomerID INT NOT NULL,
	SalespersonPersonID INT NOT NULL,
	PickedByPersonID INT NULL,
	ContactPersonID INT NOT NULL,
	BackorderOrderID INT NULL,
	OrderDate DATE NOT NULL,
	ExpectedDeliveryDate DATE NOT NULL,
	CustomerPurchaseOrderNumber NVARCHAR(20) NULL,
	IsUndersupplyBackordered BIT NOT NULL,
	Comments NVARCHAR(MAX) NULL,
	DeliveryInstructions NVARCHAR(MAX) NULL,
	InternalComments NVARCHAR(MAX) NULL,
	PickingCompletedWhen DATETIME2(7) NULL,
	LastEditedBy INT NOT NULL,
	LastEditedWhen DATETIME2(7) NOT NULL,
	INDEX CCI_Orders_MOLTP CLUSTERED COLUMNSTORE)
WITH (MEMORY_OPTIMIZED = ON, DURABILITY = SCHEMA_AND_DATA);
GO

CREATE TABLE Sales.Orders_MOLTP
(	OrderID INT NOT NULL CONSTRAINT PK_Orders_MOLTP PRIMARY KEY NONCLUSTERED HASH WITH (BUCKET_COUNT = 150000),
	CustomerID INT NOT NULL,
	SalespersonPersonID INT NOT NULL,
	PickedByPersonID INT NULL,
	ContactPersonID INT NOT NULL,
	BackorderOrderID INT NULL,
	OrderDate DATE NOT NULL,
	ExpectedDeliveryDate DATE NOT NULL,
	CustomerPurchaseOrderNumber NVARCHAR(20) NULL,
	IsUndersupplyBackordered BIT NOT NULL,
	Comments NVARCHAR(500) NULL,
	DeliveryInstructions NVARCHAR(250) NULL,
	InternalComments NVARCHAR(500) NULL,
	PickingCompletedWhen DATETIME2(7) NULL,
	LastEditedBy INT NOT NULL,
	LastEditedWhen DATETIME2(7) NOT NULL,
	INDEX NCCI_Orders_MOLTP CLUSTERED COLUMNSTORE)
WITH (MEMORY_OPTIMIZED = ON, DURABILITY = SCHEMA_AND_DATA);
GO

INSERT INTO sales.Orders_MOLTP
(	OrderID, CustomerID, SalespersonPersonID, PickedByPersonID, ContactPersonID, BackorderOrderID, OrderDate, ExpectedDeliveryDate,
    CustomerPurchaseOrderNumber, IsUndersupplyBackordered, Comments, DeliveryInstructions, InternalComments, PickingCompletedWhen,
    LastEditedBy, LastEditedWhen)
SELECT
	OrderID, CustomerID, SalespersonPersonID, PickedByPersonID, ContactPersonID, BackorderOrderID, OrderDate, ExpectedDeliveryDate,
    CustomerPurchaseOrderNumber, IsUndersupplyBackordered, Comments, DeliveryInstructions, InternalComments, PickingCompletedWhen,
    LastEditedBy, LastEditedWhen
FROM Sales.Orders;

CREATE TABLE Sales.Orders_MOLTP_NO_CCI
(	OrderID INT NOT NULL CONSTRAINT PK_Orders_MOLTP_NO_CCI PRIMARY KEY NONCLUSTERED HASH WITH (BUCKET_COUNT = 150000),
	CustomerID INT NOT NULL,
	SalespersonPersonID INT NOT NULL,
	PickedByPersonID INT NULL,
	ContactPersonID INT NOT NULL,
	BackorderOrderID INT NULL,
	OrderDate DATE NOT NULL INDEX IX_Orders_MOLTP_NO_CCI_OrderDate NONCLUSTERED,
	ExpectedDeliveryDate DATE NOT NULL,
	CustomerPurchaseOrderNumber NVARCHAR(20) NULL,
	IsUndersupplyBackordered BIT NOT NULL,
	Comments NVARCHAR(500) NULL,
	DeliveryInstructions NVARCHAR(250) NULL,
	InternalComments NVARCHAR(500) NULL,
	PickingCompletedWhen DATETIME2(7) NULL,
	LastEditedBy INT NOT NULL,
	LastEditedWhen DATETIME2(7) NOT NULL)
WITH (MEMORY_OPTIMIZED = ON, DURABILITY = SCHEMA_AND_DATA);
GO

INSERT INTO sales.Orders_MOLTP_NO_CCI
(	OrderID, CustomerID, SalespersonPersonID, PickedByPersonID, ContactPersonID, BackorderOrderID, OrderDate, ExpectedDeliveryDate,
    CustomerPurchaseOrderNumber, IsUndersupplyBackordered, Comments, DeliveryInstructions, InternalComments, PickingCompletedWhen,
    LastEditedBy, LastEditedWhen)
SELECT
	OrderID, CustomerID, SalespersonPersonID, PickedByPersonID, ContactPersonID, BackorderOrderID, OrderDate, ExpectedDeliveryDate,
    CustomerPurchaseOrderNumber, IsUndersupplyBackordered, Comments, DeliveryInstructions, InternalComments, PickingCompletedWhen,
    LastEditedBy, LastEditedWhen
FROM Sales.Orders;

SELECT
	COUNT(*) AS OrderCount,
	COUNT(DISTINCT(CustomerID)) AS DistinctCustomerCount
FROM Sales.Orders
WHERE OrderDate >= '1/1/2015'
AND OrderDate < '4/1/2015';

SELECT
	COUNT(*) AS OrderCount,
	COUNT(DISTINCT(CustomerID)) AS DistinctCustomerCount
FROM Sales.Orders_MOLTP_NO_CCI
WHERE OrderDate >= '1/1/2015'
AND OrderDate < '4/1/2015';

SELECT
	COUNT(*) AS OrderCount,
	COUNT(DISTINCT(CustomerID)) AS DistinctCustomerCount
FROM Sales.Orders_MOLTP
WHERE OrderDate >= '1/1/2015'
AND OrderDate < '4/1/2015';

-- Example of a memory-optimized table with a multi-column covering index.
CREATE TABLE Sales.Orders_MOLTP_NO_CCI_MULTI
(	OrderID INT NOT NULL CONSTRAINT PK_Orders_MOLTP_NO_CCI_MULTI PRIMARY KEY NONCLUSTERED HASH WITH (BUCKET_COUNT = 150000),
	CustomerID INT NOT NULL,
	SalespersonPersonID INT NOT NULL,
	PickedByPersonID INT NULL,
	ContactPersonID INT NOT NULL,
	BackorderOrderID INT NULL,
	OrderDate DATE NOT NULL,
	ExpectedDeliveryDate DATE NOT NULL,
	CustomerPurchaseOrderNumber NVARCHAR(20) NULL,
	IsUndersupplyBackordered BIT NOT NULL,
	Comments NVARCHAR(500) NULL,
	DeliveryInstructions NVARCHAR(250) NULL,
	InternalComments NVARCHAR(500) NULL,
	PickingCompletedWhen DATETIME2(7) NULL,
	LastEditedBy INT NOT NULL,
	LastEditedWhen DATETIME2(7) NOT NULL,
	INDEX IX_Orders_MOLTP_NO_CCI_OrderDate NONCLUSTERED (OrderDate, CustomerID))
WITH (MEMORY_OPTIMIZED = ON, DURABILITY = SCHEMA_AND_DATA);
GO

INSERT INTO sales.Orders_MOLTP_NO_CCI_MULTI
(	OrderID, CustomerID, SalespersonPersonID, PickedByPersonID, ContactPersonID, BackorderOrderID, OrderDate, ExpectedDeliveryDate,
    CustomerPurchaseOrderNumber, IsUndersupplyBackordered, Comments, DeliveryInstructions, InternalComments, PickingCompletedWhen,
    LastEditedBy, LastEditedWhen)
SELECT
	OrderID, CustomerID, SalespersonPersonID, PickedByPersonID, ContactPersonID, BackorderOrderID, OrderDate, ExpectedDeliveryDate,
    CustomerPurchaseOrderNumber, IsUndersupplyBackordered, Comments, DeliveryInstructions, InternalComments, PickingCompletedWhen,
    LastEditedBy, LastEditedWhen
FROM Sales.Orders;

SELECT
	COUNT(*) AS OrderCount,
	COUNT(DISTINCT(CustomerID)) AS DistinctCustomerCount
FROM Sales.Orders_MOLTP_NO_CCI_MULTI
WHERE OrderDate >= '1/1/2015'
AND OrderDate < '4/1/2015';