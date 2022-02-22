USE WideWorldImporters;
SET STATISTICS IO ON;
GO

SELECT
	COUNT(*) AS sale_count,
	COUNT(DISTINCT SalespersonPersonID) AS sales_people_count,
	SUM(CAST(IsUndersupplyBackordered AS INT)) AS undersupply_backorder_count
FROM Sales.Orders
WHERE CustomerID = 90
AND OrderDate >= '1/1/2015'
AND OrderDate < '1/1/2016';

CREATE NONCLUSTERED INDEX NCI_Orders_covering
ON Sales.Orders	(OrderDate, CustomerID)
INCLUDE (SalespersonPersonID, IsUndersupplyBackordered);

SELECT
	COUNT(*) AS sale_count,
	COUNT(DISTINCT SalespersonPersonID) AS sales_people_count,
	SUM(CAST(IsUndersupplyBackordered AS INT)) AS undersupply_backorder_count
FROM Sales.Orders
WHERE CustomerID = 90
AND OrderDate >= '1/1/2015'
AND OrderDate < '1/1/2016';

DROP INDEX NCI_Orders_covering ON Sales.Orders;
GO

CREATE NONCLUSTERED COLUMNSTORE INDEX NCCI_Orders ON Sales.Orders (OrderDate, CustomerID, IsUndersupplyBackordered, SalespersonPersonID);
GO

SELECT
	COUNT(*) AS sale_count,
	COUNT(DISTINCT SalespersonPersonID) AS sales_people_count,
	SUM(CAST(IsUndersupplyBackordered AS INT)) AS undersupply_backorder_count
FROM Sales.Orders
WHERE CustomerID = 90
AND OrderDate >= '1/1/2015'
AND OrderDate < '1/1/2016';

SELECT
	dm_db_column_store_row_group_physical_stats.has_vertipaq_optimization
FROM sys.dm_db_column_store_row_group_physical_stats
INNER JOIN sys.objects
ON objects.object_id = dm_db_column_store_row_group_physical_stats.object_id
INNER JOIN sys.partitions
ON partitions.object_id = objects.object_id
AND partitions.partition_number = dm_db_column_store_row_group_physical_stats.partition_number
AND partitions.index_id = dm_db_column_store_row_group_physical_stats.index_id
WHERE objects.name = 'Orders'
ORDER BY dm_db_column_store_row_group_physical_stats.row_group_id;

DROP INDEX NCCI_Orders ON Sales.Orders;
GO

CREATE NONCLUSTERED COLUMNSTORE INDEX NCCI_Orders
ON Sales.Orders (OrderDate, CustomerID, IsUndersupplyBackordered, SalespersonPersonID)
WITH (COMPRESSION_DELAY = 10 MINUTES);
GO

SELECT
	COUNT(*) AS sale_count,
	COUNT(DISTINCT SalespersonPersonID) AS sales_people_count,
	SUM(CAST(IsUndersupplyBackordered AS INT)) AS undersupply_backorder_count
FROM Sales.Orders
WHERE CustomerID = 90
AND OrderDate >= '1/1/2015'
AND OrderDate < '1/1/2016';

ALTER INDEX NCCI_Orders ON Sales.Orders
SET (COMPRESSION_DELAY = 30 MINUTES);
GO

ALTER INDEX NCCI_Orders ON Sales.Orders
SET (COMPRESSION_DELAY = 360 MINUTES);
GO

DROP INDEX NCCI_Orders ON Sales.Orders;
GO

USE WideWorldImportersDW;
GO

ALTER INDEX CCI_fact_sale_CCI ON Fact.Sale_CCI
SET (COMPRESSION_DELAY = 60 MINUTES);
GO

SELECT
	tables.name AS table_name,
	indexes.name AS index_name,
	indexes.type_desc AS index_type,
	indexes.compression_delay
FROM sys.indexes
INNER JOIN sys.tables
ON tables.object_id = indexes.object_id
WHERE indexes.type_desc IN ('NONCLUSTERED COLUMNSTORE', 'CLUSTERED COLUMNSTORE');

SELECT
	objects.name,
	partitions.partition_number,
	dm_db_column_store_row_group_physical_stats.row_group_id,
	dm_db_column_store_row_group_physical_stats.total_rows,
	dm_db_column_store_row_group_physical_stats.deleted_rows,
	CAST(100 * CAST(deleted_rows AS DECIMAL(18,2)) / CAST(total_rows AS DECIMAL(18,2)) AS DECIMAL(18,2)) AS percent_deleted
FROM sys.dm_db_column_store_row_group_physical_stats
INNER JOIN sys.objects
ON objects.object_id = dm_db_column_store_row_group_physical_stats.object_id
INNER JOIN sys.partitions
ON partitions.object_id = objects.object_id
AND partitions.partition_number = dm_db_column_store_row_group_physical_stats.partition_number
AND partitions.index_id = dm_db_column_store_row_group_physical_stats.index_id
WHERE objects.name = 'Sale_CCI'
ORDER BY dm_db_column_store_row_group_physical_stats.row_group_id;

USE WideWorldImporters;
GO

CREATE NONCLUSTERED COLUMNSTORE INDEX NCCI_Orders
ON Sales.Orders (OrderDate, CustomerID, IsUndersupplyBackordered, SalespersonPersonID)
WHERE PickedByPersonID IS NOT NULL;
GO

SELECT
	SUM(CASE WHEN PickedByPersonID IS NULL THEN 1 ELSE 0 END) AS orders_not_picked,
	SUM(CASE WHEN PickedByPersonID IS NOT NULL THEN 1 ELSE 0 END) AS orders_picked
FROM Sales.Orders

SELECT
	COUNT(*) AS sale_count,
	COUNT(DISTINCT SalespersonPersonID) AS sales_people_count,
	SUM(CAST(IsUndersupplyBackordered AS INT)) AS undersupply_backorder_count
FROM Sales.Orders
WHERE CustomerID = 90
AND OrderDate >= '1/1/2015'
AND OrderDate < '1/1/2016'
AND PickedByPersonID IS NOT NULL;

SELECT
	indexes.name,
	indexes.type_desc,
	indexes.filter_definition
FROM sys.indexes
WHERE indexes.has_filter = 1;

DROP INDEX NCCI_Orders ON Sales.Orders;
GO

CREATE NONCLUSTERED COLUMNSTORE INDEX NCCI_Orders
ON Sales.Orders (OrderDate, CustomerID, IsUndersupplyBackordered, SalespersonPersonID)
WHERE PickedByPersonID IS NOT NULL
WITH (COMPRESSION_DELAY = 30 MINUTES);
GO

SELECT
	dm_db_column_store_row_group_physical_stats.has_vertipaq_optimization
FROM sys.dm_db_column_store_row_group_physical_stats
INNER JOIN sys.objects
ON objects.object_id = dm_db_column_store_row_group_physical_stats.object_id
INNER JOIN sys.partitions
ON partitions.object_id = objects.object_id
AND partitions.partition_number = dm_db_column_store_row_group_physical_stats.partition_number
AND partitions.index_id = dm_db_column_store_row_group_physical_stats.index_id
WHERE objects.name = 'Orders'
ORDER BY dm_db_column_store_row_group_physical_stats.row_group_id;

INSERT INTO Sales.Orders
(   OrderID, CustomerID, SalespersonPersonID, PickedByPersonID, ContactPersonID, BackorderOrderID, OrderDate, ExpectedDeliveryDate, CustomerPurchaseOrderNumber,
    IsUndersupplyBackordered, Comments, DeliveryInstructions, InternalComments, PickingCompletedWhen, LastEditedBy, LastEditedWhen)
SELECT
    73595 + ROW_NUMBER() OVER (ORDER BY OrderID) AS OrderID,
    CustomerID,
    SalespersonPersonID,
    PickedByPersonID,
    ContactPersonID,
    BackorderOrderID,
    OrderDate,
    ExpectedDeliveryDate,
    CustomerPurchaseOrderNumber,
    IsUndersupplyBackordered,
    Comments,
    DeliveryInstructions,
    InternalComments,
    PickingCompletedWhen,
    LastEditedBy,
    LastEditedWhen
FROM Sales.Orders

SELECT
	objects.name AS table_name,
	partitions.partition_number,
	dm_db_column_store_row_group_physical_stats.row_group_id,
	dm_db_column_store_row_group_physical_stats.has_vertipaq_optimization,
	dm_db_column_store_row_group_physical_stats.state_desc
FROM sys.dm_db_column_store_row_group_physical_stats
INNER JOIN sys.objects
ON objects.object_id = dm_db_column_store_row_group_physical_stats.object_id
INNER JOIN sys.partitions
ON partitions.object_id = objects.object_id
AND partitions.partition_number = dm_db_column_store_row_group_physical_stats.partition_number
AND partitions.index_id = dm_db_column_store_row_group_physical_stats.index_id
WHERE objects.name = 'Orders'
ORDER BY dm_db_column_store_row_group_physical_stats.row_group_id;

ALTER INDEX NCCI_Orders ON sales.Orders REORGANIZE WITH (COMPRESS_ALL_ROW_GROUPS = ON);
GO

SELECT
	objects.name AS table_name,
	partitions.partition_number,
	dm_db_column_store_row_group_physical_stats.row_group_id,
	dm_db_column_store_row_group_physical_stats.has_vertipaq_optimization,
	dm_db_column_store_row_group_physical_stats.state_desc
FROM sys.dm_db_column_store_row_group_physical_stats
INNER JOIN sys.objects
ON objects.object_id = dm_db_column_store_row_group_physical_stats.object_id
INNER JOIN sys.partitions
ON partitions.object_id = objects.object_id
AND partitions.partition_number = dm_db_column_store_row_group_physical_stats.partition_number
AND partitions.index_id = dm_db_column_store_row_group_physical_stats.index_id
WHERE objects.name = 'Orders'
ORDER BY dm_db_column_store_row_group_physical_stats.row_group_id;
-- Wait a few minutes

SELECT
	objects.name AS table_name,
	partitions.partition_number,
	dm_db_column_store_row_group_physical_stats.row_group_id,
	dm_db_column_store_row_group_physical_stats.has_vertipaq_optimization,
	dm_db_column_store_row_group_physical_stats.state_desc
FROM sys.dm_db_column_store_row_group_physical_stats
INNER JOIN sys.objects
ON objects.object_id = dm_db_column_store_row_group_physical_stats.object_id
INNER JOIN sys.partitions
ON partitions.object_id = objects.object_id
AND partitions.partition_number = dm_db_column_store_row_group_physical_stats.partition_number
AND partitions.index_id = dm_db_column_store_row_group_physical_stats.index_id
WHERE objects.name = 'Orders'
ORDER BY dm_db_column_store_row_group_physical_stats.row_group_id;

SELECT
	tables.name AS TableName, 
	indexes.name AS IndexName,
	dm_db_index_usage_stats.user_seeks,
	dm_db_index_usage_stats.user_scans,
	dm_db_index_usage_stats.user_lookups,
	dm_db_index_usage_stats.user_updates,
	dm_db_index_usage_stats.last_user_seek,
	dm_db_index_usage_stats.last_user_scan,
	dm_db_index_usage_stats.last_user_lookup,
	dm_db_index_usage_stats.last_user_update
FROM sys.dm_db_index_usage_stats
INNER JOIN sys.tables
ON tables.object_id = dm_db_index_usage_stats.object_id
INNER JOIN sys.indexes
ON indexes.object_id = dm_db_index_usage_stats.object_id
AND indexes.index_id = dm_db_index_usage_stats.index_id
WHERE tables.name = 'Orders'

