USE WideWorldImporters;
GO

-- Query against a sales table
SELECT
	OrderID, --  An identity/primary key column
	CustomerID,
	SalespersonPersonID,
	ContactPersonID,
	OrderDate
FROM sales.Orders
WHERE OrderId = 289;

SELECT
	SUM(Quantity) AS Total_Quantity
FROM sales.OrderLines
WHERE OrderID >= 1
AND OrderID < 10000;
GO

USE WideWorldImportersDW;
GO

