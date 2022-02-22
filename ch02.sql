USE WideWorldImporters;
GO

SELECT
	*
FROM Sales.Orders;

SELECT
	Orders.OrderID,
	Customers.CustomerName,
	Orders.OrderDate,
	Orders.ExpectedDeliveryDate,
	Orders.CustomerPurchaseOrderNumber,
	OrderLines.*
FROM Sales.Orders
INNER JOIN Sales.Customers
ON Customers.CustomerID = Orders.CustomerID
INNER JOIN Sales.OrderLines
ON OrderLines.OrderID = Orders.OrderID
WHERE Customers.CustomerID = 10
AND Orders.OrderDate >= '5/20/2016'
AND Orders.OrderDate < '5/27/2016';

USE WideWorldImportersDW;
GO

SELECT
	Date.[Calendar Year] AS Order_Year,
	Date.[Calendar Month Number] AS Order_Month,
	COUNT(*) AS Order_Count,
	SUM(Quantity) AS Quantity_Total,
	SUM([Total Excluding Tax]) AS [Total Excluding Tax]
FROM Fact.[Order]
INNER JOIN Dimension.Date
ON Date.Date = [Order].[Order Date Key]
WHERE [Order].[Order Date Key] >= '1/1/2016'
AND [Order].[Order Date Key] < '1/1/2017'
GROUP BY Date.[Calendar Year], Date.[Calendar Month Number]
ORDER BY Date.[Calendar Year], Date.[Calendar Month Number];


