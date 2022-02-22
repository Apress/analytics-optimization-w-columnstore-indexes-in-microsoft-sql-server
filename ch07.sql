USE WideWorldImportersDW;
SET STATISTICS IO ON;
-- Turn on actual execution plan
GO

SELECT
	Employee,
	[WWI Employee ID],
	[Preferred Name],
	[Is Salesperson]
FROM Dimension.Employee
WHERE [Employee Key] = 17;

SELECT
	*
FROM fact.Sale
WHERE [Invoice Date Key] = '1/1/2016'
AND [Sale Key] = 198840;

SELECT
	COUNT(*),
	MIN([Invoice Date Key]),
	MAX([Invoice Date Key])
FROM fact.Sale
WHERE [Invoice Date Key] >= '1/1/2016';

SELECT
	City.City,
	City.[State Province],
	City.Country,
	COUNT(*)
FROM fact.Sale
INNER JOIN Dimension.City
ON City.[City Key] = Sale.[City Key]
WHERE [Invoice Date Key] >= '1/1/2016'
AND [Invoice Date Key] < '2/1/2016'
GROUP BY City.City, City.[State Province], City.Country;

SELECT
	databases.compatibility_level
FROM sys.databases
WHERE databases.name = 'WideWorldImportersDW';

ALTER DATABASE WideWorldImportersDW SET COMPATIBILITY_LEVEL = 150;

SELECT
	City.City,
	City.[State Province],
	City.Country,
	COUNT(*)
FROM fact.Sale
INNER JOIN Dimension.City
ON City.[City Key] = Sale.[City Key]
WHERE [Invoice Date Key] >= '1/1/2016'
AND [Invoice Date Key] < '2/1/2016'
GROUP BY City.City, City.[State Province], City.Country;

ALTER DATABASE WideWorldImportersDW SET COMPATIBILITY_LEVEL = 120;
GO

SELECT
	Sale.[City Key],
	COUNT(*)
FROM fact.Sale
GROUP BY Sale.[City Key]

ALTER DATABASE WideWorldImportersDW SET COMPATIBILITY_LEVEL = 130;
GO

SELECT
	Sale.[City Key],
	COUNT(*)
FROM fact.Sale
GROUP BY Sale.[City Key]

ALTER DATABASE WideWorldImportersDW SET COMPATIBILITY_LEVEL = 120;
GO

SELECT
	Sale.[City Key],
	COUNT(*)
FROM fact.Sale
GROUP BY Sale.[City Key]

ALTER DATABASE WideWorldImportersDW SET COMPATIBILITY_LEVEL = 130;
GO

SELECT
	Sale.[City Key],
	COUNT(*)
FROM fact.Sale
GROUP BY Sale.[City Key];

ALTER DATABASE WideWorldImportersDW SET COMPATIBILITY_LEVEL = 150;
GO

ALTER DATABASE SCOPED CONFIGURATION SET BATCH_MODE_ON_ROWSTORE = OFF;

ALTER DATABASE SCOPED CONFIGURATION SET BATCH_MODE_MEMORY_GRANT_FEEDBACK = OFF;

ALTER DATABASE SCOPED CONFIGURATION SET BATCH_MODE_ADAPTIVE_JOINS = OFF;



ALTER DATABASE SCOPED CONFIGURATION SET BATCH_MODE_ON_ROWSTORE = ON;

ALTER DATABASE SCOPED CONFIGURATION SET BATCH_MODE_MEMORY_GRANT_FEEDBACK = ON;

ALTER DATABASE SCOPED CONFIGURATION SET BATCH_MODE_ADAPTIVE_JOINS = ON;
