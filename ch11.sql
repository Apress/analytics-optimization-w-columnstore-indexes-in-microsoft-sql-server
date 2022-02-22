USE WideWorldImportersDW;
SET STATISTICS IO ON;
GO

ALTER DATABASE WideWorldImportersDW ADD FILEGROUP WideWorldImportersDW_2013_fg;
ALTER DATABASE WideWorldImportersDW ADD FILEGROUP WideWorldImportersDW_2014_fg;
ALTER DATABASE WideWorldImportersDW ADD FILEGROUP WideWorldImportersDW_2015_fg;
ALTER DATABASE WideWorldImportersDW ADD FILEGROUP WideWorldImportersDW_2016_fg;
ALTER DATABASE WideWorldImportersDW ADD FILEGROUP WideWorldImportersDW_2017_fg;

ALTER DATABASE WideWorldImportersDW ADD FILE
	(NAME = WideWorldImportersDW_2013_data, FILENAME = 'C:\SQLData\WideWorldImportersDW_2013_data.ndf',
	 SIZE = 200MB, MAXSIZE = UNLIMITED, FILEGROWTH = 1GB)
TO FILEGROUP WideWorldImportersDW_2013_fg;
ALTER DATABASE WideWorldImportersDW ADD FILE
	(NAME = WideWorldImportersDW_2014_data, FILENAME = 'C:\SQLData\WideWorldImportersDW_2014_data.ndf',
	 SIZE = 200MB, MAXSIZE = UNLIMITED, FILEGROWTH = 1GB)
TO FILEGROUP WideWorldImportersDW_2014_fg;
ALTER DATABASE WideWorldImportersDW ADD FILE
	(NAME = WideWorldImportersDW_2015_data, FILENAME = 'C:\SQLData\WideWorldImportersDW_2015_data.ndf',
	 SIZE = 200MB, MAXSIZE = UNLIMITED, FILEGROWTH = 1GB)
TO FILEGROUP WideWorldImportersDW_2015_fg;
ALTER DATABASE WideWorldImportersDW ADD FILE
	(NAME = WideWorldImportersDW_2016_data, FILENAME = 'C:\SQLData\WideWorldImportersDW_2016_data.ndf',
	 SIZE = 200MB, MAXSIZE = UNLIMITED, FILEGROWTH = 1GB)
TO FILEGROUP WideWorldImportersDW_2016_fg;
ALTER DATABASE WideWorldImportersDW ADD FILE
	(NAME = WideWorldImportersDW_2017_data, FILENAME = 'C:\SQLData\WideWorldImportersDW_2017_data.ndf',
	 SIZE = 200MB, MAXSIZE = UNLIMITED, FILEGROWTH = 1GB)
TO FILEGROUP WideWorldImportersDW_2017_fg;

CREATE PARTITION FUNCTION fact_Sale_CCI_years_function (DATE)
AS RANGE RIGHT FOR VALUES
	('1/1/2014', '1/1/2015', '1/1/2016', '1/1/2017');
GO

CREATE PARTITION SCHEME fact_Sale_CCI_years_scheme
AS PARTITION fact_Sale_CCI_years_function
TO (WideWorldImportersDW_2013_fg, WideWorldImportersDW_2014_fg, WideWorldImportersDW_2015_fg, WideWorldImportersDW_2016_fg, WideWorldImportersDW_2017_fg);
GO

-- This will generate an error if executed!
CREATE PARTITION SCHEME fact_Sale_CCI_years_scheme_with_errors
AS PARTITION fact_Sale_CCI_years_function
TO (WideWorldImportersDW_2014_fg, WideWorldImportersDW_2015_fg, WideWorldImportersDW_2016_fg, WideWorldImportersDW_2017_fg);
GO

-- This will throw an error to demonstrate that the partition column and function must have the same data type.
CREATE TABLE Fact.Sale_CCI_PARTITIONED_error
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
	[Lineage Key] [int] NOT NULL)
	ON fact_Sale_CCI_years_scheme ([Customer Key]);
	
CREATE TABLE Fact.Sale_CCI_PARTITIONED
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
	[Lineage Key] [int] NOT NULL)
	ON fact_Sale_CCI_years_scheme ([Invoice Date Key]);

CREATE CLUSTERED INDEX CCI_fact_Sale_CCI_PARTITIONED ON Fact.Sale_CCI_PARTITIONED ([Invoice Date Key]);

INSERT INTO Fact.Sale_CCI_PARTITIONED
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
CREATE CLUSTERED COLUMNSTORE INDEX CCI_fact_Sale_CCI_PARTITIONED ON Fact.Sale_CCI_PARTITIONED WITH (MAXDOP = 1, DROP_EXISTING = ON);
GO

SELECT
	SUM([Quantity])
FROM Fact.Sale_CCI_ORDERED
WHERE [Invoice Date Key] >= '1/1/2016'
AND [Invoice Date Key] < '2/1/2016';

SELECT
	SUM([Quantity])
FROM Fact.Sale_CCI_PARTITIONED
WHERE [Invoice Date Key] >= '1/1/2016'
AND [Invoice Date Key] < '2/1/2016';

-- 61 seconds
ALTER INDEX CCI_fact_Sale_CCI_ORDERED ON Fact.Sale_CCI_ORDERED REBUILD;
GO
 -- 11 seconds
ALTER INDEX CCI_fact_Sale_CCI_PARTITIONED ON Fact.Sale_CCI_PARTITIONED REBUILD PARTITION = 4;
GO
-- 1 second
ALTER INDEX CCI_fact_Sale_CCI_PARTITIONED ON Fact.Sale_CCI_PARTITIONED REBUILD PARTITION = 5;
GO

CREATE TABLE Fact.Sale_CCI_STAGING
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
	[Lineage Key] [int] NOT NULL)
	ON WideWorldImportersDW_2016_fg;

CREATE CLUSTERED COLUMNSTORE INDEX CCI_fact_Sale_CCI_STAGING ON Fact.Sale_CCI_STAGING;
GO

ALTER TABLE Fact.Sale_CCI_PARTITIONED SWITCH PARTITION 4 TO Fact.Sale_CCI_STAGING;
GO

SELECT COUNT(*) FROM Fact.Sale_CCI_STAGING;

UPDATE Sale_CCI_STAGING
	SET [Total Dry Items] = [Total Dry Items] + 1
FROM Fact.Sale_CCI_STAGING;

INSERT INTO Fact.Sale_CCI_PARTITIONED
	([Sale Key], [City Key], [Customer Key], [Bill To Customer Key], [Stock Item Key], [Invoice Date Key], [Delivery Date Key], [Salesperson Key],
     [WWI Invoice ID], Description, Package, Quantity, [Unit Price], [Tax Rate], [Total Excluding Tax], [Tax Amount], Profit, [Total Including Tax],
     [Total Dry Items], [Total Chiller Items], [Lineage Key])
SELECT
	[Sale Key], [City Key], [Customer Key], [Bill To Customer Key], [Stock Item Key], [Invoice Date Key], [Delivery Date Key], [Salesperson Key],
     [WWI Invoice ID], Description, Package, Quantity, [Unit Price], [Tax Rate], [Total Excluding Tax], [Tax Amount], Profit, [Total Including Tax],
     [Total Dry Items], [Total Chiller Items], [Lineage Key]
FROM Fact.Sale_CCI_STAGING;

DROP TABLE Fact.Sale_CCI_STAGING;
GO

UPDATE Sale_CCI_STAGING
	SET [Total Dry Items] = [Total Dry Items] + 1
FROM Fact.Sale_CCI_STAGING;

ALTER TABLE Fact.Sale_CCI_STAGING SWITCH PARTITION 4 TO Fact.Sale_CCI_PARTITIONED;
GO

DROP TABLE Fact.Sale_CCI_STAGING;
GO