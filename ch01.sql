USE WideWorldImportersDW;
GO

CREATE TABLE dbo.SalesOrder
(	SalesOrderId INT NOT NULL IDENTITY(1,1) CONSTRAINT PK_SalesOrder PRIMARY KEY CLUSTERED,
	ProductDetailList INT NOT NULL CONSTRAINT FK_SalesOrder_ProductDetail FOREIGN KEY REFERENCES dbo.ProductDetail,
	CustomerId INT NOT NULL CONSTRAINT FK_SalesOrder_Customer FOREIGN KEY REFERENCES dbo.Customer,
	OrderTime DATETIME2(3) NOT NULL,
	SalesAmount DECIMAL(18,4) NOT NULL,
	TaxRate DECIMAL(6,4) NOT NULL,
	ShipTime DATETIME2(3) NULL,
	ReceivedTime DATETIME2(3) NULL);
GO

CREATE TABLE fact.SalesOrderMetrics
(	OrderDate DATE NOT NULL,
	CustomerID INT NOT NULL,
	OrderCount INT NOT NULL,
	SalesAmountTotal DECIMAL(20,4) NOT NULL,
	SalesAmountMin DECIMAL(20,4) NOT NULL,
	SalesAmountMax DECIMAL(20,4) NOT NULL,
	AverageTaxRate DECIMAL(6,4) NOT NULL,
	MinTaxRate DECIMAL(6,4) NOT NULL,
	MaxTaxRate DECIMAL(6,4) NOT NULL,
	AverageHoursFromOrdertoShip DECIMAL(6,2) NULL,
	AverageHoursFromShiptoReceive DECIMAL(6,2) NULL,
	MinimumSecondsBetweenOrders INT NULL,
	MaximumSecondsBetweenOrders INT NULL);
GO

CREATE TABLE dbo.WebAccessLog
(	LogId INT NOT NULL,
	LogTime DATETIME2(3) NOT NULL,
	LogSource VARCHAR(250) NOT NULL,
	ErrorCode BIGINT NOT NULL);
GO

CREATE TABLE dbo.LogSource
(	LogSourceId SMALLINT NOT NULL IDENTITY(1,1) CONSTRAINT PK_LogSource PRIMARY KEY CLUSTERED,
	LogSource VARCHAR(250) NOT NULL);
	
CREATE TABLE dbo.WebAccessLog
(	LogId INT NOT NULL,
	LogTime DATETIME2(3) NOT NULL,
	LogSourceId SMALLINT NOT NULL,
	ErrorCode BIGINT NOT NULL);
