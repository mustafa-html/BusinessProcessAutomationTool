
-- Create database (run as admin, optional if DB already exists)
-- CREATE DATABASE BusinessAutomationDB;
-- GO

-- Use database
-- USE BusinessAutomationDB;
-- GO

IF OBJECT_ID('dbo.Customers', 'U') IS NOT NULL DROP TABLE dbo.Customers;
IF OBJECT_ID('dbo.Transactions', 'U') IS NOT NULL DROP TABLE dbo.Transactions;
IF OBJECT_ID('dbo.CleanData', 'U') IS NOT NULL DROP TABLE dbo.CleanData;

CREATE TABLE dbo.Customers (
    CustomerId NVARCHAR(20) PRIMARY KEY,
    Name NVARCHAR(100) NOT NULL,
    Email NVARCHAR(255) NULL
);

CREATE TABLE dbo.Transactions (
    TransactionId NVARCHAR(20) PRIMARY KEY,
    CustomerId NVARCHAR(20) NOT NULL,
    Amount DECIMAL(18,2) NOT NULL CHECK (Amount >= 0),
    CreatedAt DATETIME2 NOT NULL,
    FOREIGN KEY (CustomerId) REFERENCES dbo.Customers(CustomerId)
);

CREATE TABLE dbo.CleanData (
    Id INT IDENTITY(1,1) PRIMARY KEY,
    CustomerId NVARCHAR(20) NOT NULL,
    Name NVARCHAR(100) NOT NULL,
    Email NVARCHAR(255) NULL,
    Amount DECIMAL(18,2) NOT NULL,
    CreatedAt DATETIME2 NOT NULL,
    Source NVARCHAR(50) NOT NULL DEFAULT 'ETL'
);

CREATE INDEX IX_CleanData_CreatedAt ON dbo.CleanData(CreatedAt);
