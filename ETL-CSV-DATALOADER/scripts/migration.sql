IF DB_ID('ETL_DataLoaderDB') IS NOT NULL
BEGIN
    PRINT 'DROPPING ETL_DataLoaderDB...';
    ALTER DATABASE ETL_DataLoaderDB SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE ETL_DataLoaderDB;
END
GO

CREATE DATABASE ETL_DataLoaderDB;
GO

USE ETL_DataLoaderDB;
GO

SET QUOTED_IDENTIFIER ON;
SET ANSI_NULLS ON;
GO
  
SET QUOTED_IDENTIFIER ON;
SET ANSI_NULLS ON;
GO

IF OBJECT_ID('dbo.TaxiTrips', 'U') IS NOT NULL
BEGIN
    DROP TABLE dbo.TaxiTrips;
END
GO

CREATE TABLE dbo.TaxiTrips (
    TpepPickupDatetime DATETIME2 NOT NULL,
    TpepDropoffDatetime DATETIME2 NOT NULL,
    PassengerCount TINYINT NOT NULL,
    TripDistance DECIMAL(5,2) NOT NULL,
    StoreAndFwdFlag CHAR(3) NOT NULL, 
    PULocationID INT NOT NULL,
    DOLocationID INT NOT NULL,
    FareAmount DECIMAL(10,2) NOT NULL,
    TipAmount DECIMAL(10,2) NOT NULL,
    TripDurationMinutes AS DATEDIFF(MINUTE, TpepPickupDatetime, TpepDropoffDatetime) PERSISTED
);
GO

CREATE INDEX IDX_PULocationID_TipAmount ON dbo.TaxiTrips (PULocationID, TipAmount);
GO

CREATE INDEX IDX_TripDistance ON dbo.TaxiTrips (TripDistance);
GO

CREATE INDEX IDX_TripDurationMinutes ON dbo.TaxiTrips (TripDurationMinutes);
GO
