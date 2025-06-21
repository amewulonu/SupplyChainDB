CREATE DATABASE SupplyChainDB;

GO
USE SupplyChainDB;

GO
CREATE SCHEMA BRONZE;

GO
CREATE SCHEMA SILVER;

GO
CREATE SCHEMA GOLD;

GO 
-- Create table  for product_v6
CREATE TABLE BRONZE.product_v6 (
partNumber VARCHAR(20),
productType VARCHAR(20),
categoryCode VARCHAR(50),
brandCode VARCHAR(20),
familyCode VARCHAR(50),
lineCode VARCHAR(20),
productSegmentCode VARCHAR(20),
status VARCHAR(10),
value DECIMAL (10,2),
valueCurrency VARCHAR(10),
defaultQuantityUnits VARCHAR(10),
name VARCHAR(200),
description TEXT,
plannerCode VARCHAR(20),
sourceLink VARCHAR(300)
);
GO
-- Load CSV file for product_v6 (Raw)
BULK INSERT BRONZE.product_v6
FROM 'C:\Data_Tech\Portfolio projects\Mentor_proj_SSME_SQL\proj2\product_v6.csv'
WITH (
FORMAT = 'csv',
FIRSTROW = 2,
FIELDTERMINATOR = ',',
ROWTERMINATOR = '\n',
KEEPNULLS,
TABLOCK
);

GO
-- Create table for organisation_v3 in Bronze schema to inject Raw data  
CREATE TABLE BRONZE.organisation_v3 (
organisationIdentifier VARCHAR(20),
orgType VARCHAR(20),
locationIdentifier VARCHAR(50),
name VARCHAR(100),
division VARCHAR(20),
sourceLink VARCHAR(300)
);
GO
-- Load CSV file for organisation_v3 (Raw) into the created table in bronze schema
BULK INSERT BRONZE.organisation_v3
FROM 'C:\Data_Tech\Portfolio projects\Mentor_proj_SSME_SQL\proj2\organization_v3.csv'
WITH (
FORMAT = 'csv',
FIRSTROW = 2,
FIELDTERMINATOR = ',',
ROWTERMINATOR = '\n',
KEEPNULLS,
TABLOCK
);

GO
--Create table for location_v3 in Bronze schema to inject Raw data 
CREATE TABLE BRONZE.location_v3 (
locationIdentifier VARCHAR(50),
locationType VARCHAR(20),
locationName VARCHAR(100),
address1 VARCHAR(50),
address2 VARCHAR(50),
city VARCHAR(50),
postalCode VARCHAR(10),
stateProvince VARCHAR(10),
country VARCHAR(20),
coordinates VARCHAR(50),
includeInCorrelation VARCHAR(10), -- CAST will be used to case TRUE/FALSE, mapped to 1/0
geo VARCHAR(50),
sourceLink VARCHAR(300)
);
GO

-- Load CSV file for location_v3 (Raw) into the created table in bronze schema
BULK INSERT BRONZE.location_v3
FROM 'C:\Data_Tech\Portfolio projects\Mentor_proj_SSME_SQL\proj2\location_v3.csv'
WITH (
FORMAT = 'csv',
FIRSTROW = 2,
FIELDTERMINATOR = ',',
ROWTERMINATOR = '\n',
KEEPNULLS,
TABLOCK
);
GO
-- CAST, add a BIT column for clean data, update the bit column based on real column values
GO
-- Add this BEFORE the UPDATE:
ALTER TABLE BRONZE.location_v3
ADD includeInCorrelation_bit BIT;

GO
UPDATE BRONZE.location_v3
SET includeInCorrelation_bit = CASE 
    WHEN LOWER(includeInCorrelation) = 'true' THEN 1
    WHEN LOWER(includeInCorrelation) = 'false' THEN 0
    ELSE NULL
END;


GO
-- Create table for inventory_v2 in Bronze schema to inject Raw data
CREATE TABLE BRONZE.inventory_v2 (
productPartNumber VARCHAR(20),
locationIdentifier VARCHAR (10),
inventoryType VARCHAR(20),
quantity INT,
quantityUnits VARCHAR(10),
value DECIMAL(9,2),
valueCurrency VARCHAR(10),
researvationOrders INT,
daysOfSupply INT,
shelfLife INT,
reorderLevel INT,
expectedLeadTime INT,
quantityUpperThreshold INT,
quantityLowerThreshold INT,
daysOfSupplyUpperThreshold INT,
daysOfSupplyLowerThreshold INT,
expiringThreshold INT,
plannerCode VARCHAR(50),
velocityCode VARCHAR(10),
inventoryParentType VARCHAR(20),
class VARCHAR(10),
segment VARCHAR(20)
);
GO
-- Load CSV file for inventory_v2 (Raw) into the created table in bronze schema
BULK INSERT BRONZE.inventory_v2
FROM 'C:\Data_Tech\Portfolio projects\Mentor_proj_SSME_SQL\proj2\inventory_v2.csv'
WITH (
FORMAT = 'CSV',
FIRSTROW = 2,
FIELDTERMINATOR = ',',
ROWTERMINATOR = '\n',
TABLOCK
);

GO
-- Create table for supplyPlan_v2 in Bronze schema to inject Raw data
CREATE TABLE BRONZE.supplyPlan_v2 (
productPartNumber VARCHAR(20),
locationIdentifier VARCHAR(10),
startDate DATE,
duration INT,
planParentType VARCHAR(20),
planType VARCHAR(20),
quantity INT,
quantityUnits VARCHAR(10),
planningCycle INT,
source VARCHAR(10),
sourceLink VARCHAR(300)
);
GO
-- Load CSV file for supplyPlan_v2 (Raw) into the created table in bronze schema
BULK INSERT BRONZE.supplyPlan_v2 
FROM 'C:\Data_Tech\Portfolio projects\Mentor_proj_SSME_SQL\proj2\supplyPlan_v2.csv'
WITH (
FORMAT = 'csv',
FIRSTROW = 2,
FIELDTERMINATOR = ',',
ROWTERMINATOR = '\n',
KEEPNULLS,
TABLOCK
);

GO
-- Create table for order_v3 in Bronze schema to inject Raw data
CREATE TABLE BRONZE.order_v3 (
orderIdentifier INT,
orderType VARCHAR(20),
vendorOrganisationIdentifier VARCHAR(50),
buyerOrganisationIdentifier VARCHAR(50),
shipFromInstructionLocationIdentifier VARCHAR(50),
shipToLocationIdentifier VARCHAR(100),
orderStatus VARCHAR(20),
createdDate DATETIME,
requestedShipDate DATETIME,
requestedDeliveryDate DATETIME,
plannedShipDate DATETIME,
plannedDeliveryDate DATETIME,
quantity INT,
quantityUnits VARCHAR(10),
totalValue DECIMAL(14,2),
orderValueCurrency VARCHAR(10),
lineCount INT,
totalShippedQuantity INT,
exclude BIT,
sourceLink VARCHAR(300)
);
GO
-- Load CSV file for Order_v3 (Raw) into the created table in bronze schema
BULK INSERT BRONZE.order_v3
FROM 'C:\Data_Tech\Portfolio projects\Mentor_proj_SSME_SQL\proj2\order_v3.csv'
WITH (
FORMAT = 'csv',
FIRSTROW = 2,
FIELDTERMINATOR = ',',
ROWTERMINATOR = '\n',
KEEPNULLS,
TABLOCK
);

GO
-- Create table for shipment_v4 in Bronze schema to inject Raw data
CREATE TABLE BRONZE.shipment_v4 (
shipmentIdentifier VARCHAR(50),
shipmentType VARCHAR(20),
shipFromLocationIdentifier VARCHAR(50),
shipToLocationIdentifier VARCHAR(50),
vendorOrganizationIdentifier VARCHAR(20),
buyerOrganizationIdentifier VARCHAR(20),
carrierOrganizationIdentifier VARCHAR(20),
status VARCHAR(10),
dateCreated DATETIME,
requestedTimeOfArrival DATETIME,
committedTimeOfArrival DATETIME,
actualShipDate DATETIME,
estimatedTimeOfArrival DATETIME,
revisedEstimatedTimeOfArrival DATETIME,
predictedTimeOfArrival DATETIME,
actualTimeOfArrival DATETIME,
lineCount INT NULL,
weight DECIMAL(8,2) NULL,
weightUnits VARCHAR(20),
currentLocationCoordinates VARCHAR(50),
currentRegion VARCHAR(20),
transportMode VARCHAR(50),
houseAirwayBill VARCHAR(50),
parcelTrackingNumber VARCHAR(50),
airwayMasterNumber VARCHAR(50),
billOfLadingNumber VARCHAR(50),
proNumber VARCHAR(50),
manifest	VARCHAR(20),
exclude INT NULL,
sourcelink VARCHAR(300)
);
GO
-- Load CSV file for Shipment_v4 (Raw) into the created table in bronze schema
BULK INSERT BRONZE.shipment_v4
FROM 'C:\Data_Tech\Portfolio projects\Mentor_proj_SSME_SQL\proj2\shipment_v4.csv'
WITH (
FORMAT = 'csv',
FIRSTROW = 2,
FIELDTERMINATOR = ',',
ROWTERMINATOR = '\n',
KEEPNULLS,
TABLOCK
);

GO
-- viewed the tables to ensure the data was well loaded
SELECT * FROM BRONZE.order_v3; -- did same for all tables imported

GO

SELECT 
    s.name AS SchemaName,          -- Selects the schema name from sys.schemas, renaming it as SchemaName
    t.name AS TableName            -- Selects the table name from sys.tables, renaming it as TableName
FROM sys.tables t                 -- From the system view sys.tables (alias t), which contains all user tables
JOIN sys.schemas s                -- Join with the system view sys.schemas (alias s), which contains all schemas
    ON t.schema_id = s.schema_id  -- Join condition: match tables to their schemas by schema_id
ORDER BY s.name, t.name;          -- Sort the results first by schema name, then by table name alphabetically


GO




