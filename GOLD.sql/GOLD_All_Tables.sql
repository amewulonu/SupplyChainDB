-- DIM_PRODUCT
CREATE TABLE GOLD.dimProduct (
    productKey INT IDENTITY(1,1) PRIMARY KEY,
    partNumber VARCHAR(50) NOT NULL UNIQUE,
    productType VARCHAR(20) NOT NULL,
    categoryCode VARCHAR(50) NOT NULL,
    brandCode VARCHAR(50) NOT NULL,
    familyCode VARCHAR(50) NOT NULL,
    lineCode VARCHAR(50) NOT NULL,
    productSegmentCode VARCHAR(50) NOT NULL,
    status VARCHAR(20) NOT NULL,
    value DECIMAL(10,2) NOT NULL,
    valueCurrency VARCHAR(10) NOT NULL
);

GO
-- DIM_ORGANIZATION
CREATE TABLE GOLD.dimOrganization (
    organizationKey INT IDENTITY(1,1) PRIMARY KEY,
    organizationIdentifier VARCHAR(50) NOT NULL UNIQUE,
    orgType VARCHAR(50) NOT NULL,
    name VARCHAR(100) NOT NULL,
    division VARCHAR(50) NULL
);

GO
-- Task No3 Dimension Tables Creation (GOLD Layer)
-- DIM_LOCATION
CREATE TABLE GOLD.dimLocation (
    locationKey INT IDENTITY(1,1) PRIMARY KEY,
    locationIdentifier VARCHAR(50) NOT NULL,
    locationType VARCHAR(20) NOT NULL,
    locationName VARCHAR(100) NOT NULL,
    address1 VARCHAR(50) NOT NULL,
    address2 VARCHAR(50) NULL,
    city VARCHAR(50) NOT NULL,
    postalCode VARCHAR(10) NOT NULL,
    stateProvince VARCHAR(50) NULL,
    country VARCHAR(20) NOT NULL,
    coordinates VARCHAR(50) NULL,
    includeInCorrelation BIT NULL,
    geo VARCHAR(50) NULL
);

GO
-- FACT_SUPPLY_PLAN
CREATE TABLE GOLD.factSupplyPlan (
    supplyPlanKey INT IDENTITY(1,1) PRIMARY KEY,
    productKey INT NOT NULL,
    locationKey INT NOT NULL,
    startDateKey INT NOT NULL,
    duration INT,
    planParentType VARCHAR(50),
    planType VARCHAR(50),
    quantity INT,
    quantityUnits VARCHAR(10),
    planningCycle INT,
    source VARCHAR(50),
    
    FOREIGN KEY (productKey) REFERENCES GOLD.DimProduct(productKey),
    FOREIGN KEY (locationKey) REFERENCES GOLD.DimLocation(locationKey),
    FOREIGN KEY (startDateKey) REFERENCES GOLD.DimDate(dateKey)
);

GO
 -- Fact Tables Creation (GOLD Layer)
 -- FACT_SHIPMENT
CREATE TABLE GOLD.factShipment (
    shipmentIdentifier VARCHAR(50) PRIMARY KEY,
    shipmentType VARCHAR(50),
    shipFromLocationKey INT NOT NULL,
    shipToLocationKey INT NOT NULL,
    vendorKey INT NOT NULL,
    buyerKey INT NOT NULL,
    carrierKey INT NOT NULL,
    status VARCHAR(50),
    dateCreatedKey INT NOT NULL,
    requestedArrivalKey INT NULL,
    committedArrivalKey INT NULL,
    actualShipDateKey INT NULL,
    estimatedArrivalKey INT NULL,
    revisedArrivalKey INT NULL,
    predictedArrivalKey INT NULL,
    actualArrivalKey INT NULL,
    lineCount INT,
    weight DECIMAL(10,2),
    weightUnits VARCHAR(20),
    
    CONSTRAINT FK_ShipFromLoc 
        FOREIGN KEY (shipFromLocationKey) REFERENCES GOLD.DimLocation(locationKey),
    CONSTRAINT FK_ShipToLoc 
        FOREIGN KEY (shipToLocationKey) REFERENCES GOLD.DimLocation(locationKey),
    CONSTRAINT FK_VendorOrg 
        FOREIGN KEY (vendorKey) REFERENCES GOLD.DimOrganization(organizationKey),
    CONSTRAINT FK_BuyerOrg 
        FOREIGN KEY (buyerKey) REFERENCES GOLD.DimOrganization(organizationKey),
    CONSTRAINT FK_CarrierOrg 
        FOREIGN KEY (carrierKey) REFERENCES GOLD.DimOrganization(organizationKey),
    CONSTRAINT FK_DateCreated 
        FOREIGN KEY (dateCreatedKey) REFERENCES GOLD.DimDate(dateKey),
    CONSTRAINT FK_RequestedArrival 
        FOREIGN KEY (requestedArrivalKey) REFERENCES GOLD.DimDate(dateKey),
    -- Repeat similar FKs for other date fields
);

GO
-- DIM_DATE (Enhanced from existing)
CREATE TABLE GOLD.dimDate (
    dateKey INT IDENTITY(1,1) PRIMARY KEY,
    date DATE NOT NULL UNIQUE,
    year INT NOT NULL,
    quarter INT NOT NULL,
    month INT NOT NULL,
    monthName VARCHAR(10) NOT NULL,
    day INT NOT NULL,
    dayOfWeek INT NOT NULL,
    dayOfWeekName VARCHAR(20) NOT NULL,
    isWeekend BIT NOT NULL,
    isHoliday BIT DEFAULT 0,
    fiscalYear INT NULL,
    fiscalQuarter INT NULL
);

GO
 -- Fact Tables Creation (GOLD Layer)
 -- FACT_SHIPMENT
CREATE TABLE GOLD.factShipment (
    shipmentIdentifier VARCHAR(50) PRIMARY KEY,
    shipmentType VARCHAR(50),
    shipFromLocationKey INT NOT NULL,
    shipToLocationKey INT NOT NULL,
    vendorKey INT NOT NULL,
    buyerKey INT NOT NULL,
    carrierKey INT NOT NULL,
    status VARCHAR(50),
    dateCreatedKey INT NOT NULL,
    requestedArrivalKey INT NULL,
    committedArrivalKey INT NULL,
    actualShipDateKey INT NULL,
    estimatedArrivalKey INT NULL,
    revisedArrivalKey INT NULL,
    predictedArrivalKey INT NULL,
    actualArrivalKey INT NULL,
    lineCount INT,
    weight DECIMAL(10,2),
    weightUnits VARCHAR(20),
    
    CONSTRAINT FK_ShipFromLoc 
        FOREIGN KEY (shipFromLocationKey) REFERENCES GOLD.DimLocation(locationKey),
    CONSTRAINT FK_ShipToLoc 
        FOREIGN KEY (shipToLocationKey) REFERENCES GOLD.DimLocation(locationKey),
    CONSTRAINT FK_VendorOrg 
        FOREIGN KEY (vendorKey) REFERENCES GOLD.DimOrganization(organizationKey),
    CONSTRAINT FK_BuyerOrg 
        FOREIGN KEY (buyerKey) REFERENCES GOLD.DimOrganization(organizationKey),
    CONSTRAINT FK_CarrierOrg 
        FOREIGN KEY (carrierKey) REFERENCES GOLD.DimOrganization(organizationKey),
    CONSTRAINT FK_DateCreated 
        FOREIGN KEY (dateCreatedKey) REFERENCES GOLD.DimDate(dateKey),
    CONSTRAINT FK_RequestedArrival 
        FOREIGN KEY (requestedArrivalKey) REFERENCES GOLD.DimDate(dateKey),
    -- Repeat similar FKs for other date fields
);

GO
-- FACT_SUPPLY_PLAN
CREATE TABLE GOLD.factSupplyPlan (
    supplyPlanKey INT IDENTITY(1,1) PRIMARY KEY,
    productKey INT NOT NULL,
    locationKey INT NOT NULL,
    startDateKey INT NOT NULL,
    duration INT,
    planParentType VARCHAR(50),
    planType VARCHAR(50),
    quantity INT,
    quantityUnits VARCHAR(10),
    planningCycle INT,
    source VARCHAR(50),
    
    FOREIGN KEY (productKey) REFERENCES GOLD.DimProduct(productKey),
    FOREIGN KEY (locationKey) REFERENCES GOLD.DimLocation(locationKey),
    FOREIGN KEY (startDateKey) REFERENCES GOLD.DimDate(dateKey)
);

GO
-- ETL SQL to Populate Star Schema
-- Complete ETL implementation for all date dimensions in the FactShipment table, with optimized SQL and error handling):
-- POPULATE DIMENSIONS
INSERT INTO GOLD.dimLocation (
    locationIdentifier, locationType, locationName, 
    address1, address2, city, postalCode, 
    stateProvince, country, coordinates, 
    includeInCorrelation, geo
)
SELECT 
    locationIdentifier, locationType, locationName,
    address1, address2, city, postalCode,
    stateProvince, country, coordinates,
    includeInCorrelation, geo
FROM SILVER.location_v3;

GO
INSERT INTO GOLD.dimProduct (
    partNumber, productType, categoryCode, 
    brandCode, familyCode, lineCode, 
    productSegmentCode, status, value, valueCurrency
)
SELECT 
    partNumber, productType, categoryCode,
    brandCode, familyCode, lineCode,
    productSegmentCode, status, value, valueCurrency
FROM SILVER.product_v6;

GO
INSERT INTO GOLD.dimOrganization (
    organizationIdentifier, orgType, name, division
)
SELECT 
    organizationIdentifier, orgType, name, division
FROM SILVER.organisation_v3;

GO
-- POPULATE FACT_SHIPMENT
-- POPULATE FACT_SHIPMENT WITH ALL DATE DIMENSIONS
INSERT INTO GOLD.factShipment (
    shipmentIdentifier, shipmentType, 
    shipFromLocationKey, shipToLocationKey,
    vendorKey, buyerKey, carrierKey,
    status, dateCreatedKey, requestedArrivalKey,
    committedArrivalKey, actualShipDateKey,
    estimatedArrivalKey, revisedArrivalKey,
    predictedArrivalKey, actualArrivalKey,
    lineCount, weight, weightUnits
)
SELECT
    s.shipmentIdentifier,
    s.shipmentType,
    shipFromLoc.locationKey,
    shipToLoc.locationKey,
    vendorOrg.organizationKey,
    buyerOrg.organizationKey,
    carrierOrg.organizationKey,
    s.status,
    COALESCE(dc.dateKey, -1) AS dateCreatedKey,           -- -1 for unknown dates
    COALESCE(drta.dateKey, -1) AS requestedArrivalKey,
    COALESCE(dcta.dateKey, -1) AS committedArrivalKey,
    COALESCE(dasd.dateKey, -1) AS actualShipDateKey,
    COALESCE(deta.dateKey, -1) AS estimatedArrivalKey,
    COALESCE(dreta.dateKey, -1) AS revisedArrivalKey,
    COALESCE(dpta.dateKey, -1) AS predictedArrivalKey,
    COALESCE(data.dateKey, -1) AS actualArrivalKey,
    s.lineCount,
    s.weight,
    s.weightUnits
FROM SILVER.shipment_v4 s
-- Location dimensions
JOIN GOLD.dimLocation shipFromLoc 
    ON s.shipFromLocationID = shipFromLoc.locationKey
JOIN GOLD.DimLocation shipToLoc 
    ON s.shipToLocationID = shipToLoc.locationKey
-- Organization dimensions
JOIN GOLD.dimOrganization vendorOrg 
    ON s.vendorOrganisationID = vendorOrg.organizationKey
JOIN GOLD.dimOrganization buyerOrg 
    ON s.buyerOrganisationID = buyerOrg.organizationKey
JOIN GOLD.dimOrganization carrierOrg 
    ON s.carrierOrganisationID = carrierOrg.organizationKey
-- Date dimensions (using OUTER APPLY for better performance)
OUTER APPLY (
    SELECT TOP 1 dateKey 
    FROM GOLD.DimDate 
    WHERE date = CAST(s.dateCreated AS DATE)
) dc
OUTER APPLY (
    SELECT TOP 1 dateKey 
    FROM GOLD.DimDate 
    WHERE date = CAST(s.requestedTimeOfArrival AS DATE)
) drta
OUTER APPLY (
    SELECT TOP 1 dateKey 
    FROM GOLD.DimDate 
    WHERE date = CAST(s.committedTimeOfArrival AS DATE)
) dcta
OUTER APPLY (
    SELECT TOP 1 dateKey 
    FROM GOLD.DimDate 
    WHERE date = CAST(s.actualShipDate AS DATE)
) dasd
OUTER APPLY (
    SELECT TOP 1 dateKey 
    FROM GOLD.DimDate 
    WHERE date = CAST(s.estimatedTimeOfArrival AS DATE)
) deta
OUTER APPLY (
    SELECT TOP 1 dateKey 
    FROM GOLD.DimDate 
    WHERE date = CAST(s.revisedEstimatedTimeOfArrival AS DATE)
) dreta
OUTER APPLY (
    SELECT TOP 1 dateKey 
    FROM GOLD.DimDate 
    WHERE date = CAST(s.predictedTimeOfArrival AS DATE)
) dpta
OUTER APPLY (
    SELECT TOP 1 dateKey 
    FROM GOLD.DimDate 
    WHERE date = CAST(s.actualTimeOfArrival AS DATE)
) data
-- Handle cases where date fields might be NULL
WHERE s.dateCreated IS NOT NULL;  -- Essential date field


GO
-- POPULATE FACT_SUPPLY_PLAN
INSERT INTO GOLD.factSupplyPlan (
    productKey, locationKey, startDateKey,
    duration, planParentType, planType,
    quantity, quantityUnits, planningCycle, source
)
SELECT
    p.productKey,
    l.locationKey,
    dd.dateKey,
    sp.duration,
    sp.planParentType,
    sp.planType,
    sp.quantity,
    sp.quantityUnits,
    sp.planningCycle,
    sp.source
FROM SILVER.supplyPlan_v2 sp
JOIN GOLD.DimProduct p 
    ON sp.productID = p.productKey
JOIN GOLD.DimLocation l 
    ON sp.locationID = l.locationKey
JOIN GOLD.DimDate dd 
    ON sp.startDate = dd.date;

GO
-- Optimizations for BI Performance
-- Indexing Strategy:
-- Fact Shipment
CREATE INDEX idx_fact_shipment_dates ON GOLD.FactShipment (
    dateCreatedKey, actualShipDateKey, actualArrivalKey
);

GO
CREATE INDEX idx_fact_shipment_locations ON GOLD.FactShipment (
    shipFromLocationKey, shipToLocationKey
);

GO
-- Fact SupplyPlan
CREATE INDEX idx_fact_supply_date ON GOLD.FactSupplyPlan (startDateKey);

GO
CREATE INDEX idx_fact_supply_product ON GOLD.FactSupplyPlan (productKey);

GO
-- Dimensions
CREATE INDEX idx_dim_date_date ON GOLD.DimDate (date);

GO
CREATE INDEX idx_dim_product_part ON GOLD.DimProduct (partNumber);


GO
-- Partitioning:
-- Partition FactShipment by dateCreatedKey
CREATE PARTITION FUNCTION pf_shipment_date (INT)
AS RANGE LEFT FOR VALUES (20230101, 20230601, 20231231);

GO
CREATE PARTITION SCHEME ps_shipment_date
AS PARTITION pf_shipment_date
ALL TO ([PRIMARY]);

GO
-- Materialized Views for Aggregates:
CREATE MATERIALIZED VIEW GOLD.mv_shipment_weekly
WITH (DISTRIBUTION = HASH(shipFromLocationKey)) 
AS
SELECT 
    dd.year,
    dd.week,
    fl.locationKey AS shipFromKey,
    tl.locationKey AS shipToKey,
    COUNT(*) AS total_shipments,
    AVG(weight) AS avg_weight
FROM GOLD.FactShipment fs
JOIN GOLD.DimDate dd 
    ON fs.dateCreatedKey = dd.dateKey
JOIN GOLD.DimLocation fl 
    ON fs.shipFromLocationKey = fl.locationKey
JOIN GOLD.DimLocation tl 
    ON fs.shipToLocationKey = tl.locationKey
GROUP BY dd.year, dd.week, fl.locationKey, tl.locationKey;

GO
-- Complete Date Dimension Population:
-- POPULATE DIMDATE (10-year range example)
DECLARE @StartDate DATE = '2020-01-01'
DECLARE @EndDate DATE = '2030-12-31'

WHILE @StartDate <= @EndDate
BEGIN
    INSERT INTO GOLD.dimDate (
        date, year, quarter, month, monthName,
        day, dayOfWeek, dayOfWeekName, isWeekend
    )
    SELECT
        @StartDate,
        YEAR(@StartDate),
        DATEPART(QUARTER, @StartDate),
        MONTH(@StartDate),
        DATENAME(MONTH, @StartDate),
        DAY(@StartDate),
        DATEPART(WEEKDAY, @StartDate),
        DATENAME(WEEKDAY, @StartDate),
        CASE WHEN DATEPART(WEEKDAY, @StartDate) IN (1,7) THEN 1 ELSE 0 END
    
    SET @StartDate = DATEADD(DAY, 1, @StartDate)
END

GO
-- Slowly Changing Dimensions (SCD) Handling:
-- SCD Type 2 for DimOrganization
MERGE GOLD.dimOrganization AS target
USING SILVER.organisation_v3 AS source
ON target.organizationIdentifier = source.organizationIdentifier
WHEN MATCHED AND (
    target.orgType <> source.orgType OR
    target.name <> source.name OR
    target.division <> source.division
)
THEN UPDATE SET
    target.isCurrent = 0,
    target.validTo = GETDATE()
WHEN NOT MATCHED BY TARGET
THEN INSERT (
    organizationIdentifier, orgType, name, division, 
    validFrom, validTo, isCurrent
)
VALUES (
    source.organizationIdentifier, source.orgType, 
    source.name, source.division,
    GETDATE(), NULL, 1
);

GO
-- Task 4 4. Load Data into Star Schema
-- Load the transformed data into the tables of your star schema.
-- 1. POPULATE DIMENSION TABLES FIRST
BEGIN TRY
    BEGIN TRANSACTION;

    -- DimDate (Populate 20-year date range)
    WITH DateRange AS (
        SELECT CAST('2020-01-01' AS DATE) AS [date]
        UNION ALL
        SELECT DATEADD(DAY, 1, [date])
        FROM DateRange
        WHERE DATEADD(DAY, 1, [date]) < '2040-12-31'
    )
    INSERT INTO GOLD.dimDate (
        date, year, quarter, month, monthName,
        day, dayOfWeek, dayOfWeekName, isWeekend
    )
    SELECT
        [date],
        YEAR([date]),
        DATEPART(QUARTER, [date]),
        MONTH([date]),
        DATENAME(MONTH, [date]),
        DAY([date]),
        DATEPART(WEEKDAY, [date]),
        DATENAME(WEEKDAY, [date]),
        CASE WHEN DATEPART(WEEKDAY, [date]) IN (1,7) THEN 1 ELSE 0 END
    FROM DateRange
    OPTION (MAXRECURSION 10000);
    -- DimLocation
    INSERT INTO GOLD.DimLocation (
        locationIdentifier, locationType, locationName, 
        address1, address2, city, postalCode, 
        stateProvince, country, coordinates, 
        includeInCorrelation, geo
    )
    SELECT 
        locationIdentifier,
        locationType,
        locationName,
        address1,
        address2,
        city,
        postalCode,
        stateProvince,
        country,
        coordinates,
        includeInCorrelation,
        geo
    FROM SILVER.location_v3;

    -- DimOrganization
    INSERT INTO GOLD.dimOrganization (
        organizationIdentifier, orgType, name, division
    )
    SELECT 
        organizationIdentifier,
        orgType,
        name,
        division
    FROM SILVER.organisation_v3;

    -- DimProduct
    INSERT INTO GOLD.DimProduct (
        partNumber, productType, categoryCode, 
        brandCode, familyCode, lineCode, 
        productSegmentCode, status, value, valueCurrency
    )
    SELECT 
        partNumber,
        productType,
        categoryCode,
        brandCode,
        familyCode,
        lineCode,
        productSegmentCode,
        status,
        value,
        valueCurrency
    FROM SILVER.product_v6;

    COMMIT TRANSACTION;
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0 ROLLBACK;
    INSERT INTO GOLD.ETL_ErrorLog (TableName, ErrorMessage, ErrorDateTime)
    VALUES ('Dimension Load', ERROR_MESSAGE(), GETDATE());
    THROW;
END CATCH

GO
-- 2. POPULATE FACT TABLES WITH OPTIMIZED LOOKUPS
BEGIN TRY
    BEGIN TRANSACTION;

    -- Temporary staging table for Shipment
    SELECT 
        s.*,
        dc.dateKey AS dateCreatedKey,
        drta.dateKey AS requestedArrivalKey,
        dcta.dateKey AS committedArrivalKey,
        dasd.dateKey AS actualShipDateKey,
        deta.dateKey AS estimatedArrivalKey,
        dreta.dateKey AS revisedArrivalKey,
        dpta.dateKey AS predictedArrivalKey,
        data.dateKey AS actualArrivalKey
    INTO #StagingShipments
    FROM SILVER.shipment_v4 s
    LEFT JOIN GOLD.DimDate dc ON CONVERT(DATE, s.dateCreated) = dc.date
    LEFT JOIN GOLD.DimDate drta ON CONVERT(DATE, s.requestedTimeOfArrival) = drta.date
    LEFT JOIN GOLD.DimDate dcta ON CONVERT(DATE, s.committedTimeOfArrival) = dcta.date
    LEFT JOIN GOLD.DimDate dasd ON CONVERT(DATE, s.actualShipDate) = dasd.date
    LEFT JOIN GOLD.DimDate deta ON CONVERT(DATE, s.estimatedTimeOfArrival) = deta.date
    LEFT JOIN GOLD.DimDate dreta ON CONVERT(DATE, s.revisedEstimatedTimeOfArrival) = dreta.date
    LEFT JOIN GOLD.DimDate dpta ON CONVERT(DATE, s.predictedTimeOfArrival) = dpta.date
    LEFT JOIN GOLD.DimDate data ON CONVERT(DATE, s.actualTimeOfArrival) = data.date;

    -- Load FactShipment
    INSERT INTO GOLD.FactShipment (
        shipmentIdentifier, shipmentType, 
        shipFromLocationKey, shipToLocationKey,
        vendorKey, buyerKey, carrierKey,
        status, dateCreatedKey, requestedArrivalKey,
        committedArrivalKey, actualShipDateKey,
        estimatedArrivalKey, revisedArrivalKey,
        predictedArrivalKey, actualArrivalKey,
        lineCount, weight, weightUnits
    )
    SELECT
        s.shipmentIdentifier,
        s.shipmentType,
        ISNULL(sf.locationKey, -1),
        ISNULL(st.locationKey, -1),
        ISNULL(vo.organizationKey, -1),
        ISNULL(bo.organizationKey, -1),
        ISNULL(co.organizationKey, -1),
        s.status,
        ISNULL(ss.dateCreatedKey, -1),
        ISNULL(ss.requestedArrivalKey, -1),
        ISNULL(ss.committedArrivalKey, -1),
        ISNULL(ss.actualShipDateKey, -1),
        ISNULL(ss.estimatedArrivalKey, -1),
        ISNULL(ss.revisedArrivalKey, -1),
        ISNULL(ss.predictedArrivalKey, -1),
        ISNULL(ss.actualArrivalKey, -1),
        s.lineCount,
        s.weight,
        s.weightUnits
    FROM SILVER.shipment_v4 s
    JOIN #StagingShipments ss ON s.shipmentIdentifier = ss.shipmentIdentifier
    LEFT JOIN GOLD.DimLocation sf ON s.shipFromLocationID = sf.locationKey
    LEFT JOIN GOLD.DimLocation st ON s.shipToLocationID = st.locationKey
    LEFT JOIN GOLD.DimOrganization vo ON s.vendorOrganisationID = vo.organizationKey
    LEFT JOIN GOLD.DimOrganization bo ON s.buyerOrganisationID = bo.organizationKey
    LEFT JOIN GOLD.DimOrganization co ON s.carrierOrganisationID = co.organizationKey;

    -- Load FactSupplyPlan
    INSERT INTO GOLD.FactSupplyPlan (
        productKey, locationKey, startDateKey,
        duration, planParentType, planType,
        quantity, quantityUnits, planningCycle, source
    )
    SELECT
        ISNULL(p.productKey, -1),
        ISNULL(l.locationKey, -1),
        ISNULL(d.dateKey, -1),
        sp.duration,
        sp.planParentType,
        sp.planType,
        sp.quantity,
        sp.quantityUnits,
        sp.planningCycle,
        sp.source
    FROM SILVER.supplyPlan_v2 sp
    LEFT JOIN GOLD.DimProduct p ON sp.productID = p.productKey
    LEFT JOIN GOLD.DimLocation l ON sp.locationID = l.locationKey
    LEFT JOIN GOLD.DimDate d ON sp.startDate = d.date;

    DROP TABLE #StagingShipments;
    COMMIT TRANSACTION;
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0 ROLLBACK;
    INSERT INTO GOLD.ETL_ErrorLog (TableName, ErrorMessage, ErrorDateTime)
    VALUES ('Fact Load', ERROR_MESSAGE(), GETDATE());
    THROW;
END CATCH

GO
-- 3. POST-LOAD INDEXING & OPTIMIZATION
BEGIN
    -- Create indexes AFTER data loading
    CREATE INDEX IX_FactShipment_DateCreated ON GOLD.FactShipment(dateCreatedKey);
    CREATE INDEX IX_FactShipment_Vendor ON GOLD.FactShipment(vendorKey);
    CREATE INDEX IX_FactSupplyPlan_StartDate ON GOLD.FactSupplyPlan(startDateKey);
    
    -- Update statistics
    EXEC sp_updatestats;
END

GO
-- Post-Load Validation Queries:
-- Check for missing references
SELECT 'Shipment' AS TableName, COUNT(*) AS MissingOrgs
FROM GOLD.FactShipment
WHERE vendorKey = -1 OR buyerKey = -1 OR carrierKey = -1;

SELECT 'SupplyPlan' AS TableName, COUNT(*) AS MissingProducts
FROM GOLD.FactSupplyPlan
WHERE productKey = -1;

-- Validate record counts
SELECT 
    (SELECT COUNT(*) FROM SILVER.shipment_v4) AS SilverShipments,
    (SELECT COUNT(*) FROM GOLD.FactShipment) AS GoldShipments,
    (SELECT COUNT(*) FROM SILVER.supplyPlan_v2) AS SilverSupplyPlans,
    (SELECT COUNT(*) FROM GOLD.FactSupplyPlan) AS GoldSupplyPlans;

-- GO Incremental Loading (for daily updates):
-- For FactShipment
INSERT INTO GOLD.FactShipment (...)
SELECT ...
FROM SILVER.shipment_v4 s
WHERE s.lastUpdatedAt > @lastLoadDate

GO
-- Partitioning Strategy:
-- Monthly partitioning for shipment fact
CREATE PARTITION FUNCTION pf_monthly (DATE)
AS RANGE RIGHT FOR VALUES ('2023-01-01', '2023-02-01', ...);

GO
-- Columnstore Indexes (for large tables):
CREATE CLUSTERED COLUMNSTORE INDEX CCI_FactShipment 
ON GOLD.FactShipment;
