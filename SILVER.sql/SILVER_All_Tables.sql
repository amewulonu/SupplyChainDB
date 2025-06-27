USE  SupplyChainDB;
-- ===============================================================
-- Data Cleaning and Transformation for SILVER.product_v6
-- ===============================================================
-- Step 1: Create the SILVER.product_v6 table with surrogate key and cleaned structure
CREATE TABLE SILVER.product_v6 (
    productID INT IDENTITY(1,1) PRIMARY KEY NOT NULL,
    partNumber VARCHAR(50) NOT NULL UNIQUE,
    productType VARCHAR(20) NOT NULL,
    categoryCode VARCHAR(50) NOT NULL,
    brandCode VARCHAR(50) NOT NULL,
    familyCode VARCHAR(50) NOT NULL,
    lineCode VARCHAR(50) NOT NULL,
    productSegmentCode VARCHAR(50) NOT NULL,
    status VARCHAR(20) NOT NULL,
    value DECIMAL (10,2) NOT NULL,
    valueCurrency VARCHAR(10) NOT NULL,
    defaultQuantityUnits VARCHAR(10) NOT NULL,
    name VARCHAR(255) NOT NULL,
    description VARCHAR(MAX) NOT NULL,
    plannerCode VARCHAR(50) NOT NULL,
    sourceLink VARCHAR(255) NOT NULL,
    sourceSystem VARCHAR(100) NULL,         -- Optional source system tracker
    loadBatchID INT NULL,                   -- Optional batch tracking
    insertedAt DATETIME DEFAULT GETDATE(),
    lastUpdatedAt DATETIME DEFAULT GETDATE()
);
GO

-- Step 2: Insert cleaned, deduplicated data from BRONZE layer with missing value handling
WITH RankedProducts AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY partNumber ORDER BY GETDATE() DESC) AS rn
    FROM BRONZE.product_v6
)
INSERT INTO SILVER.product_v6 (
    partNumber, productType, categoryCode, brandCode, familyCode,
    lineCode, productSegmentCode, status, value, valueCurrency,
    defaultQuantityUnits, name, description, plannerCode, sourceLink,
    sourceSystem, loadBatchID
)
SELECT
    ISNULL(NULLIF(LTRIM(RTRIM(CAST(partNumber AS VARCHAR(MAX)))), ''), 'UNKNOWN'),          -- Cleaned partNumber or 'UNKNOWN'
    ISNULL(NULLIF(LTRIM(RTRIM(CAST(productType AS VARCHAR(MAX)))), ''), 'UNKNOWN'),         -- Cleaned productType or 'UNKNOWN'
    ISNULL(NULLIF(LTRIM(RTRIM(CAST(categoryCode AS VARCHAR(MAX)))), ''), 'UNKNOWN'),        -- Cleaned categoryCode or 'UNKNOWN'
    ISNULL(NULLIF(LTRIM(RTRIM(CAST(brandCode AS VARCHAR(MAX)))), ''), 'UNKNOWN'),           -- Cleaned brandCode or 'UNKNOWN'
    ISNULL(NULLIF(LTRIM(RTRIM(CAST(familyCode AS VARCHAR(MAX)))), ''), 'UNKNOWN'),          -- Cleaned familyCode or 'UNKNOWN'
    ISNULL(NULLIF(LTRIM(RTRIM(CAST(lineCode AS VARCHAR(MAX)))), ''), 'UNKNOWN'),            -- Cleaned lineCode or 'UNKNOWN'
    ISNULL(NULLIF(LTRIM(RTRIM(CAST(productSegmentCode AS VARCHAR(MAX)))), ''), 'UNKNOWN'),  -- Cleaned productSegmentCode or 'UNKNOWN'
    ISNULL(NULLIF(LTRIM(RTRIM(CAST(status AS VARCHAR(MAX)))), ''), 'UNKNOWN'),              -- Cleaned status or 'UNKNOWN'
    ISNULL(value, 0.00),                                                                    -- Numeric value or 0.00 if NULL
    ISNULL(NULLIF(LTRIM(RTRIM(CAST(valueCurrency AS VARCHAR(MAX)))), ''), 'N/A'),           -- Cleaned valueCurrency or 'N/A'
    ISNULL(NULLIF(LTRIM(RTRIM(CAST(defaultQuantityUnits AS VARCHAR(MAX)))), ''), 'N/A'),    -- Cleaned defaultQuantityUnits or 'N/A'
    ISNULL(NULLIF(LTRIM(RTRIM(CAST(name AS VARCHAR(MAX)))), ''), 'UNKNOWN'),                -- Cleaned name or 'UNKNOWN'
    ISNULL(NULLIF(LTRIM(RTRIM(CAST(description AS VARCHAR(MAX)))), ''), 'No Description'),   -- Cleaned description or 'No Description'
    ISNULL(NULLIF(LTRIM(RTRIM(CAST(plannerCode AS VARCHAR(MAX)))), ''), 'UNKNOWN'),         -- Cleaned plannerCode or 'UNKNOWN'
    ISNULL(NULLIF(LTRIM(RTRIM(CAST(sourceLink AS VARCHAR(MAX)))), ''), 'N/A'),              -- Cleaned sourceLink or 'N/A'
    'ERP System A' AS sourceSystem,                                                         -- Hardcoded source system
    1 AS loadBatchID                                                                         -- Example batch ID
FROM RankedProducts
WHERE rn = 1;

-- Step 3: Impute missing values (value = 0.00) using average per productType
UPDATE SILVER.product_v6
SET value = avgValues.avgValue
FROM SILVER.product_v6 p
JOIN (
    SELECT productType, AVG(value) AS avgValue
    FROM SILVER.product_v6
    WHERE value > 0
    GROUP BY productType
) AS avgValues
ON p.productType = avgValues.productType
WHERE p.value = 0;

-- Step 4: OPTIONAL: Enforce Foreign Key Constraint (if dimensions exist)
-- ALTER TABLE SILVER.product_v6
-- ADD CONSTRAINT FK_Product_Brand FOREIGN KEY (brandCode) REFERENCES SILVER.brand(brandCode);


GO

-- ===============================================================
-- 🧼 Data Cleaning and Transformation for SILVER.organisation_v3
-- ===============================================================
-- Step 1: Create the SILVER.organisation_v3 table
CREATE TABLE SILVER.organisation_v3 (
    organisationID INT IDENTITY(1,1) PRIMARY KEY NOT NULL,
    organisationIdentifier VARCHAR(50) NOT NULL UNIQUE,
    orgType VARCHAR(50) NOT NULL,
    locationIdentifier VARCHAR(50) NOT NULL,
    name VARCHAR(100) NOT NULL,
    division VARCHAR(50),
    sourceLink VARCHAR(255),
    sourceSystem VARCHAR(100) NULL,     -- Track source system
    loadBatchID INT NULL,               -- Track data load batch
    insertedAt DATETIME DEFAULT GETDATE(),
    lastUpdatedAt DATETIME DEFAULT GETDATE()
);
GO

-- Step 2: Insert cleaned, deduplicated organisation data from BRONZE layer
WITH RankedOrgs AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY organisationIdentifier ORDER BY GETDATE() DESC) AS rn
    FROM BRONZE.organisation_v3
    WHERE organisationIdentifier IS NOT NULL
)
INSERT INTO SILVER.organisation_v3 (
    organisationIdentifier,
    orgType,
    locationIdentifier,
    name,
    division,
    sourceLink,
    sourceSystem,
    loadBatchID
)
SELECT
    ISNULL(NULLIF(LTRIM(RTRIM(organisationIdentifier)), ''), 'UNKNOWN'),
    ISNULL(NULLIF(LTRIM(RTRIM(orgType)), ''), 'UNKNOWN'),
    ISNULL(NULLIF(LTRIM(RTRIM(locationIdentifier)), ''), 'UNKNOWN'),
    ISNULL(NULLIF(LTRIM(RTRIM(name)), ''), 'UNKNOWN'),
    ISNULL(NULLIF(LTRIM(RTRIM(division)), ''), 'N/A'),
    ISNULL(NULLIF(LTRIM(RTRIM(sourceLink)), ''), 'N/A'),
    'ERP System B' AS sourceSystem,     -- Example fixed system name
    1 AS loadBatchID                    -- Example batch ID
FROM RankedOrgs
WHERE rn = 1;


GO
-- ===============================================================
-- 🧼 Data Cleaning and Transformation for SILVER.location_v3
-- ===============================================================

-- Step 1: Create the SILVER.location_v3 table with audit & metadata columns
CREATE TABLE SILVER.location_v3 (
	locationID INT IDENTITY(1,1) PRIMARY KEY NOT NULL,                   -- Surrogate primary key
    locationIdentifier VARCHAR(50) NOT NULL UNIQUE,                     -- Unique business key, cleaned and enforced unique
    locationType VARCHAR(20) NOT NULL,                                  -- Type of location (e.g., warehouse, store)
    locationName VARCHAR(100) NOT NULL,                                 -- Name of the location
    address1 VARCHAR(50) NOT NULL,                                      -- Primary address line
    address2 VARCHAR(50) NULL,                                          -- Secondary address line (optional)
    city VARCHAR(50) NOT NULL,                                          -- City name
    postalCode VARCHAR(10) NOT NULL,                                    -- Postal code
    stateProvince VARCHAR(50) NULL,                                     -- State or province (optional)
    country VARCHAR(20) NOT NULL,                                       -- Country name
    coordinates VARCHAR(50) NULL,                                       -- Geospatial coordinates (optional)
    includeInCorrelation BIT NULL,                                      -- Flag for inclusion in correlation analyses
    geo VARCHAR(50) NULL,                                               -- Additional geographic info (optional)
    sourceLink VARCHAR(300) NOT NULL,                                   -- Source reference URL or info
    sourceSystem VARCHAR(100) NULL,                                     -- Metadata: Source system of data load
    loadBatchID INT NULL,                                               -- Metadata: Batch ID of data load
	insertedAt DATETIME DEFAULT GETDATE(),                              -- Timestamp when record was inserted
    lastUpdatedAt DATETIME DEFAULT GETDATE()                            -- Timestamp when record was last updated
);
GO

-- Step 2: Insert cleaned, deduplicated data from BRONZE.location_v3
WITH RankedLocations AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY locationIdentifier 
               ORDER BY insertedAt DESC  -- Assuming insertedAt exists in BRONZE to pick latest record per locationIdentifier
           ) AS rn
    FROM BRONZE.location_v3
    WHERE locationIdentifier IS NOT NULL  -- Filter out rows missing business key
)
INSERT INTO SILVER.location_v3 (
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
    geo,
    sourceLink,
    sourceSystem,
    loadBatchID
)
SELECT
    ISNULL(NULLIF(LTRIM(RTRIM(locationIdentifier)), ''), 'UNKNOWN'),  -- Clean & default empty to 'UNKNOWN'
    ISNULL(NULLIF(LTRIM(RTRIM(locationType)), ''), 'UNKNOWN'),        -- Clean & default empty to 'UNKNOWN'
    ISNULL(NULLIF(LTRIM(RTRIM(locationName)), ''), 'UNKNOWN'),        -- Clean & default empty to 'UNKNOWN'
    ISNULL(NULLIF(LTRIM(RTRIM(address1)), ''), 'N/A'),                -- Clean & default empty to 'N/A'
    NULLIF(LTRIM(RTRIM(address2)), ''),                               -- Trim and allow NULL if empty
    ISNULL(NULLIF(LTRIM(RTRIM(city)), ''), 'UNKNOWN'),                -- Clean & default empty to 'UNKNOWN'
    ISNULL(NULLIF(LTRIM(RTRIM(postalCode)), ''), 'N/A'),              -- Clean & default empty to 'N/A'
    NULLIF(LTRIM(RTRIM(stateProvince)), ''),                          -- Trim and allow NULL if empty
    ISNULL(NULLIF(LTRIM(RTRIM(country)), ''), 'UNKNOWN'),             -- Clean & default empty to 'UNKNOWN'
    NULLIF(LTRIM(RTRIM(coordinates)), ''),                            -- Trim and allow NULL if empty
    CASE 
    WHEN UPPER(LTRIM(RTRIM(ISNULL(includeInCorrelation, 'FALSE')))) = 'TRUE' THEN 1  -- Default NULL boolean to 0 (false)
    ELSE 0
END,
    NULLIF(LTRIM(RTRIM(geo)), ''),                                   -- Trim and allow NULL if empty
    ISNULL(NULLIF(LTRIM(RTRIM(sourceLink)), ''), 'N/A'),             -- Clean & default empty to 'N/A'
    'ERP System C' AS sourceSystem,                                   -- Static source system for this load
    1 AS loadBatchID                                                 -- Static load batch ID for this example
FROM RankedLocations
WHERE rn = 1;  -- Only insert latest record per locationIdentifier
GO

-- Step 3: Create trigger to update lastUpdatedAt on record update
CREATE TRIGGER SILVER.location_v3_UpdateTimestamp
ON SILVER.location_v3
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE loc
    SET lastUpdatedAt = GETDATE()
    FROM SILVER.location_v3 loc
    INNER JOIN inserted i ON loc.locationID = i.locationID;
END;
GO

-- Optional Step 4: Index for lookup by locationIdentifier (already UNIQUE constraint)
-- CREATE UNIQUE INDEX idx_locationIdentifier ON SILVER.location_v3(locationIdentifier);

-- Optional Step 5: Index for common filter columns (e.g., city, country) for faster queries
-- CREATE INDEX idx_city_country ON SILVER.location_v3(city, country);


GO
-- ===============================================================
-- 🧼 Data Cleaning and Transformation for SILVER.inventory_v2
-- ===============================================================

-- Step 1: Create the SILVER.inventory_v2 table
CREATE TABLE SILVER.inventory_v2 (
    inventoryID INT IDENTITY(1,1) PRIMARY KEY NOT NULL,             -- Surrogate primary key with auto-increment
    productID INT NOT NULL,                                         -- Foreign key to SILVER.product_v6(productID)
    locationID INT NOT NULL,                                        -- Foreign key to SILVER.location_v3(locationID)
    inventoryType VARCHAR(50) NULL,                                 -- Type/category of inventory (e.g., Raw Material, Finished Goods)
    quantity INT NULL,                                              -- Quantity available
    quantityUnits VARCHAR(10) NULL,                                 -- Units for quantity (e.g., pcs, kg)
    value DECIMAL(12,2) NULL,                                       -- Monetary value of inventory
    valueCurrency VARCHAR(10) NULL,                                 -- Currency code for value (e.g., USD)
    reservationOrders INT NULL,                                     -- Number of orders reserved against inventory
    daysOfSupply INT NULL,                                          -- Days of supply available
    shelfLife INT NULL,                                             -- Shelf life in days
    reorderLevel INT NULL,                                          -- Inventory reorder level threshold
    expectedLeadTime INT NULL,                                      -- Expected lead time for replenishment in days
    quantityUpperThreshold INT NULL,                               -- Upper threshold for quantity (alerts, planning)
    quantityLowerThreshold INT NULL,                               -- Lower threshold for quantity (alerts, planning)
    daysOfSupplyUpperThreshold INT NULL,                           -- Upper threshold for days of supply
    daysOfSupplyLowerThreshold INT NULL,                           -- Lower threshold for days of supply
    expiringThreshold INT NULL,                                    -- Threshold for near-expiry inventory in days
    plannerCode VARCHAR(50) NULL,                                  -- Code for inventory planner or management group
    velocityCode VARCHAR(20) NULL,                                 -- Inventory velocity classification (e.g., fast-moving)
    inventoryParentType VARCHAR(50) NULL,                          -- Parent type/category of inventory
    class VARCHAR(50) NULL,                                        -- Classification group of inventory
    segment VARCHAR(50) NULL,                                      -- Market or business segment classification
    insertedAt DATETIME DEFAULT GETDATE(),                         -- Timestamp when record was inserted
    lastUpdatedAt DATETIME DEFAULT GETDATE(),                      -- Timestamp when record was last updated
    FOREIGN KEY (productID) REFERENCES SILVER.product_v6(productID),  -- FK to products
    FOREIGN KEY (locationID) REFERENCES SILVER.location_v3(locationID) -- FK to locations
);
GO

-- Step 2: Insert cleaned, transformed data from BRONZE.inventory_v2
WITH CleanedInventory AS (
    SELECT 
        b.productPartNumber,
        b.locationIdentifier,
        TRIM(b.inventoryType) AS inventoryType,
        b.quantity,
        TRIM(b.quantityUnits) AS quantityUnits,
        b.value,
        TRIM(b.valueCurrency) AS valueCurrency,
        b.reservationOrders,               -- Fixed typo here (was researvationOrders)
        b.daysOfSupply,
        b.shelfLife,
        b.reorderLevel,
        b.expectedLeadTime,
        b.quantityUpperThreshold,
        b.quantityLowerThreshold,
        b.daysOfSupplyUpperThreshold,
        b.daysOfSupplyLowerThreshold,
        b.expiringThreshold,
        TRIM(b.plannerCode) AS plannerCode,
        TRIM(b.velocityCode) AS velocityCode,
        TRIM(b.inventoryParentType) AS inventoryParentType,
        TRIM(b.class) AS class,
        TRIM(b.segment) AS segment
    FROM BRONZE.inventory_v2 b
    WHERE b.productPartNumber IS NOT NULL
      AND b.locationIdentifier IS NOT NULL
)
INSERT INTO SILVER.inventory_v2 (
    productID,
    locationID,
    inventoryType,
    quantity,
    quantityUnits,
    value,
    valueCurrency,
    reservationOrders,
    daysOfSupply,
    shelfLife,
    reorderLevel,
    expectedLeadTime,
    quantityUpperThreshold,
    quantityLowerThreshold,
    daysOfSupplyUpperThreshold,
    daysOfSupplyLowerThreshold,
    expiringThreshold,
    plannerCode,
    velocityCode,
    inventoryParentType,
    class,
    segment
)
SELECT 
    p.productID,                                -- Surrogate productID from SILVER.product_v6
    l.locationID,                               -- Surrogate locationID from SILVER.location_v3
    c.inventoryType,
    c.quantity,
    c.quantityUnits,
    c.value,
    c.valueCurrency,
    c.reservationOrders,
    c.daysOfSupply,
    c.shelfLife,
    c.reorderLevel,
    c.expectedLeadTime,
    c.quantityUpperThreshold,
    c.quantityLowerThreshold,
    c.daysOfSupplyUpperThreshold,
    c.daysOfSupplyLowerThreshold,
    c.expiringThreshold,
    c.plannerCode,
    c.velocityCode,
    c.inventoryParentType,
    c.class,
    c.segment
FROM CleanedInventory c
JOIN SILVER.product_v6 p ON c.productPartNumber = p.partNumber         -- Join on product part number to get productID
JOIN SILVER.location_v3 l ON c.locationIdentifier = l.locationIdentifier; -- Join on location identifier to get locationID

GO
-- Step 3: Create trigger to update lastUpdatedAt on record update
CREATE TRIGGER SILVER.inventory_v2_UpdateTimestamp
ON SILVER.inventory_v2
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE inv
    SET lastUpdatedAt = GETDATE()
    FROM SILVER.inventory_v2 inv
    INNER JOIN inserted i ON inv.inventoryID = i.inventoryID;
END;
-- Optional: Add indexes on foreign keys for performance
-- CREATE INDEX idx_inventory_productID ON SILVER.inventory_v2(productID);
-- CREATE INDEX idx_inventory_locationID ON SILVER.inventory_v2(locationID);


GO
-- ===============================================================
-- 🧼 Data Cleaning and Transformation for SILVER.supplyPlan_v2
-- ===============================================================

-- Step 1: Create the SILVER.supplyPlan_v2 table
CREATE TABLE SILVER.supplyPlan_v2 (
    supplyPlanID INT IDENTITY(1,1) PRIMARY KEY NOT NULL,        -- Surrogate primary key with auto-increment
    productID INT NOT NULL,                                     -- Foreign key referencing SILVER.product_v6(productID)
    locationID INT NOT NULL,                                    -- Foreign key referencing SILVER.location_v3(locationID)
    startDate DATE NOT NULL,                                    -- Start date of the supply plan
    duration INT NULL,                                          -- Duration of the plan (e.g., days)
    planParentType VARCHAR(50) NULL,                            -- Parent type/category of the plan (nullable)
    planType VARCHAR(50) NULL,                                  -- Specific plan type/category (nullable)
    quantity INT NULL,                                          -- Quantity planned
    quantityUnits VARCHAR(10) NULL,                             -- Units for quantity (e.g., pcs, kg)
    planningCycle INT NULL,                                     -- Planning cycle number (e.g., week number)
    source VARCHAR(50) NULL,                                    -- Source system or method for the plan (nullable)
    sourceLink VARCHAR(255) NULL DEFAULT 'N/A',                 -- URL or reference link to the source (nullable, defaults to 'N/A')
    insertedAt DATETIME DEFAULT GETDATE(),                      -- Record insert timestamp (default to current date/time)
    lastUpdatedAt DATETIME DEFAULT GETDATE(),                   -- Record last update timestamp (default to current date/time)
    FOREIGN KEY (productID) REFERENCES SILVER.product_v6(productID),  -- Foreign key constraint to product table
    FOREIGN KEY (locationID) REFERENCES SILVER.location_v3(locationID) -- Foreign key constraint to location table
);
GO

-- Step 2: Insert cleaned and transformed data from BRONZE.supplyPlan_v2
WITH CleanedSupplyPlan AS (
    SELECT 
        LTRIM(RTRIM(sp.productPartNumber)) AS productPartNumber,      -- Trim product part number
        LTRIM(RTRIM(sp.locationIdentifier)) AS locationIdentifier,    -- Trim location identifier
        sp.startDate,
        sp.duration,
        NULLIF(LTRIM(RTRIM(sp.planParentType)), '') AS planParentType, -- Trim & convert empty to NULL
        NULLIF(LTRIM(RTRIM(sp.planType)), '') AS planType,             -- Same
        sp.quantity,
        NULLIF(LTRIM(RTRIM(sp.quantityUnits)), '') AS quantityUnits,   -- Same
        sp.planningCycle,
        NULLIF(LTRIM(RTRIM(sp.source)), '') AS source,                 -- Same
        ISNULL(NULLIF(LTRIM(RTRIM(sp.sourceLink)), ''), 'N/A') AS sourceLink -- Default 'N/A' if empty
    FROM BRONZE.supplyPlan_v2 sp
)
INSERT INTO SILVER.supplyPlan_v2 (
    productID,
    locationID,
    startDate,
    duration,
    planParentType,
    planType,
    quantity,
    quantityUnits,
    planningCycle,
    source,
    sourceLink
)
SELECT 
    p.productID,                   -- Surrogate productID from SILVER.product_v6
    l.locationID,                  -- Surrogate locationID from SILVER.location_v3
    c.startDate,
    c.duration,
    c.planParentType,
    c.planType,
    c.quantity,
    c.quantityUnits,
    c.planningCycle,
    c.source,
    c.sourceLink
FROM CleanedSupplyPlan c
INNER JOIN SILVER.product_v6 p ON c.productPartNumber = p.partNumber  -- Join on product part number
INNER JOIN SILVER.location_v3 l ON c.locationIdentifier = l.locationIdentifier; -- Join on location identifier
GO

-- Step 3: Create trigger to update lastUpdatedAt on record update
CREATE TRIGGER SILVER.supplyPlan_v2_UpdateTimestamp
ON SILVER.supplyPlan_v2
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE sp
    SET lastUpdatedAt = GETDATE()
    FROM SILVER.supplyPlan_v2 sp
    INNER JOIN inserted i ON sp.supplyPlanID = i.supplyPlanID;
END;
GO

-- Optional: Indexes for faster joins and queries
-- CREATE INDEX idx_supplyPlan_productID ON SILVER.supplyPlan_v2(productID);
-- CREATE INDEX idx_supplyPlan_locationID ON SILVER.supplyPlan_v2(locationID);


GO
-- ============================================
-- Create SILVER.order_v3 table with constraints
-- ============================================
CREATE TABLE SILVER.order_v3 (
    orderID INT IDENTITY(1,1) PRIMARY KEY NOT NULL,                  -- Surrogate primary key
    orderIdentifier VARCHAR(50) NOT NULL UNIQUE,                     -- Business key (unique)
    orderType VARCHAR(50) NULL,                                      -- Order type/category
    vendorOrganisationID INT NOT NULL,                               -- FK to vendor organisation
    buyerOrganisationID INT NOT NULL,                                -- FK to buyer organisation
    shipFromLocationID INT NOT NULL,                                 -- FK to shipping origin location
    shipToLocationID INT NOT NULL,                                   -- FK to shipping destination location
    orderStatus VARCHAR(50) NULL,                                    -- Current order status
    createdDate DATETIME NULL,                                       -- Date order created
    requestedShipDate DATETIME NULL,                                 -- Requested ship date
    requestedDeliveryDate DATETIME NULL,                             -- Requested delivery date
    plannedShipDate DATETIME NULL,                                   -- Planned ship date
    plannedDeliveryDate DATETIME NULL,                               -- Planned delivery date
    quantity INT NULL,                                               -- Order quantity
    quantityUnits VARCHAR(10) NULL,                                  -- Units for quantity (e.g., pcs)
    totalValue DECIMAL(14,2) NULL,                                   -- Total monetary value of order
    orderValueCurrency VARCHAR(10) NULL,                             -- Currency of order value (e.g., USD)
    lineCount INT NULL,                                              -- Number of lines/items in order
    totalShippedQuantity INT NULL,                                   -- Total quantity shipped so far
    exclude BIT NULL,                                                -- Exclude flag (for reporting)
    sourceLink VARCHAR(255) NULL DEFAULT 'N/A',                      -- Source reference/link (default 'N/A')
    insertedAt DATETIME DEFAULT GETDATE(),                           -- Insert timestamp
    lastUpdatedAt DATETIME DEFAULT GETDATE(),                        -- Last update timestamp
    FOREIGN KEY (vendorOrganisationID) REFERENCES SILVER.organisation_v3(organisationID),  -- FK vendor org
    FOREIGN KEY (buyerOrganisationID) REFERENCES SILVER.organisation_v3(organisationID),   -- FK buyer org
    FOREIGN KEY (shipFromLocationID) REFERENCES SILVER.location_v3(locationID),           -- FK ship-from location
    FOREIGN KEY (shipToLocationID) REFERENCES SILVER.location_v3(locationID)              -- FK ship-to location
);
GO

-- ============================================
-- Insert deduplicated and cleaned data from BRONZE.order_v3
-- Deduplicate by orderIdentifier, keep latest createdDate record only
-- ============================================
;WITH DeduplicatedOrders AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY orderIdentifier ORDER BY createdDate DESC) AS rn
    FROM BRONZE.order_v3
)
INSERT INTO SILVER.order_v3 (
    orderIdentifier,
    orderType,
    vendorOrganisationID,
    buyerOrganisationID,
    shipFromLocationID,
    shipToLocationID,
    orderStatus,
    createdDate,
    requestedShipDate,
    requestedDeliveryDate,
    plannedShipDate,
    plannedDeliveryDate,
    quantity,
    quantityUnits,
    totalValue,
    orderValueCurrency,
    lineCount,
    totalShippedQuantity,
    exclude,
    sourceLink
)
SELECT 
    o.orderIdentifier,
    NULLIF(LTRIM(RTRIM(o.orderType)), ''),                               -- Trimmed orderType or NULL
    v.organisationID,                                                    -- Vendor org surrogate key
    b.organisationID,                                                    -- Buyer org surrogate key
    sf.locationID,                                                       -- Ship-from location surrogate key
    st.locationID,                                                       -- Ship-to location surrogate key
    NULLIF(LTRIM(RTRIM(o.orderStatus)), ''),                            -- Trimmed orderStatus or NULL
    o.createdDate,
    o.requestedShipDate,
    o.requestedDeliveryDate,
    o.plannedShipDate,
    o.plannedDeliveryDate,
    o.quantity,
    NULLIF(LTRIM(RTRIM(o.quantityUnits)), ''),                          -- Trimmed quantityUnits or NULL
    o.totalValue,
    NULLIF(LTRIM(RTRIM(o.orderValueCurrency)), ''),                     -- Trimmed currency or NULL
    o.lineCount,
    o.totalShippedQuantity,
    o.exclude,
    ISNULL(NULLIF(LTRIM(RTRIM(o.sourceLink)), ''), 'N/A')              -- Trimmed sourceLink or default 'N/A'
FROM DeduplicatedOrders o
INNER JOIN SILVER.organisation_v3 v
    ON LTRIM(RTRIM(o.vendorOrganisationIdentifier)) = v.organisationIdentifier
INNER JOIN SILVER.organisation_v3 b
    ON LTRIM(RTRIM(o.buyerOrganisationIdentifier)) = b.organisationIdentifier
INNER JOIN SILVER.location_v3 sf
    ON LTRIM(RTRIM(o.shipFromInstructionLocationIdentifier)) = sf.locationIdentifier
INNER JOIN SILVER.location_v3 st
    ON LTRIM(RTRIM(o.shipToLocationIdentifier)) = st.locationIdentifier
WHERE o.rn = 1;  -- Only latest record per orderIdentifier
GO

-- ============================================
-- Trigger to update lastUpdatedAt timestamp on UPDATE
-- ============================================
CREATE TRIGGER SILVER.order_v3_UpdateTimestamp
ON SILVER.order_v3
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE o
    SET lastUpdatedAt = GETDATE()
    FROM SILVER.order_v3 o
    INNER JOIN inserted i ON o.orderID = i.orderID;
END;


GO
-- Step 1: Drop existing SILVER.shipment_v4 table if it exists
DROP TABLE IF EXISTS SILVER.shipment_v4;

GO
--- Create SILVER.shipment_v4
CREATE TABLE SILVER.shipment_v4 (
    shipmentID INT IDENTITY(1,1) PRIMARY KEY,                           -- Surrogate key for shipment
    shipmentIdentifier VARCHAR(100) NOT NULL UNIQUE,                    -- Business key unique shipment ID
    orderIdentifier VARCHAR(100),                                       -- Business key for order
    productIdentifier VARCHAR(100),                                     -- Business key for product

    productID INT,                                                      -- Surrogate foreign key to product
    orderID INT,                                                        -- Surrogate foreign key to order
    shipFromLocationID INT,                                             -- Surrogate foreign key for origin location
    shipToLocationID INT,                                               -- Surrogate foreign key for destination location
    vendorOrganisationID INT,                                           -- Surrogate foreign key for vendor organisation
    buyerOrganisationID INT,                                            -- Surrogate foreign key for buyer organisation
    carrierOrganisationID INT,                                          -- Surrogate foreign key for carrier organisation

    quantity INT,                                                      -- Shipment quantity
    weight DECIMAL(10, 2),                                             -- Shipment weight
    weightUOM VARCHAR(10),                                             -- Weight unit of measure
    transportMode VARCHAR(50),                                         -- Mode of transport (Air, Sea, etc.)
    status VARCHAR(50),                                                -- Shipment status

    requestedShipmentDate DATE,                                        -- Shipment scheduling dates
    committedShipmentDate DATE,
    estimatedShipmentDate DATE,
    revisedShipmentDate DATE,
    predictedShipmentDate DATE,
    actualShipmentDate DATE,

    requestedArrivalDate DATE,                                        -- Arrival scheduling dates
    committedArrivalDate DATE,
    estimatedArrivalDate DATE,
    revisedArrivalDate DATE,
    predictedArrivalDate DATE,
    actualArrivalDate DATE,

    trackingNumber VARCHAR(100),                                       -- Tracking and shipping document info
    houseAirwayBill VARCHAR(100),
    billOfLadingNumber VARCHAR(100),
    manifest VARCHAR(100),
    proNumber VARCHAR(100),

    currentRegion VARCHAR(100),                                        -- Current shipment region/location info
    currentLocationCoordinates VARCHAR(100),

    exclude BIT DEFAULT 0,                                             -- Flag for excluding record from processing
    insertedAt DATETIME DEFAULT GETDATE(),                            -- Insertion timestamp
    lastUpdatedAt DATETIME DEFAULT GETDATE(),                         -- Last update timestamp

    -- Foreign key constraints linking surrogate keys to their parent tables
    CONSTRAINT FK_Shipment_Product FOREIGN KEY (productID)
        REFERENCES SILVER.product_v6(productID),

    CONSTRAINT FK_Shipment_Order FOREIGN KEY (orderID)
        REFERENCES SILVER.order_v3(orderID),

    CONSTRAINT FK_Shipment_ShipFromLocation FOREIGN KEY (shipFromLocationID)
        REFERENCES SILVER.location_v3(locationID),

    CONSTRAINT FK_Shipment_ShipToLocation FOREIGN KEY (shipToLocationID)
        REFERENCES SILVER.location_v3(locationID),

    CONSTRAINT FK_Shipment_VendorOrg FOREIGN KEY (vendorOrganisationID)
        REFERENCES SILVER.organisation_v3(organisationID),

    CONSTRAINT FK_Shipment_BuyerOrg FOREIGN KEY (buyerOrganisationID)
        REFERENCES SILVER.organisation_v3(organisationID),

    CONSTRAINT FK_Shipment_CarrierOrg FOREIGN KEY (carrierOrganisationID)
        REFERENCES SILVER.organisation_v3(organisationID)
);
GO
-- Step 3: Extract the most recent shipment records for deduplication and transform them for insertion
;WITH RankedShipments AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY shipmentIdentifier 
               ORDER BY dateCreated DESC
           ) AS rn
    FROM BRONZE.shipment_v4
),
CleanedShipments AS (
    SELECT *
    FROM RankedShipments
    WHERE rn = 1
),
Transformed AS (
    SELECT
        -- Raw identifiers
        cs.shipmentIdentifier,
        cs.orderIdentifier,
        cs.productIdentifier,

        -- Surrogate keys from SILVER dimension tables by joining on business keys
        p.productID,
        o.orderID,
        lf.locationID AS shipFromLocationID,
        lt.locationID AS shipToLocationID,
        ov.organisationID AS vendorOrganisationID,
        ob.organisationID AS buyerOrganisationID,
        oc.organisationID AS carrierOrganisationID,

        -- Numeric and text fields, cleaned and casted
        TRY_CAST(cs.lineCount AS INT) AS quantity,						-- Convert lineCount to integer, handling possible non-numeric safely
        TRY_CAST(cs.weight AS DECIMAL(10, 2)) AS weight,				-- Convert weight to decimal with 2 decimal places
        NULLIF(LTRIM(RTRIM(cs.weightUnits)), '') AS weightUOM,			-- Trim spaces and convert empty strings to NULL for weight unit
        NULLIF(LTRIM(RTRIM(cs.transportMode)), '') AS transportMode,	-- Trim spaces and nullify empty transport mode strings
        NULLIF(LTRIM(RTRIM(cs.status)), '') AS status,					-- Trim spaces and convert empty status strings to NULL

        -- Date fields converted properly
        CAST(cs.dateCreated AS DATE) AS requestedShipmentDate,					-- Cast datetime to date for requested shipment date
        CAST(cs.committedTimeOfArrival AS DATE) AS committedShipmentDate,
        CAST(cs.estimatedTimeOfArrival AS DATE) AS estimatedShipmentDate,
        CAST(cs.revisedEstimatedTimeOfArrival AS DATE) AS revisedShipmentDate,	-- Cast datetime to date for revised estimated shipment date
        CAST(cs.predictedTimeOfArrival AS DATE) AS predictedShipmentDate,
        CAST(cs.actualShipDate AS DATE) AS actualShipmentDate,

        CAST(cs.requestedTimeOfArrival AS DATE) AS requestedArrivalDate,
        CAST(cs.committedTimeOfArrival AS DATE) AS committedArrivalDate,
        CAST(cs.estimatedTimeOfArrival AS DATE) AS estimatedArrivalDate,
        CAST(cs.revisedEstimatedTimeOfArrival AS DATE) AS revisedArrivalDate,		-- Cast datetime to date for revised estimated arrival date (arrival)
        CAST(cs.predictedTimeOfArrival AS DATE) AS predictedArrivalDate,
        CAST(cs.actualTimeOfArrival AS DATE) AS actualArrivalDate,

        -- Tracking/document info cleaned
        NULLIF(LTRIM(RTRIM(cs.parcelTrackingNumber)), '') AS trackingNumber,		-- Trim spaces and nullify empty tracking numbers
        NULLIF(LTRIM(RTRIM(cs.houseAirwayBill)), '') AS houseAirwayBill,
        NULLIF(LTRIM(RTRIM(cs.billOfLadingNumber)), '') AS billOfLadingNumber,
        NULLIF(LTRIM(RTRIM(cs.manifest)), '') AS manifest,
        NULLIF(LTRIM(RTRIM(cs.proNumber)), '') AS proNumber,

        -- Current location info cleaned
        NULLIF(LTRIM(RTRIM(cs.currentRegion)), '') AS currentRegion,
        NULLIF(LTRIM(RTRIM(cs.currentLocationCoordinates)), '') AS currentLocationCoordinates,		-- Trim spaces and nullify empty coordinates

        -- Flags and audit columns
        ISNULL(cs.exclude, 0) AS exclude,				-- Replace null exclude flags with 0 (false)
        GETDATE() AS insertedAt,						-- Set current timestamp for insert
        GETDATE() AS lastUpdatedAt						-- Set current timestamp for update

    FROM CleanedShipments cs
    LEFT JOIN SILVER.product_v6 p ON p.partNumber = cs.productIdentifier
    LEFT JOIN SILVER.order_v3 o ON o.orderIdentifier = cs.orderIdentifier
    LEFT JOIN SILVER.location_v3 lf ON lf.locationIdentifier = cs.shipFromLocationIdentifier
    LEFT JOIN SILVER.location_v3 lt ON lt.locationIdentifier = cs.shipToLocationIdentifier
    LEFT JOIN SILVER.organisation_v3 ov ON ov.organisationIdentifier = cs.vendorOrganizationIdentifier
    LEFT JOIN SILVER.organisation_v3 ob ON ob.organisationIdentifier = cs.buyerOrganizationIdentifier
    LEFT JOIN SILVER.organisation_v3 oc ON oc.organisationIdentifier = cs.carrierOrganizationIdentifier
    WHERE cs.shipmentIdentifier IS NOT NULL
)

INSERT INTO SILVER.shipment_v4 (
    shipmentIdentifier, orderIdentifier, productIdentifier, productID, orderID,
    shipFromLocationID, shipToLocationID, vendorOrganisationID, buyerOrganisationID, carrierOrganisationID,
    quantity, weight, weightUOM, transportMode, status,
    requestedShipmentDate, committedShipmentDate, estimatedShipmentDate, revisedShipmentDate, predictedShipmentDate, actualShipmentDate,
    requestedArrivalDate, committedArrivalDate, estimatedArrivalDate, revisedArrivalDate, predictedArrivalDate, actualArrivalDate,
    trackingNumber, houseAirwayBill, billOfLadingNumber, manifest, proNumber,
    currentRegion, currentLocationCoordinates, exclude, insertedAt, lastUpdatedAt
)
SELECT
    shipmentIdentifier, orderIdentifier, productIdentifier, productID, orderID,
    shipFromLocationID, shipToLocationID, vendorOrganisationID, buyerOrganisationID, carrierOrganisationID,
    quantity, weight, weightUOM, transportMode, status,
    requestedShipmentDate, committedShipmentDate, estimatedShipmentDate, revisedShipmentDate, predictedShipmentDate, actualShipmentDate,
    requestedArrivalDate, committedArrivalDate, estimatedArrivalDate, revisedArrivalDate, predictedArrivalDate, actualArrivalDate,
    trackingNumber, houseAirwayBill, billOfLadingNumber, manifest, proNumber,
    currentRegion, currentLocationCoordinates, exclude, insertedAt, lastUpdatedAt
FROM Transformed;


GO
-- Create SILVER.DimDate table
CREATE TABLE SILVER.dimDate (
    dateID INT IDENTITY(1,1) PRIMARY KEY,           -- Surrogate key for each date (auto-increment)
    date DATE NOT NULL UNIQUE,                       -- Actual calendar date (unique)
    year INT NOT NULL,                               -- Year part of the date (calendar year)
    quarter INT NOT NULL,                            -- Quarter of the year (1 to 4, calendar)
    month INT NOT NULL,                              -- Month number (1 to 12)
    monthName VARCHAR(20) NOT NULL,                  -- Month name (e.g., January)
    day INT NOT NULL,                                -- Day of the month (1 to 31)
    week INT NOT NULL,                               -- ISO 8601 Week number in the year (1-53)
    dayOfWeekNumber INT NOT NULL,                    -- ISO day of week number (Monday=1, Sunday=7)
    dayOfWeekName VARCHAR(20) NOT NULL,             -- Name of the day (e.g., Monday)
    isWeekend BIT NOT NULL,                          -- Flag if day is weekend (1 = yes, 0 = no)
    isHoliday BIT NOT NULL DEFAULT 0,                -- Flag if date is holiday (default 0 = no)
    fiscalYear INT NOT NULL,                         -- Fiscal year (example fiscal year starts April 1)
    fiscalQuarter INT NOT NULL,                      -- Fiscal quarter (1 to 4)
    insertedAt DATETIME DEFAULT GETDATE(),          -- Timestamp when row inserted
    lastUpdatedAt DATETIME DEFAULT GETDATE()        -- Timestamp when row last updated (update via ETL or triggers)
);

-- Notes:
-- Week number uses ISO 8601 standard: weeks start on Monday; the first week has the first Thursday of the year.
-- dayOfWeekNumber follows ISO 8601: Monday = 1, Sunday = 7.
-- Fiscal year starts April 1 (adjust logic if your fiscal year differs).
-- Update lastUpdatedAt column as needed on row updates.
