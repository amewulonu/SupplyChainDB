-- Data Cleaning and Transformation for Product Data
-- Create table SILVER.product_v6
-- Created the SILVER.product_v6 table to store detailed product information with a surrogate primary key (productID).
-- The table enforces uniqueness on partNumber and includes various product attributes such as type, category, brand, family, line, segment, status, value, and units.
-- It also tracks descriptive fields (name, description), planner code, and source link.
-- Automatic timestamps (insertedAt and lastUpdatedAt) are included to record when each row is created or last updated.
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
insertedAt DATETIME DEFAULT GETDATE(),
lastUpdatedAt DATETIME DEFAULT GETDATE()
);

GO
-- Insert cleaned and standardized product data from BRONZE.product_v6 into SILVER.product_v6.
-- For each field, trims whitespace, converts to VARCHAR, and replaces empty or NULL values with default placeholders (e.g., 'UNKNOWN', 'N/A', or 0.00 for value).
-- This ensures data consistency and handles missing or malformed values during the transition from raw BRONZE to cleaned SILVER layer.

INSERT INTO SILVER.product_v6 (
    partNumber, productType, categoryCode, brandCode, familyCode,
    lineCode, productSegmentCode, status, value, valueCurrency,
    defaultQuantityUnits, name, description, plannerCode, sourceLink
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
    ISNULL(NULLIF(LTRIM(RTRIM(CAST(sourceLink AS VARCHAR(MAX)))), ''), 'N/A')               -- Cleaned sourceLink or 'N/A'
FROM BRONZE.product_v6;

GO

-- Created the SILVER.organisation_v3 table to store organisation details with a surrogate primary key (organisationID).
-- Each organisation has a unique organisationIdentifier and includes attributes like type, location, name, and division.
-- The table also tracks the source of the data (sourceLink) and records timestamps for row creation and last update.
CREATE TABLE SILVER.organisation_v3 (
    organisationID INT IDENTITY(1,1) PRIMARY KEY NOT NULL,
    organisationIdentifier VARCHAR(50) NOT NULL UNIQUE,
    orgType VARCHAR(50) NOT NULL,
    locationIdentifier VARCHAR(50) NOT NULL,
    name VARCHAR(100) NOT NULL,
    division VARCHAR(50),
    sourceLink VARCHAR(255),
    insertedAt DATETIME DEFAULT GETDATE(),
    lastUpdatedAt DATETIME DEFAULT GETDATE()
);

GO
-- Insert organisation data into SILVER.organisation_v3 with specified columns.
-- This populates the table with organisation identifiers, types, locations, names, divisions, and source links.
-- Data is expected to be transformed or cleaned before insertion to ensure consistency in the SILVER layer.
 INSERT INTO SILVER.organisation_v3 (
    organisationIdentifier,
    orgType,
    locationIdentifier,
    name,
    division,
    sourceLink
)
-- Retrieve and clean organisation data from BRONZE.organisation_v3 by trimming leading and trailing spaces 
-- from all relevant text fields to ensure data consistency.
-- Only includes rows where organisationIdentifier is not NULL, filtering out incomplete records.

SELECT 
    TRIM(organisationIdentifier),
    TRIM(orgType),
    TRIM(locationIdentifier),
    TRIM(name),
    TRIM(division),
    TRIM(sourceLink)
FROM BRONZE.organisation_v3
WHERE organisationIdentifier IS NOT NULL;

GO
-- Task 2 
-- Create table SILVER.location_v3
CREATE TABLE SILVER.location_v3 (
	locationID INT IDENTITY(1,1) PRIMARY KEY NOT NULL,
    locationIdentifier VARCHAR(50) NOT NULL UNIQUE,
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
    geo VARCHAR(50) NULL,
    sourceLink VARCHAR(300) NOT NULL,
	insertedAt DATETIME DEFAULT GETDATE(),
    lastUpdatedAt DATETIME DEFAULT GETDATE()
);
GO

INSERT INTO SILVER.location_v3 (
    locationIdentifier,       -- Unique identifier for the location, cleaned and defaulted if missing
    locationType,             -- Type of location, cleaned and defaulted if missing
    locationName,             -- Name of the location, cleaned and defaulted if missing
    address1,                 -- Primary address line, cleaned and defaulted if missing
    address2,                 -- Secondary address line, trimmed, nullable if empty
    city,                     -- City name, cleaned and defaulted if missing
    postalCode,               -- Postal code, cleaned and defaulted if missing
    stateProvince,            -- State or province, trimmed, nullable if empty
    country,                  -- Country name, cleaned and defaulted if missing
    coordinates,              -- Coordinates string, trimmed, nullable if empty
    includeInCorrelation,     -- Boolean flag indicating inclusion in correlation analyses
    geo,                      -- Geographic info, trimmed, nullable if empty
    sourceLink                -- Source reference link, cleaned and defaulted if missing
)
SELECT
    ISNULL(NULLIF(LTRIM(RTRIM(locationIdentifier)), ''), 'UNKNOWN'),  -- Trim, replace empty with 'UNKNOWN'
    ISNULL(NULLIF(LTRIM(RTRIM(locationType)), ''), 'UNKNOWN'),        -- Trim, replace empty with 'UNKNOWN'
    ISNULL(NULLIF(LTRIM(RTRIM(locationName)), ''), 'UNKNOWN'),        -- Trim, replace empty with 'UNKNOWN'
    ISNULL(NULLIF(LTRIM(RTRIM(address1)), ''), 'N/A'),                -- Trim, replace empty with 'N/A'
    NULLIF(LTRIM(RTRIM(address2)), ''),                               -- Trim, keep NULL if empty
    ISNULL(NULLIF(LTRIM(RTRIM(city)), ''), 'UNKNOWN'),                -- Trim, replace empty with 'UNKNOWN'
    ISNULL(NULLIF(LTRIM(RTRIM(postalCode)), ''), 'N/A'),              -- Trim, replace empty with 'N/A'
    NULLIF(LTRIM(RTRIM(stateProvince)), ''),                          -- Trim, keep NULL if empty
    ISNULL(NULLIF(LTRIM(RTRIM(country)), ''), 'UNKNOWN'),             -- Trim, replace empty with 'UNKNOWN'
    NULLIF(LTRIM(RTRIM(coordinates)), ''),                            -- Trim, keep NULL if empty
    includeInCorrelation_bit,                                         -- Keep boolean flag as is
    NULLIF(LTRIM(RTRIM(geo)), ''),                                   -- Trim, keep NULL if empty
    ISNULL(NULLIF(LTRIM(RTRIM(sourceLink)), ''), 'N/A')               -- Trim, replace empty with 'N/A'
FROM BRONZE.location_v3;

GO
-- Create SILVER.inventory_v2
CREATE TABLE SILVER.inventory_v2 (
    inventoryID INT IDENTITY(1,1) PRIMARY KEY NOT NULL,  -- Surrogate primary key with auto-increment
    productID INT NOT NULL,                              -- Foreign key to product table
    locationID INT NOT NULL,                             -- Foreign key to location table
    inventoryType VARCHAR(50),                           -- Type/category of inventory
    quantity INT,                                        -- Quantity available
    quantityUnits VARCHAR(10),                           -- Units for quantity (e.g., pcs, kg)
    value DECIMAL(12,2),                                 -- Monetary value of inventory
    valueCurrency VARCHAR(10),                           -- Currency code for value (e.g., USD)
    reservationOrders INT,                               -- Number of orders reserved against inventory
    daysOfSupply INT,                                    -- Days of supply available
    shelfLife INT,                                       -- Shelf life in days
    reorderLevel INT,                                    -- Inventory reorder level threshold
    expectedLeadTime INT,                                -- Expected lead time for replenishment
    quantityUpperThreshold INT,                          -- Upper threshold for quantity (alerts, planning)
    quantityLowerThreshold INT,                          -- Lower threshold for quantity (alerts, planning)
    daysOfSupplyUpperThreshold INT,                      -- Upper threshold for days of supply
    daysOfSupplyLowerThreshold INT,                      -- Lower threshold for days of supply
    expiringThreshold INT,                               -- Threshold for near-expiry inventory
    plannerCode VARCHAR(50),                             -- Code for inventory planner or management group
    velocityCode VARCHAR(20),                            -- Inventory velocity classification (e.g., fast-moving)
    inventoryParentType VARCHAR(50),                     -- Parent type/category of inventory
    class VARCHAR(50),                                   -- Classification group of inventory
    segment VARCHAR(50),                                 -- Market or business segment classification
    insertedAt DATETIME DEFAULT GETDATE(),               -- Timestamp for insertion
    lastUpdatedAt DATETIME DEFAULT GETDATE(),            -- Timestamp for last update
    FOREIGN KEY (productID) REFERENCES SILVER.product_v6(productID), -- Foreign key constraint to product table
    FOREIGN KEY (locationID) REFERENCES SILVER.location_v3(locationID) -- Foreign key constraint to location table
);
GO

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
    p.productID,                                           -- Lookup productID from SILVER.product_v6 by matching partNumber
    l.locationID,                                          -- Lookup locationID from SILVER.location_v3 by matching locationIdentifier
    TRIM(b.inventoryType),                                 -- Trim whitespace from inventoryType from BRONZE source
    b.quantity,                                           -- Quantity value from BRONZE source
    b.quantityUnits,                                      -- Quantity units from BRONZE source
    b.value,                                             -- Inventory value from BRONZE source
    b.valueCurrency,                                      -- Currency code from BRONZE source
    b.researvationOrders,                                 -- Reserved orders (typo here: should be reservationOrders) from BRONZE source
    b.daysOfSupply,                                       -- Days of supply from BRONZE source
    b.shelfLife,                                          -- Shelf life from BRONZE source
    b.reorderLevel,                                       -- Reorder level from BRONZE source
    b.expectedLeadTime,                                   -- Expected lead time from BRONZE source
    b.quantityUpperThreshold,                             -- Upper quantity threshold from BRONZE source
    b.quantityLowerThreshold,                             -- Lower quantity threshold from BRONZE source
    b.daysOfSupplyUpperThreshold,                         -- Upper days of supply threshold from BRONZE source
    b.daysOfSupplyLowerThreshold,                         -- Lower days of supply threshold from BRONZE source
    b.expiringThreshold,                                  -- Expiring threshold from BRONZE source
    b.plannerCode,                                        -- Planner code from BRONZE source
    b.velocityCode,                                       -- Velocity code from BRONZE source
    b.inventoryParentType,                                -- Parent type of inventory from BRONZE source
    b.class,                                             -- Classification from BRONZE source
    b.segment                                            -- Segment from BRONZE source
FROM BRONZE.inventory_v2 b
JOIN SILVER.product_v6 p ON b.productPartNumber = p.partNumber  -- Join to map product part number to surrogate key
JOIN SILVER.location_v3 l ON b.locationIdentifier = l.locationIdentifier; -- Join to map location identifier to surrogate key


GO
-- Create SILVER.supplyPlan_v2
CREATE TABLE SILVER.supplyPlan_v2 (
    supplyPlanID INT IDENTITY(1,1) PRIMARY KEY NOT NULL,        -- Surrogate primary key with auto-increment
    productID INT NOT NULL,                                     -- Foreign key referencing product table
    locationID INT NOT NULL,                                    -- Foreign key referencing location table
    startDate DATE NOT NULL,                                    -- Start date of the supply plan
    duration INT,                                               -- Duration of the plan (e.g., days)
    planParentType VARCHAR(50),                                 -- Parent type/category of the plan (nullable)
    planType VARCHAR(50),                                       -- Specific plan type/category (nullable)
    quantity INT,                                               -- Quantity planned
    quantityUnits VARCHAR(10),                                  -- Units for quantity (e.g., pcs, kg)
    planningCycle INT,                                          -- Planning cycle number (e.g., week number)
    source VARCHAR(50),                                         -- Source system or method for the plan (nullable)
    sourceLink VARCHAR(255),                                    -- URL or reference link to the source (nullable, default 'N/A')
    insertedAt DATETIME DEFAULT GETDATE(),                      -- Record insert timestamp (default to current date/time)
    lastUpdatedAt DATETIME DEFAULT GETDATE(),                   -- Record last update timestamp (default to current date/time)
    FOREIGN KEY (productID) REFERENCES SILVER.product_v6(productID),  -- Foreign key constraint to product table
    FOREIGN KEY (locationID) REFERENCES SILVER.location_v3(locationID) -- Foreign key constraint to location table
);

GO

INSERT INTO SILVER.supplyplan_v2 (
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
    p.productID,                                          -- Lookup productID by matching trimmed productPartNumber
    l.locationID,                                         -- Lookup locationID by matching trimmed locationIdentifier
    sp.startDate,                                         -- Start date from source table
    sp.duration,                                          -- Duration from source table
    NULLIF(LTRIM(RTRIM(sp.planParentType)), ''),         -- Trim whitespace, convert empty strings to NULL
    NULLIF(LTRIM(RTRIM(sp.planType)), ''),               -- Same trimming and NULL conversion for planType
    sp.quantity,                                          -- Quantity planned from source
    NULLIF(LTRIM(RTRIM(sp.quantityUnits)), ''),          -- Trim and convert empty quantity units to NULL
    sp.planningCycle,                                     -- Planning cycle number from source
    NULLIF(LTRIM(RTRIM(sp.source)), ''),                  -- Trim source field, convert empty to NULL
    ISNULL(NULLIF(LTRIM(RTRIM(sp.sourceLink)), ''), 'N/A')  -- Trim sourceLink, if empty replace with 'N/A'
FROM BRONZE.supplyPlan_v2 sp
INNER JOIN SILVER.product_v6 p
    ON LTRIM(RTRIM(sp.productPartNumber)) = p.partNumber  -- Join on trimmed product part number
INNER JOIN SILVER.location_v3 l
    ON LTRIM(RTRIM(sp.locationIdentifier)) = l.locationIdentifier;  -- Join on trimmed location identifier

GO
-- Create a table SILVER.order_v3
CREATE TABLE SILVER.order_v3 (
    orderID INT IDENTITY(1,1) PRIMARY KEY NOT NULL,                  -- Surrogate primary key
    orderIdentifier VARCHAR(50) NOT NULL UNIQUE,                     -- Unique business key for order
    orderType VARCHAR(50),                                           -- Type/category of order (nullable)
    vendorOrganisationID INT NOT NULL,                               -- FK to vendor organisation
    buyerOrganisationID INT NOT NULL,                                -- FK to buyer organisation
    shipFromLocationID INT NOT NULL,                                 -- FK to shipping origin location
    shipToLocationID INT NOT NULL,                                   -- FK to shipping destination location
    orderStatus VARCHAR(50),                                         -- Current order status (nullable)
    createdDate DATETIME,                                            -- Date order was created
    requestedShipDate DATETIME,                                      -- Requested ship date
    requestedDeliveryDate DATETIME,                                  -- Requested delivery date
    plannedShipDate DATETIME,                                        -- Planned ship date
    plannedDeliveryDate DATETIME,                                    -- Planned delivery date
    quantity INT,                                                   -- Order quantity
    quantityUnits VARCHAR(10),                                       -- Units for quantity (e.g., pcs)
    totalValue DECIMAL(14,2),                                        -- Total monetary value of order
    orderValueCurrency VARCHAR(10),                                 -- Currency for order value (e.g., USD)
    lineCount INT,                                                  -- Number of lines/items in order
    totalShippedQuantity INT,                                        -- Total quantity shipped so far
    exclude BIT,                                                    -- Flag to exclude order from reports/analysis
    sourceLink VARCHAR(255),                                         -- Link or reference to source data
    insertedAt DATETIME DEFAULT GETDATE(),                          -- Insert timestamp, defaults to current time
    lastUpdatedAt DATETIME DEFAULT GETDATE(),                       -- Last update timestamp, defaults to current time
    FOREIGN KEY (vendorOrganisationID) REFERENCES SILVER.organisation_v3(organisationID),  -- FK vendor org
    FOREIGN KEY (buyerOrganisationID) REFERENCES SILVER.organisation_v3(organisationID),   -- FK buyer org
    FOREIGN KEY (shipFromLocationID) REFERENCES SILVER.location_v3(locationID),           -- FK ship-from location
    FOREIGN KEY (shipToLocationID) REFERENCES SILVER.location_v3(locationID)              -- FK ship-to location
);
GO

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
    o.orderIdentifier,                                         -- Order business key
    NULLIF(LTRIM(RTRIM(o.orderType)), ''),                    -- Trim and convert empty strings to NULL
    v.organisationID,                                          -- FK vendor org ID matched by identifier
    b.organisationID,                                          -- FK buyer org ID matched by identifier
    sf.locationID,                                             -- FK ship-from location matched by identifier
    st.locationID,                                             -- FK ship-to location matched by identifier
    NULLIF(LTRIM(RTRIM(o.orderStatus)), ''),                  -- Trimmed order status or NULL
    o.createdDate,                                            -- Order creation datetime
    o.requestedShipDate,                                      -- Requested ship date
    o.requestedDeliveryDate,                                  -- Requested delivery date
    o.plannedShipDate,                                        -- Planned ship date
    o.plannedDeliveryDate,                                    -- Planned delivery date
    o.quantity,                                               -- Order quantity
    NULLIF(LTRIM(RTRIM(o.quantityUnits)), ''),                -- Trimmed quantity units or NULL
    o.totalValue,                                             -- Total value amount
    NULLIF(LTRIM(RTRIM(o.orderValueCurrency)), ''),           -- Trimmed currency or NULL
    o.lineCount,                                              -- Number of order lines
    o.totalShippedQuantity,                                   -- Quantity shipped so far
    o.exclude,                                                -- Exclude flag
    ISNULL(NULLIF(LTRIM(RTRIM(o.sourceLink)), ''), 'N/A')    -- Source link or default 'N/A'
FROM DeduplicatedOrders o
INNER JOIN SILVER.organisation_v3 v
    ON LTRIM(RTRIM(o.vendorOrganisationIdentifier)) = v.organisationIdentifier  -- Join vendor org on trimmed identifier
INNER JOIN SILVER.organisation_v3 b
    ON LTRIM(RTRIM(o.buyerOrganisationIdentifier)) = b.organisationIdentifier   -- Join buyer org on trimmed identifier
INNER JOIN SILVER.location_v3 sf
    ON LTRIM(RTRIM(o.shipFromInstructionLocationIdentifier)) = sf.locationIdentifier  -- Join ship-from location
INNER JOIN SILVER.location_v3 st
    ON LTRIM(RTRIM(o.shipToLocationIdentifier)) = st.locationIdentifier         -- Join ship-to location
WHERE o.rn = 1;  -- Only keep the latest record per orderIdentifier based on createdDate

GO

-- Create a table SILVER.shipment_v4
CREATE TABLE SILVER.shipment_v4 (
    shipmentID INT IDENTITY(1,1) PRIMARY KEY NOT NULL,   -- Surrogate key, unique shipment ID
    shipmentIdentifier VARCHAR(50) NOT NULL UNIQUE,      -- Business key, unique shipment code
    shipmentType VARCHAR(50),                             -- Type/category of the shipment
    shipFromLocationID INT NOT NULL,                      -- FK to location_v3, origin location ID
    shipToLocationID INT NOT NULL,                        -- FK to location_v3, destination location ID
    vendorOrganisationID INT NOT NULL,                    -- FK to organisation_v3, vendor/supplier org ID
    buyerOrganisationID INT NOT NULL,                     -- FK to organisation_v3, buyer org ID
    carrierOrganisationID INT NOT NULL,                   -- FK to organisation_v3, carrier/shipping org ID
    status VARCHAR(50),                                   -- Current shipment status
    dateCreated DATETIME,                                 -- Timestamp when shipment record was created
    requestedTimeOfArrival DATETIME,                      -- Requested arrival time by receiver
    committedTimeOfArrival DATETIME,                      -- Committed arrival time by carrier
    actualShipDate DATETIME,                              -- Actual date shipment was sent
    estimatedTimeOfArrival DATETIME,                      -- Estimated arrival date/time
    revisedEstimatedTimeOfArrival DATETIME,               -- Revised ETA if updated
    predictedTimeOfArrival DATETIME,                       -- Predicted ETA from forecasting
    actualTimeOfArrival DATETIME,                          -- Actual arrival timestamp
    lineCount INT,                                        -- Number of line items in shipment
    weight DECIMAL(10,2),                                 -- Total weight of shipment
    weightUnits VARCHAR(20),                              -- Units for weight (e.g., kg, lbs)
    currentLocationCoordinates VARCHAR(100),              -- GPS or coordinate data for current location
    currentRegion VARCHAR(50),                            -- Region name for current shipment position
    transportMode VARCHAR(50),                            -- Mode of transport (e.g., air, sea, road)
    houseAirwayBill VARCHAR(50),                          -- House airway bill number
    parcelTrackingNumber VARCHAR(50),                     -- Parcel tracking code
    airwayMasterNumber VARCHAR(50),                       -- Master airway bill number
    billOfLadingNumber VARCHAR(50),                       -- Bill of lading number for shipments
    proNumber VARCHAR(50),                                -- PRO number (freight tracking number)
    manifest VARCHAR(50),                                 -- Manifest identifier
    exclude BIT,                                          -- Flag to exclude from reports/analysis
    sourceLink VARCHAR(255),                              -- Source system reference or link
    insertedAt DATETIME DEFAULT GETDATE(),                -- Record insertion timestamp (audit)
    lastUpdatedAt DATETIME DEFAULT GETDATE(),             -- Last update timestamp (audit)
    
    -- Foreign keys enforcing referential integrity:
    FOREIGN KEY (shipFromLocationID) REFERENCES SILVER.location_v3(locationID),
    FOREIGN KEY (shipToLocationID) REFERENCES SILVER.location_v3(locationID),
    FOREIGN KEY (vendorOrganisationID) REFERENCES SILVER.organisation_v3(organisationID),
    FOREIGN KEY (buyerOrganisationID) REFERENCES SILVER.organisation_v3(organisationID),
    FOREIGN KEY (carrierOrganisationID) REFERENCES SILVER.organisation_v3(organisationID)
);
GO

INSERT INTO SILVER.shipment_v4 (
    shipmentIdentifier,
    shipmentType,
    shipFromLocationID,
    shipToLocationID,
    vendorOrganisationID,
    buyerOrganisationID,
    carrierOrganisationID,
    status,
    dateCreated,
    requestedTimeOfArrival,
    committedTimeOfArrival,
    actualShipDate,
    estimatedTimeOfArrival,
    revisedEstimatedTimeOfArrival,
    predictedTimeOfArrival,
    actualTimeOfArrival,
    lineCount,
    weight,
    weightUnits,
    currentLocationCoordinates,
    currentRegion,
    transportMode,
    houseAirwayBill,
    parcelTrackingNumber,
    airwayMasterNumber,
    billOfLadingNumber,
    proNumber,
    manifest,
    exclude,
    sourceLink
)
SELECT 
    b.shipmentIdentifier,                              -- Shipment business key
    b.shipmentType,                                    -- Shipment type/category
    l_from.locationID AS shipFromLocationID,           -- FK: origin location
    l_to.locationID AS shipToLocationID,               -- FK: destination location
    o_vendor.organisationID AS vendorOrganisationID,   -- FK: vendor org
    o_buyer.organisationID AS buyerOrganisationID,     -- FK: buyer org
    o_carrier.organisationID AS carrierOrganisationID, -- FK: carrier org
    b.status,                                          -- Shipment status
    b.dateCreated,                                     -- Created date/time
    b.requestedTimeOfArrival,                          -- Requested arrival
    b.committedTimeOfArrival,                          -- Committed arrival
    b.actualShipDate,                                  -- Actual shipped date
    b.estimatedTimeOfArrival,                          -- Estimated arrival
    b.revisedEstimatedTimeOfArrival,                   -- Revised ETA
    b.predictedTimeOfArrival,                           -- Predicted ETA
    b.actualTimeOfArrival,                              -- Actual arrival time
    b.lineCount,                                       -- Number of lines in shipment
    COALESCE(b.weight, 0),                             -- Weight with null fallback
    b.weightUnits,                                     -- Weight units
    b.currentLocationCoordinates,                      -- Current GPS/location data
    b.currentRegion,                                   -- Current region of shipment
    b.transportMode,                                   -- Mode of transport
    b.houseAirwayBill,                                 -- House airway bill number
    b.parcelTrackingNumber,                            -- Parcel tracking number
    b.airwayMasterNumber,                              -- Master airway bill number
    b.billOfLadingNumber,                              -- Bill of lading number
    b.proNumber,                                       -- PRO number
    b.manifest,                                        -- Manifest identifier
    b.exclude,                                         -- Exclude flag
    ISNULL(NULLIF(b.sourceLink, ''), 'N/A')           -- Source system link, default N/A if blank
FROM BRONZE.shipment_v4 b
LEFT JOIN SILVER.location_v3 l_from                  -- Join "ship from" location, trimming whitespace
    ON LTRIM(RTRIM(b.shipFromLocationIdentifier)) = l_from.locationIdentifier

LEFT JOIN SILVER.location_v3 l_to                    -- Join "ship to" location, trimming whitespace
    ON LTRIM(RTRIM(b.shipToLocationIdentifier)) = l_to.locationIdentifier

LEFT JOIN SILVER.organisation_v3 o_vendor            -- Join vendor organisation, trimming whitespace
    ON LTRIM(RTRIM(b.vendorOrganizationIdentifier)) = o_vendor.organisationIdentifier

LEFT JOIN SILVER.organisation_v3 o_buyer             -- Join buyer organisation, trimming whitespace
    ON LTRIM(RTRIM(b.buyerOrganizationIdentifier)) = o_buyer.organisationIdentifier

LEFT JOIN SILVER.organisation_v3 o_carrier           -- Join carrier organisation, trimming whitespace
    ON LTRIM(RTRIM(b.carrierOrganizationIdentifier)) = o_carrier.organisationIdentifier

WHERE 
    l_from.locationID IS NOT NULL                     -- Only keep if "ship from" location found
    AND l_to.locationID IS NOT NULL                   -- Only keep if "ship to" location found
    AND o_vendor.organisationID IS NOT NULL           -- Only keep if vendor organisation found
    AND o_buyer.organisationID IS NOT NULL            -- Only keep if buyer organisation found
    AND o_carrier.organisationID IS NOT NULL;         -- Only keep if carrier organisation found

GO
-- Create SILVER.DimDate table
-- Create SILVER.DimDate table
CREATE TABLE SILVER.dimDate (
    dateID INT IDENTITY(1,1) PRIMARY KEY,           -- Surrogate key for each date (auto-increment)
    date DATE NOT NULL UNIQUE,                       -- Actual calendar date (unique)
    year INT NOT NULL,                               -- Year part of the date
    quarter INT NOT NULL,                            -- Quarter of the year (1 to 4)
    month INT NOT NULL,                              -- Month number (1 to 12)
    day INT NOT NULL,                                -- Day of the month (1 to 31)
    week INT NOT NULL,                               -- Week number in the year
    dayOfWeekName VARCHAR(20) NOT NULL,             -- Name of the day (e.g. Monday)
    isWeekend BIT NOT NULL,                          -- Flag if day is weekend (1 = yes, 0 = no)
    insertedAt DATETIME DEFAULT GETDATE(),          -- Timestamp when row inserted
    lastUpdatedAt DATETIME DEFAULT GETDATE()        -- Timestamp when row last updated
);