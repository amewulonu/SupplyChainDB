# Capstone Project: Supply Chain Data Analysis

🔍 Overview
The SupplyChain project  analyses supply chain data to uncover insights into supply planning, shipment trends, and logistical performance. It encompasses full-cycle data engineering steps, from ingestion to optimisation, utilising MySQL. 
________________________________________
## Project Workflow
1. Data Ingestion
•	Source: Provided CSV files
•	Tools: MySQL Workbench, Medallion Architecture (Bronze → Silver → Gold)
•	Task: Load raw CSVs into BRONZE schema staging tables.

2. Data Cleaning & Transformation
•	Handle missing values (e.g., impute weight using average values).
•	Standardise and enrich location data.
•	Convert date/time formats.
•	Resolve data inconsistencies.
•	Create DimDate from relevant timestamps.
3. Data Modelling (Star Schema)
•	Fact Tables:
o	ShipmentFacts
o	SupplyPlanFacts
•	Dimension Tables:
o	DimProduct
o	DimLocation
o	DimOrganization
o	DimDate
•	Design Tasks:
o	Create ER diagram
o	Normalise data
o	Build SILVER schema with surrogate keys

4. Load Data into Star Schema
•	Populate GOLD tables from SILVER layer using SQL transformation scripts.

5. Advanced SQL Queries
•	Stored Procedure: Total planned quantity per location over the date range.
•	CTE: Top 5 vendors by shipment count in a region.
•	Subquery: Shipments with above-average weight.
•	Window Function: Rolling avg. shipment weight by carrier.

6. MySQL Optimisation
•	Add indexes on keys and frequently queried fields.
•	Consider partitioning ShipmentFacts by date for large volumes.
•	Use EXPLAIN to troubleshoot and optimise slow queries.
________________________________________
##  Key Design Decisions
🔸 Why This Schema?
The star schema improves query performance and simplifies reporting by separating facts (measurable events) from dimensions (contextual data).
🔸 Handling Slowly Changing Dimensions
Use Type 2 SCD for tracking historical changes (e.g., carrier renames), preserving records with effective dates.
🔸 SQL Techniques for Missing Data
•	COALESCE to handle NULLs
•	Derived values via averages or lookups from similar entries
🔸 Data Consistency Strategy
•	Use JOINs to cross-verify records
•	Standardise identifiers and formats
•	Deduplicate where necessary
________________________________________
💡 Interview-Style Q&A
Q: What's the difference between a CTE and a subquery?
A: CTEs improve readability and reusability; subqueries are nested within a main query. Use CTEs when logic must be reused or layered.
Q: How do window functions enhance performance?
A: They avoid unnecessary GROUP BY operations and let you calculate aggregates without collapsing rows.
Q: Benefits of Table Partitioning?
A: Speeds up queries on date ranges and large datasets by scanning only relevant partitions.
Q: Troubleshooting slow queries?
A: Use EXPLAIN, check index usage, avoid SELECT *, and monitor for expensive joins or subqueries.
________________________________________
To do… Repository Structure
graphql
CopyEdit
📁 SupplyChain-Capstone
├── 📂 data            # Raw CSVs
├── 📂 sql_scripts     # All DDL/DML scripts
├── 📂 diagrams        # ERD and schema visualisations
├── 📂 reports         # Query results & insights
└── 📄 README.md       # Project summary and key insights


## further questions:
## Questions for this Capstone Project

### Why did you choose a particular schema for this project? Explain your rationale for selecting the fact and dimension tables.
The star schema was chosen for this project because it provides a clear and efficient structure for analytical queries in a supply chain context. It separates measurable facts (like shipments and supply plans) from descriptive dimensions (like product, location, date, and organisations), which simplifies querying, improves performance, and aligns well with data warehousing best practices.

This simplifies getting answers to complex business questions such as:
Using this schema, we can answer strategic business questions such as:

1. Which regions experience the highest shipping delays?

2. What is the average lead time from vendor to buyer by product category?

3. How has the planned inventory volume changed over different planning cycles?

4. What is the on-time delivery performance of each carrier over the last 6 months?

### How would you handle changes in dimension tables over time (e.g., a carrier changes its name)? Discuss strategies for handling slowly changing dimensions.
To handle changes in dimension tables over time.
I would implement Slowly Changing Dimension (SCD) Type 2. This preserves historical data by creating a new record with the updated information while retaining the original record. This approach ensures accurate historical reporting. Additional fields, such as EffectiveDate, EndDate, and IsCurrent, help track changes over time and maintain data integrity for time-based analyses.


### What SQL techniques did you use to handle missing data?
In this project, I used several SQL techniques to handle missing data, ensuring data quality and consistency:

1. LTRIM(RTRIM(...)) - To remove unwanted leading and trailing spaces before checking for nulls or blanks.

2. NULLIF(..., '') - To convert empty strings to NULL for uniformity.

3. ISNULL(..., default_value) - To replace NULL with appropriate default values like 'UNKNOWN', 'N/A', or 0.00.

4. CAST(... AS VARCHAR(MAX)) - To ensure a consistent datatype for string fields before transformations.

These techniques collectively cleaned and standardised the data for loading into dimension and fact tables.

### How did you identify and handle inconsistencies between the two datasets?
To identify inconsistencies;
1.  I performed cross-checks using JOIN operations between shared fields (e.g., locationIdentifier, organisationIdentifier, partNumber) across datasets. 
2. I used LEFT JOIN with IS NULL filters to spot mismatches. 
3. Then, I cleaned the data by trimming spaces (LTRIM(RTRIM(...))), handling casing inconsistencies, standardising formats, and replacing blanks with NULLIF and ISNULL defaults. 
This ensured referential integrity between fact and dimension tables.
