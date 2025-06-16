# SupplyChainDB
SupplyChainDB project analyzes supply chain data to improve planning, shipment tracking, and logistics. Importing CSV data, designing a star schema with fact and dimension tables, cleaning and transforming data, and loading into MySQL. Advanced SQL stored procedures, CTEs, subqueries, and window functions, optimization enhances performance and insights.

## Questions for the Capstone Project (Add as README on your Github)

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
