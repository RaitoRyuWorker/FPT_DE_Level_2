# Data Engineering Exercise: Customer Transaction Pipeline

## Exercise Overview

**Level 2 Data Engineer (Middle) Exercise**

## My Approach

### 1. Generate Realistic Test Data
I create synthetic data that mirrors real-world problems:
- Invalid/missing email addresses
- Inconsistent product category naming
- Invalid transaction dates
- Duplicate records

This ensures my ETL pipeline is tested against realistic scenarios.

### 2. Build a Robust ETL Pipeline
My pipeline handles data quality issues through:
- **Extract**: Load CSV files with error handling
- **Transform**: Clean and validate data (emails, dates, categories)
- **Load**: Store clean data in SQLite database with proper constraints

### 3. Simple Quality Monitoring
I use a straightforward validation approach:
- Track row counts at each stage
- Compare transformed vs loaded data
- Show PASS/FAIL status for each dataset
- Immediately indicate if the pipeline is broken

### 4. Meet Specific Requirements
I focus on exactly what's needed:
- Create customer revenue table with total transaction amounts per customer
- Provide simple verification that everything worked
- Keep the code clean and maintainable

## Key Benefits

- **Realistic Testing**: Data mirrors production issues
- **Simple Validation**: Clear PASS/FAIL pipeline checks
- **Focused Implementation**: Each component does exactly what's required
- **Easy Maintenance**: Clean, modular design

## Project Files

- `generate_data.py` - Creates realistic test data with quality issues
- `etl_pipeline.py` - Main ETL pipeline with simple validation
- `create_customer_revenue_table.py` - Creates customer revenue table
- `query_database.py` - Basic database queries for verification