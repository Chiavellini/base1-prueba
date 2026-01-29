<context>
The current database architecture is WRONG. We have 155 separate tables (one per ticker). 

The CORRECT architecture should be:
- ONE single table called `market_data.closing_prices`
- Rows = dates (one row per trading day)
- Columns = tickers (AAPL, MSFT, GOOGL, ... ~155 columns)
- Cell values = closing price for that ticker on that date

Example of correct structure:
| date       | AAPL   | MSFT   | GOOGL  | NVDA   | ... |
|------------|--------|--------|--------|--------|-----|
| 2025-01-26 | 255.65 | 420.30 | 175.50 | 140.25 | ... |
| 2025-01-25 | 254.00 | 419.00 | 174.80 | 139.50 | ... |
</context>

<task>
We need to rebuild the data ingestion. Before writing code, analyze what needs to change:

1. **Show current ticker list** (we need these as column names):
   head -20 data/info/tickers_list.csv

2. **Count total tickers**:
   wc -l data/info/tickers_list.csv

3. **Show current shared module structure** (this needs to be rewritten):
   head -60 plugins/market_data_shared.py

4. **Outline the new architecture**:
   Provide a detailed plan for:
   - New table DDL (CREATE TABLE with date + 155 ticker columns)
   - Modified upsert function (fetch all tickers, pivot to wide format, insert row)
   - How the 30-minute update will work (UPDATE the current day's row)
</task>

<output_format>
## Analysis

### Current Tickers
[List first 20 and total count]

### New Table DDL
```sql
CREATE TABLE market_data.closing_prices (
    date DATE PRIMARY KEY,
    aapl NUMERIC(18,6),
    msft NUMERIC(18,6),
    ...
);
```

### Migration Plan
1. [Step 1]
2. [Step 2]
3. [Step 3]

### Key Code Changes Needed
- [ ] plugins/market_data_shared.py — new upsert function
- [ ] dags/market_data/create_tables.py — new DDL
- [ ] dags/market_data/ingest.py — fetch all tickers, pivot, insert
- [ ] dags/market_data/intraday_update.py — update current day's row

### Questions Before Proceeding
[Any clarifications needed]
</output_format>

Do NOT write any code yet. Just provide the analysis and plan.