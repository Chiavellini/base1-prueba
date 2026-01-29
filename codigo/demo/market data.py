# %%
from scripts import actidb 
QUERY = "SELECT * FROM market_data.nvda nvda ORDER BY nvda.ts;"
nvda = actidb.fetch(QUERY)
print(nvda)