"""
python -m demo.muestra
"""

# %%
from scripts import actidb 

QUERY = "SELECT * FROM muestra.nvda nvda ORDER BY nvda.date;"
nvda = actidb.fetch(QUERY)
print(nvda)