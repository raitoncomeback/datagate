import duckdb

conn = duckdb.connect('data/duckdb/datagate.db')

tables = conn.execute("""
    SELECT table_name, table_schema, table_type
    FROM information_schema.tables
    WHERE table_schema IN ('main_silver', 'main_gold', 'main')
    AND table_type IN ('VIEW', 'BASE TABLE')
    ORDER BY table_schema, table_name
""").fetchall()

print("=== Tables/Views in DuckDB ===")
for t in tables:
    print(f"  {t[1]}.{t[0]} ({t[2]})")

print()
print("=== mart_ticker_intelligence row count ===")
count = conn.execute("SELECT COUNT(*) FROM main_gold.mart_ticker_intelligence").fetchone()
print(f"  {count[0]} rows")

print()
print("=== Sample mart row ===")
row = conn.execute("""
    SELECT ticker, date, close, market_sentiment,
           stocks_trust_score, advisor_can_serve
    FROM main_gold.mart_ticker_intelligence
    LIMIT 1
""").fetchone()
print(f"  {row}")

conn.close()