import pymysql
import pandas as pd

db = pymysql.connect(unix_socket="/tmp/mysql.sock", user='nicholashoffman', database='gcd_paleofire_in_progress')
cursor = db.cursor()

cursor.execute("""
SELECT TABLE_NAME, COLUMN_NAME, COLUMN_TYPE 
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_SCHEMA = 'gcd_paleofire_in_progress';
""")

rows = []
for table, column, type in cursor.fetchall():
    try:
        cursor.execute(f"SELECT `{column}` FROM `{table}` WHERE `{column}` IS NOT NULL LIMIT 3;")
        examples = [str(r[0]) for r in cursor.fetchall()]
        rows.append((table, column,type, ', '.join(examples)))
    except Exception as e:
        rows.append((table, column,type, f"(error: {e})"))

df = pd.DataFrame(rows, columns=['Table', 'Column', 'Type','Example_Values'])
df.to_csv('schema_with_examples.csv', index=False)
