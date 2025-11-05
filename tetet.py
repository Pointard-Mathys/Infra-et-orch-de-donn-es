import happybase

conn = happybase.Connection('localhost')  # ou l'IP du container HBase
conn.open()

table = conn.table('weapons_results')

for key, data in table.scan(limit=10):
    print(key, data)
