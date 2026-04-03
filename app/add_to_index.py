import sys
import re
from collections import Counter
from cassandra.cluster import Cluster

# Read local file
filepath = sys.argv[1]
filename = filepath.split('/')[-1].replace('.txt', '')
doc_id = filename.split('_')[0]

with open(filepath, 'r', encoding='utf-8') as f:
    text = f.read().lower()

words = re.findall(r'[a-z0-9]+', text)
dl = len(words)
tf_counts = Counter(words)

# DB Update
cluster = Cluster(['cassandra-server'], port=9042)
session = cluster.connect('search_engine')

row = session.execute("SELECT value FROM global_stats WHERE key='TOTAL_DOCS'").one()
session.execute("INSERT INTO global_stats (key, value) VALUES ('TOTAL_DOCS', %s)", (row.value + 1 if row else 1,))
session.execute("INSERT INTO document_lengths (doc_id, length) VALUES (%s, %s)", (doc_id, dl))

for word, tf in tf_counts.items():
    row = session.execute("SELECT df FROM document_frequencies WHERE term=%s", (word,)).one()
    new_df = (row.df + 1) if row else 1
    session.execute("INSERT INTO document_frequencies (term, df) VALUES (%s, %s)", (word, new_df))
    session.execute("INSERT INTO term_frequencies (term, doc_id, tf) VALUES (%s, %s, %s)", (word, doc_id, tf))

print(f"File {filename} indexed in ScyllaDB successfully.")
cluster.shutdown()
