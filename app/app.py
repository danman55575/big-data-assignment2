import sys
from cassandra.cluster import Cluster

print("Connecting to Cassandra/ScyllaDB...")
# Connect to the database container
cluster = Cluster(['cassandra-server'], port=9042)
session = cluster.connect()

print("Creating Keyspace and Tables...")
# Create Keyspace
session.execute("""
    CREATE KEYSPACE IF NOT EXISTS search_engine 
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
""")
session.set_keyspace('search_engine')

# Create Tables for BM25 components
session.execute("CREATE TABLE IF NOT EXISTS global_stats (key text PRIMARY KEY, value int)")
session.execute("CREATE TABLE IF NOT EXISTS document_lengths (doc_id text PRIMARY KEY, length int)")
session.execute("CREATE TABLE IF NOT EXISTS document_frequencies (term text PRIMARY KEY, df int)")
session.execute("CREATE TABLE IF NOT EXISTS term_frequencies (term text, doc_id text, tf int, PRIMARY KEY (term, doc_id))")

# Prepare statements for fast async execution
insert_global = session.prepare("INSERT INTO global_stats (key, value) VALUES (?, ?)")
insert_dl = session.prepare("INSERT INTO document_lengths (doc_id, length) VALUES (?, ?)")
insert_df = session.prepare("INSERT INTO document_frequencies (term, df) VALUES (?, ?)")
insert_tf = session.prepare("INSERT INTO term_frequencies (term, doc_id, tf) VALUES (?, ?, ?)")

print("Parsing index and loading data into Database...")
# Read the aggregated local file
count = 0
with open("local_index.txt", "r", encoding="utf-8") as f:
    for line in f:
        parts = line.strip().split('\t')
        if len(parts) != 2:
            continue
            
        key_part, value_part = parts
        val = int(value_part)

        # Route data to correct tables based on prefix
        if key_part == "TOTAL_DOCS":
            session.execute(insert_global, (key_part, val))
        elif key_part.startswith("DL:"):
            doc_id = key_part[3:]
            # Changed to execute_async
            session.execute_async(insert_dl, (doc_id, val))
        elif key_part.startswith("DF:"):
            term = key_part[3:]
            # Changed to execute_async
            session.execute_async(insert_df, (term, val))
        elif key_part.startswith("TF:"):
            content = key_part[3:]
            term, doc_id = content.split('@', 1)
            # Changed to execute_async
            session.execute_async(insert_tf, (term, doc_id, val))
        
        count += 1
        if count % 50000 == 0:
            print(f"Processed {count} records...")

print("Data successfully loaded into Cassandra!")
cluster.shutdown()
