import sys
import re
import math
from pyspark.sql import SparkSession
from cassandra.cluster import Cluster

# Init Spark
spark = SparkSession.builder.appName("BM25_Ranker").getOrCreate()
sc = spark.sparkContext

# Parse query
query = " ".join(sys.argv[1:]).lower()
words = re.findall(r'[a-z0-9]+', query)

# Connect DB
cluster = Cluster(['cassandra-server'], port=9042)
session = cluster.connect('search_engine')

# Get N
row = session.execute("SELECT value FROM global_stats WHERE key='TOTAL_DOCS'").one()
N = row.value if row else 0

# Get DLs
dl_rows = session.execute("SELECT doc_id, length FROM document_lengths")
doc_lengths = {r.doc_id: r.length for r in dl_rows}
dl_avg = sum(doc_lengths.values()) / N if N > 0 else 1

k1, b = 1.0, 0.75
scores = []

# Calculate partial BM25
for word in words:
    df_row = session.execute("SELECT df FROM document_frequencies WHERE term=%s", (word,)).one()
    if not df_row: 
        continue
    
    idf = math.log(N / df_row.df)
    tf_rows = session.execute("SELECT doc_id, tf FROM term_frequencies WHERE term=%s", (word,))
    
    for r in tf_rows:
        tf, doc_id = r.tf, r.doc_id
        dl = doc_lengths.get(doc_id, dl_avg)
        score = idf * ((tf * (k1 + 1)) / (tf + k1 * (1 - b + b * (dl / dl_avg))))
        scores.append((doc_id, score))

cluster.shutdown()

if not scores:
    print(f"\nNo results for: {query}\n")
    spark.stop()
    sys.exit(0)

# Aggregate scores via RDD
scores_rdd = sc.parallelize(scores)
total_scores = scores_rdd.reduceByKey(lambda x, y: x + y)

# Get Top 10
top_10 = total_scores.sortBy(lambda x: x[1], ascending=False).take(10)
top_10_ids = set([x[0] for x in top_10])

# Fetch titles from HDFS
def extract_metadata(line):
    parts = line.split('\t', 2)
    return (parts[0], parts[1]) if len(parts) >= 2 else (None, None)

docs_rdd = sc.textFile("hdfs:///input/data")
titles_rdd = docs_rdd.map(extract_metadata).filter(lambda x: x[0] in top_10_ids)
titles_map = dict(titles_rdd.collect())

# Output
print("\n" + "="*60)
print(f"Top 10 BM25 Results: '{query}'")
print("="*60)
for rank, (doc_id, score) in enumerate(top_10, 1):
    title = titles_map.get(doc_id, "Unknown").replace("_", " ")
    print(f"{rank}. [Score: {score:.4f}] ID: {doc_id} | Title: {title}")
print("="*60 + "\n")

spark.stop()
