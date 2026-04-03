import os
import re
import shutil
from pyspark.sql import SparkSession

# Delete old local folder data if exists
if os.path.exists("data"):
    shutil.rmtree("data")
os.makedirs("data", exist_ok=True)

# Initialize SparkSession
spark = SparkSession.builder \
    .appName('Data Preparation RDD') \
    .master("local[*]") \
    .config("spark.sql.parquet.enableVectorizedReader", "true") \
    .getOrCreate()

sc = spark.sparkContext

print("Reading parquet file...")
df = spark.read.parquet("hdfs:///z.parquet")

total_count = df.count()
n = 1500
df_sampled = df.select(['id', 'title', 'text']).sample(fraction=min(1.0, (2 * n) / total_count), seed=42).limit(n)

print("Generate text files...")
def create_doc(row):
    # replace space with '_' and delete everything but english letters, digits and '_'
    raw_title = str(row['title']).replace(" ", "_")
    safe_title = re.sub(r'[^A-Za-z0-9_]', '', raw_title)
    
    filename = f"data/{row['id']}_{safe_title}.txt"
    clean_text = str(row['text']).replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
    
    with open(filename, "w", encoding="utf-8") as f:
        f.write(clean_text)

# save files locally
df_sampled.foreach(create_doc)

print("Transform through RDD and saving to /input/data...")
rdd = sc.wholeTextFiles("file://" + os.path.abspath("data") + "/*.txt")

def process_file(record):
    filepath, content = record
    filename = filepath.split('/')[-1].replace('.txt', '')
    parts = filename.split('_', 1)
    doc_id = parts[0]
    doc_title = parts[1] if len(parts) > 1 else "Unknown"
    
    return f"{doc_id}\t{doc_title}\t{content}"

formatted_rdd = rdd.map(process_file).coalesce(1)

# Clean old directory in HDFS
URI = sc._gateway.jvm.java.net.URI
Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
fs = FileSystem.get(URI("hdfs:///"), sc._jsc.hadoopConfiguration())
if fs.exists(Path("/input/data")):
    fs.delete(Path("/input/data"), True)

formatted_rdd.saveAsTextFile("hdfs:///input/data")

print("Data are successfully prepared and saved to HDFS.")
spark.stop()
