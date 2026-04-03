import sys
import re
from collections import Counter

# Process each line from HDFS input
for line in sys.stdin:
    parts = line.strip().split('\t')
    
    # Ensure correct format: id, title, text
    if len(parts) != 3:
        continue
        
    doc_id, title, text = parts
    
    # Tokenize text by lowercase alphanumeric only
    words = re.findall(r'[a-z0-9]+', text.lower())
    doc_length = len(words)
    
    if doc_length == 0:
        continue
        
    # Emit Document Length (DL)
    print(f"DL:{doc_id}\t{doc_length}")
    
    # Emit global doc count to compute N
    print("TOTAL_DOCS\t1")
    
    # In-mapper aggregation for Term Frequency (TF)
    word_counts = Counter(words)
    
    for word, count in word_counts.items():
        # Emit Term Frequency (TF) per document
        print(f"TF:{word}@{doc_id}\t{count}")
        # Emit Document Frequency (DF) per unique word
        print(f"DF:{word}\t1")
