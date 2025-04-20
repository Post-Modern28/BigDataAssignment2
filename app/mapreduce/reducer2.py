#reducer2.py
#!/usr/bin/env python3
import sys
import math
from cassandra.cluster import Cluster

cluster = Cluster(['127.0.0.1'])
session = cluster.connect('search_engine')

doc_freq = {}
doc_count = set()

for line in sys.stdin:
    term, val = line.strip().split("\t")
    doc_id, tf, doc_len = val.split(",")

    tf = int(tf)
    doc_len = int(doc_len)

    # Store in inverted index
    session.execute("""
        INSERT INTO inverted_index (term, doc_id, tf, doc_len)
        VALUES (%s, %s, %s, %s)
    """, (term, doc_id, tf, doc_len))

    # Update document stats
    session.execute("""
        INSERT INTO doc_stats (doc_id, doc_len)
        VALUES (%s, %s)
    """, (doc_id, doc_len))

    # Track DF
    key = (term)
    doc_freq[key] = doc_freq.get(key, set())
    doc_freq[key].add(doc_id)
    doc_count.add(doc_id)

# Insert vocabulary with DF and IDF
N = len(doc_count)
for term in doc_freq:
    df = len(doc_freq[term])
    idf = math.log((N - df + 0.5) / (df + 0.5) + 1)

    session.execute("""
        INSERT INTO vocabulary (term, df, idf)
        VALUES (%s, %s, %s)
    """, (term, df, idf))

cluster.shutdown()
