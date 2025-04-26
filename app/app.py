#!/usr/bin/env python3
from cassandra.cluster import Cluster

cluster = Cluster(["cassandra-server"])
session = cluster.connect()

# Create keyspace and tables
print("Creating keyspace and tables...")

session.execute("""
    CREATE KEYSPACE IF NOT EXISTS search_index
    WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 1};
""")

session.execute("USE search_index;")

session.execute("""
    CREATE TABLE IF NOT EXISTS vocabulary (
        term text PRIMARY KEY,
        document_frequency int,
        total_occurrences bigint
    );
""")

session.execute("""
    CREATE TABLE IF NOT EXISTS document_stats (
        doc_id text PRIMARY KEY,
        title text,
        doc_length int,
        avg_term_frequency double
    );
""")

session.execute("""
    CREATE TABLE IF NOT EXISTS inverted_index (
        term text,
        doc_id text,
        term_frequency int,
        positions list<int>,
        PRIMARY KEY (term, doc_id)
    );
""")

session.execute("""
    CREATE TABLE IF NOT EXISTS bm25_scores (
        term text,
        doc_id text,
        score double,
        PRIMARY KEY (term, doc_id)
    );
""")

print("Keyspace and tables created successfully!")


print("Inserting BM25 scores into bm25_scores table...")

insert_stmt = session.prepare("""
    INSERT INTO bm25_scores (term, doc_id, score) VALUES (?, ?, ?)
""")

with open("output_stage2.txt", "r", encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        parts = line.split("\t")
        if len(parts) != 3:
            continue
        term, doc_id, score = parts
        score = float(score)
        session.execute(insert_stmt, (term, doc_id, score))

print("BM25 scores inserted successfully!")

# session.shutdown()
