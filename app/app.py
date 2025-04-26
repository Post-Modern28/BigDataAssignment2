#!/usr/bin/env python3
from cassandra.cluster import Cluster

cluster = Cluster(["cassandra-server"])
session = cluster.connect()


# Create keyspace and tables
print("Creating keyspace and tables...")

# Create keyspace
session.execute("""
    CREATE KEYSPACE IF NOT EXISTS search_index
    WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 1};
""")

# Use the keyspace
session.execute("USE search_index;")

# Create tables
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

# # Close the connection
# session.shutdown()