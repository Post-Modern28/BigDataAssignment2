#!/usr/bin/env python3
from cassandra.cluster import Cluster

def main():
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
            doc_length int
        );
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS inverted_index (
            term text,
            doc_id text,
            term_frequency int,
            PRIMARY KEY (term, doc_id)
        );
    """)


    print("Keyspace and tables created successfully!")

    insert_vocab = session.prepare("INSERT INTO vocabulary (term, document_frequency, total_occurrences) VALUES (?, ?, ?)")
    insert_docstat = session.prepare("INSERT INTO document_stats (doc_id, doc_length) VALUES (?, ?)")
    insert_index = session.prepare("INSERT INTO inverted_index (term, doc_id, term_frequency) VALUES (?, ?, ?)")


    print("Inserting data into Cassandra...")

    with open("output_stage2.txt", "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split("\t")
            if parts[0] == "VOCAB":
                _, term, df, total_occurrences = parts
                session.execute(insert_vocab, (term, int(df), int(total_occurrences)))
            elif parts[0] == "DOCSTAT":
                _, doc_id, doc_length = parts
                session.execute(insert_docstat, (doc_id, int(doc_length)))
            elif parts[0] == "INDEX":
                _, term, doc_id, tf = parts
                session.execute(insert_index, (term, doc_id, int(tf)))


    print("Data inserted successfully!")

if __name__ == "__main__":
    main()
