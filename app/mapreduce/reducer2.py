#!/usr/bin/env python3
import sys
import math
# from cassandra.cluster import Cluster


def main():
    # Initialize document statistics
    total_docs = 0
    total_length = 0
    doc_lengths = {}

    # First pass: collect document statistics
    for line in sys.stdin:
        parts = line.strip().split('\t')
        if parts[0] == "DOC":
            doc_id, _, length, _ = parts[1:]
            doc_lengths[doc_id] = int(length)
            total_docs += 1
            total_length += int(length)

    # Calculate average document length
    avg_doc_length = total_length / total_docs if total_docs > 0 else 0

    # Second pass: calculate and emit BM25 scores
    for line in sys.stdin:
        parts = line.strip().split('\t')
        if parts[0] == "INDEX":
            term, doc_id, freq, _ = parts[1:]
            doc_length = doc_lengths.get(doc_id, 0)

            # Calculate BM25 score
            k1 = 1.2
            b = 0.75
            idf = math.log((total_docs - 1 + 0.5) / (1 + 0.5))  # Simplified IDF
            tf = int(freq)
            score = idf * (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * (doc_length / avg_doc_length)))

            # Emit BM25 score
            print(f"{term}\t{doc_id}\t{score}")


if __name__ == "__main__":
    main()

# cluster = Cluster(['127.0.0.1'])
# session = cluster.connect('search_engine')
#
# doc_freq = {}
# doc_count = set()
#
# for line in sys.stdin:
#     term, val = line.strip().split("\t")
#     doc_id, tf, doc_len = val.split(",")
#
#     tf = int(tf)
#     doc_len = int(doc_len)
#
#     # Store in inverted index
#     session.execute("""
#         INSERT INTO inverted_index (term, doc_id, tf, doc_len)
#         VALUES (%s, %s, %s, %s)
#     """, (term, doc_id, tf, doc_len))
#
#     # Update document stats
#     session.execute("""
#         INSERT INTO doc_stats (doc_id, doc_len)
#         VALUES (%s, %s)
#     """, (doc_id, doc_len))
#
#     # Track DF
#     key = (term)
#     doc_freq[key] = doc_freq.get(key, set())
#     doc_freq[key].add(doc_id)
#     doc_count.add(doc_id)
#
# # Insert vocabulary with DF and IDF
# N = len(doc_count)
# for term in doc_freq:
#     df = len(doc_freq[term])
#     idf = math.log((N - df + 0.5) / (df + 0.5) + 1)
#
#     session.execute("""
#         INSERT INTO vocabulary (term, df, idf)
#         VALUES (%s, %s, %s)
#     """, (term, df, idf))
#
# cluster.shutdown()
