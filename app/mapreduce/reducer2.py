import sys
import math
from collections import defaultdict

def main():
    doc_lengths = {}
    total_docs = 0
    total_length = 0
    term_doc_freq = defaultdict(set)
    term_total_freq = defaultdict(int)
    index_entries = []

    # First pass: read all input
    for line in sys.stdin:
        parts = line.strip().split('\t')
        if not parts:
            continue

        if parts[0] == "DOC":
            if len(parts) < 5:
                continue  # Skip bad line
            _, doc_id, title, doc_length, avg_tf = parts
            try:
                doc_lengths[doc_id] = int(doc_length)
                total_docs += 1
                total_length += int(doc_length)
                print(f"DOCSTAT\t{doc_id}\t{doc_lengths[doc_id]}")
            except ValueError:
                continue

        elif parts[0] == "INDEX":
            if len(parts) < 5:
                continue
            _, term, doc_id, freq, positions = parts
            try:
                freq = int(freq)
                term_doc_freq[term].add(doc_id)
                term_total_freq[term] += freq
                index_entries.append((term, doc_id, freq))
                # Immediately emit INDEX
                print(f"INDEX\t{term}\t{doc_id}\t{freq}")
            except ValueError:
                continue

        elif parts[0] == "VOCAB":
            if len(parts) < 3:
                continue
            _, term, df = parts
            try:
                df = int(df)
                pass
            except ValueError:
                continue

    if total_docs == 0:
        print("No documents found!", file=sys.stderr)
        return

    avg_doc_length = total_length / total_docs

    # Emit corrected VOCAB with term stats
    for term in sorted(term_doc_freq.keys()):
        df = len(term_doc_freq[term])
        total_occurrences = term_total_freq[term]
        print(f"VOCAB\t{term}\t{df}\t{total_occurrences}")


if __name__ == "__main__":
    main()
