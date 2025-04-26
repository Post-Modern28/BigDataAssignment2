#!/usr/bin/env python3
import sys


def main():
    current_term = None
    doc_freq = 0

    for line in sys.stdin:
        parts = line.strip().split('\t')

        # Handle document statistics
        if parts[0] == "STATS":
            doc_id, title, doc_length, avg_term_freq = parts[1:]
            print(f"DOC\t{doc_id}\t{title}\t{doc_length}\t{avg_term_freq}")
            continue

        # Handle term frequencies
        term, doc_id, freq, positions = parts

        if term != current_term:
            if current_term is not None:
                # Emit vocabulary for previous term
                print(f"VOCAB\t{current_term}\t{doc_freq}")
            current_term = term
            doc_freq = 0

        doc_freq += 1

        # Emit inverted index entry
        print(f"INDEX\t{term}\t{doc_id}\t{freq}\t{positions}")

    # Emit vocabulary for last term
    if current_term is not None:
        print(f"VOCAB\t{current_term}\t{doc_freq}")


if __name__ == "__main__":
    main()