import sys
import math

def main():
    doc_lengths = {}
    total_docs = 0
    total_length = 0
    lines = []

    # First pass: collect document statistics and store all input
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        parts = line.split('\t')
        lines.append(parts)
        if parts[0] == "DOC":
            doc_id, _, length, _ = parts[1:]
            doc_lengths[doc_id] = int(length)
            total_docs += 1
            total_length += int(length)

    # Calculate average document length
    avg_doc_length = total_length / total_docs if total_docs > 0 else 0

    # Second pass: calculate and emit BM25 scores
    for parts in lines:
        if parts[0] == "INDEX":
            term, doc_id, freq, _ = parts[1:]
            doc_length = doc_lengths.get(doc_id, 0)

            # BM25 parameters
            k1 = 1.2
            b = 0.75
            idf = math.log((total_docs - 1 + 0.5) / (1 + 0.5))  # TODO: change at real IDF
            tf = int(freq)
            score = idf * (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * (doc_length / avg_doc_length)))

            print(f"{term}\t{doc_id}\t{score}")

if __name__ == "__main__":
    main()
