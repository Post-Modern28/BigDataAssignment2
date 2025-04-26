#!/usr/bin/env python3

import re
import sys

# sys.stderr.write("Mapper1 started\n")
# sys.stderr.flush()

def tokenize(text):
    # Convert to lowercase and split into words
    words = re.findall(r'\w+', text.lower())
    return words


def main():
    # Limit the number of documents to process
    LIMIT = 100
    counter = 0

    for line in sys.stdin:
        if counter >= LIMIT:
            break
        line = line.strip()
        parts = line.split('\t', 2)
        if len(parts) != 3:
            # print(f"Skipping invalid line: {line}", file=sys.stderr)
            continue  # или можно log'нуть: print(f"Skipping invalid line: {line}", file=sys.stderr)

        doc_id, title, content = line.strip().split('\t', 2)

        # Tokenize content
        tokens = tokenize(content)

        # Calculate document length
        doc_length = len(tokens)

        # Calculate term frequencies and positions
        term_freq = {}
        term_positions = {}
        for pos, term in enumerate(tokens):
            if term not in term_freq:
                term_freq[term] = 0
                term_positions[term] = []
            term_freq[term] += 1
            term_positions[term].append(pos)

        # Emit document statistics
        print(f"STATS\t{doc_id}\t{title}\t{doc_length}\t{sum(term_freq.values()) / len(term_freq) if term_freq else 0}")

        # Emit term frequencies and positions
        for term, freq in term_freq.items():
            positions = term_positions[term]
            print(f"{term}\t{doc_id}\t{freq}\t{','.join(map(str, positions))}")

        # Increment the counter
        counter += 1


if __name__ == "__main__":
    main()
    # sys.stderr.write("Mapper1 finished\n")
    # sys.stderr.flush()

# import sys
# import re
# import os
# print("ENV:", os.environ)
#
#
# WORD_RE = re.compile(r"\w+")
#
# for line in sys.stdin:
#     parts = line.strip().split("\t")
#     if len(parts) != 3:
#         continue
#     doc_id, title, content = parts
#     words = WORD_RE.findall(content.lower())
#     doc_len = len(words)
#     freq = {}
#     for word in words:
#         freq[word] = freq.get(word, 0) + 1
#     for word, count in freq.items():
#         print(f"{word}\t{doc_id}\t{count}\t{doc_len}")


