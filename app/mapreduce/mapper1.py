
import re
import sys

# sys.stderr.write("Mapper1 started\n")
# sys.stderr.flush()

def tokenize(text):
    # Convert to lowercase and split into words
    words = re.findall(r'\w+', text.lower())
    return words


def main():
    LIMIT = 1000
    counter = 0
    processing = True

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        if processing:
            parts = line.split('\t', 2)
            if len(parts) != 3:
                continue

            doc_id, title, content = parts
            tokens = tokenize(content)
            doc_length = len(tokens)

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

            counter += 1
            if counter >= LIMIT:
                processing = False
        else:

            continue

if __name__ == "__main__":
    main()
    # sys.stderr.write("Mapper1 finished\n")
    sys.stderr.flush()

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


