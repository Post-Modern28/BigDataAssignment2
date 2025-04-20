#include mapper1.py
#!/usr/bin/env python3
import sys
import re

WORD_RE = re.compile(r"\w+")

for line in sys.stdin:
    parts = line.strip().split("\t")
    if len(parts) != 3:
        continue
    doc_id, title, content = parts
    words = WORD_RE.findall(content.lower())
    doc_len = len(words)
    freq = {}
    for word in words:
        freq[word] = freq.get(word, 0) + 1
    for word, count in freq.items():
        print(f"{word}\t{doc_id}\t{count}\t{doc_len}")
