#mapper2.py
#!/usr/bin/env python3
import sys

for line in sys.stdin:
    term, doc_id, tf, doc_len = line.strip().split("\t")
    print(f"{term}\t{doc_id},{tf},{doc_len}")
