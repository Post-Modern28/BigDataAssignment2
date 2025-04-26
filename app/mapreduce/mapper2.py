#!/usr/bin/env python3
import sys


def main():
    for line in sys.stdin:
        term, doc_id, tf, doc_len = line.strip().split("\t")
        print(f"{term}\t{doc_id},{tf},{doc_len}")


if __name__ == "__main__":
    main()