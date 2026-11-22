"""
Identity resolution using Disjoint Set Union (Union-Find).
Follows rules.md:
1. Parse: drop malformed lines (wrong number of fields, quotes inside field, etc.)
   and lines where either field is empty.
2. Join: transitive union of user IDs.
3. Name each person by the byte-wise smallest of their IDs.
   An ID that appears in no link is named by itself.
"""
import glob
import os
from typing import Dict, Optional, Tuple
from pipeline.csv_dialect import parse_csv_line


class UnionFind:
    def __init__(self):
        # parent points to the parent in the tree
        self.parent: Dict[str, str] = {}
        # min_id tracks the byte-wise smallest ID in the component
        self.min_id: Dict[str, str] = {}

    def find(self, x: str) -> str:
        # Path compression
        root = x
        while self.parent.get(root, root) != root:
            root = self.parent[root]
        curr = x
        while curr != root:
            nxt = self.parent.get(curr, curr)
            self.parent[curr] = root
            curr = nxt
        return root

    def union(self, a: str, b: str):
        if a not in self.parent:
            self.parent[a] = a
            self.min_id[a] = a
        if b not in self.parent:
            self.parent[b] = b
            self.min_id[b] = b

        root_a = self.find(a)
        root_b = self.find(b)

        if root_a != root_b:
            # Determine which root has the byte-wise smallest canonical ID
            canonical = min(self.min_id[root_a], self.min_id[root_b])
            self.parent[root_b] = root_a
            self.min_id[root_a] = canonical

    def get_canonical(self, x: str) -> str:
        if x not in self.parent:
            return x
        root = self.find(x)
        return self.min_id[root]


def build_identity_mapping(data_dir: str) -> UnionFind:
    """
    Reads all links/*.csv in data_dir, skips headers, parses lines,
    and returns a UnionFind structure.
    """
    uf = UnionFind()
    pattern = os.path.join(data_dir, "links", "*.csv")
    link_files = sorted(glob.glob(pattern))
    
    for file_path in link_files:
        with open(file_path, "rb") as f:
            header = f.readline()  # skip header row
            for line in f:
                row = parse_csv_line(line, 2)
                if row is not None:
                    u_a, u_b = row[0], row[1]
                    # Drop lines where either field is empty/null
                    if u_a is None or u_b is None:
                        continue
                    uf.union(u_a, u_b)
                    
    return uf
