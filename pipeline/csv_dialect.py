"""
CSV parsing utilities strictly following rules.md dialect:
- Comma-separated, UTF-8, \n line endings.
- Only \n ends a line: a \r is an ordinary character of the field it is in!
- Any field may be wrapped in double quotes: quotes are syntax, not part of the value.
  "u1" -> u1, "" -> null.
- No value ever holds a comma, quote, or newline, quoted or not.
- Quotes anywhere except around a whole field makes the line malformed -> drop line.
- Empty field means null (represented as None).
- Empty line is malformed.
- Line with wrong number of fields is malformed -> drop line.
- Spaces are part of value: " u1" != "u1".
- Digits means ASCII '0'-'9'.
"""
from typing import List, Optional


def parse_csv_line(raw_line: bytes, expected_fields: int) -> Optional[List[Optional[str]]]:
    """
    Parses a single raw byte line ending with \n.
    Returns a list of Optional[str] of length expected_fields, or None if malformed.
    """
    if not raw_line or raw_line == b"\n":
        return None
    if raw_line.endswith(b"\n"):
        raw_line = raw_line[:-1]
    
    parts = raw_line.split(b",")
    if len(parts) != expected_fields:
        return None
    
    row: List[Optional[str]] = []
    for p in parts:
        if len(p) >= 2 and p.startswith(b'"') and p.endswith(b'"'):
            inner = p[1:-1]
            if b'"' in inner:
                return None
            val = inner
        else:
            if b'"' in p:
                return None
            val = p
        
        if len(val) == 0:
            row.append(None)
        else:
            try:
                row.append(val.decode("utf-8"))
            except UnicodeDecodeError:
                return None
                
    return row
