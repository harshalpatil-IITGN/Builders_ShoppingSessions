"""
Timestamp parser strictly conforming to rules.md:
Three valid forms:
F1: 2026-10-03T23:58:10Z      (UTC, whole seconds)
F2: 2026-10-03T19:58:10-04:00 (Offset +HH:MM or -HH:MM, subtract offset to get UTC)
F3: 1791071890000             (ASCII digits only, epoch milliseconds, leading zeros allowed)

Returns epoch milliseconds (int) or None if invalid.
"""
import calendar
from typing import Optional

# Days in months for non-leap years
DAYS_IN_MONTH = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]

def is_leap_year(year: int) -> bool:
    return year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)

def parse_iso_datetime(s: str) -> Optional[int]:
    # Form F1: len == 20, ending in 'Z'
    # Form F2: len == 25, offset +HH:MM or -HH:MM
    # Form: YYYY-MM-DDTHH:MM:SS...
    n = len(s)
    if n != 20 and n != 25:
        return None
    
    # Check delimiters
    if s[4] != '-' or s[7] != '-' or s[10] != 'T' or s[13] != ':' or s[16] != ':':
        return None
    
    # Must have only ASCII digits in numeric positions
    for idx in (0, 1, 2, 3, 5, 6, 8, 9, 11, 12, 14, 15, 17, 18):
        c = ord(s[idx])
        if c < 48 or c > 57: # '0'-'9'
            return None
            
    year = int(s[0:4])
    month = int(s[5:7])
    day = int(s[8:10])
    hour = int(s[11:13])
    minute = int(s[14:16])
    second = int(s[17:19])
    
    # Validation
    if month < 1 or month > 12:
        return None
    
    max_days = DAYS_IN_MONTH[month]
    if month == 2 and is_leap_year(year):
        max_days = 29
    if day < 1 or day > max_days:
        return None
        
    if hour > 23 or minute > 59 or second > 59:
        return None
        
    # Check timezone part
    offset_seconds = 0
    if n == 20:
        if s[19] != 'Z':
            return None
    else: # n == 25
        sign = s[19]
        if sign != '+' and sign != '-':
            return None
        if s[22] != ':':
            return None
        for idx in (20, 21, 23, 24):
            c = ord(s[idx])
            if c < 48 or c > 57:
                return None
        off_h = int(s[20:22])
        off_m = int(s[23:25])
        if off_h > 23 or off_m > 59:
            return None
        offset_seconds = off_h * 3600 + off_m * 60
        if sign == '+':
            offset_seconds = offset_seconds
        else:
            offset_seconds = -offset_seconds

    # Compute UTC epoch seconds using calendar.timegm
    # timegm takes (year, month, day, hour, minute, second)
    try:
        epoch_sec = calendar.timegm((year, month, day, hour, minute, second))
    except (ValueError, OverflowError):
        return None
        
    utc_epoch_sec = epoch_sec - offset_seconds
    return utc_epoch_sec * 1000


def parse_timestamp(s: Optional[str]) -> Optional[int]:
    """
    Parses F1, F2, F3 format.
    Returns epoch milliseconds as int, or None if invalid.
    """
    if not s:
        return None
    
    # Check if F3 (all ASCII digits)
    # Digits only: epoch milliseconds. Leading zeros allowed.
    is_all_ascii_digits = True
    for ch in s:
        c = ord(ch)
        if c < 48 or c > 57:
            is_all_ascii_digits = False
            break
            
    if is_all_ascii_digits:
        try:
            return int(s)
        except (ValueError, OverflowError):
            return None
            
    # Try ISO formats F1 / F2
    return parse_iso_datetime(s)
