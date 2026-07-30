"""
Consistent hashing utilities for Chord.
"""
import hashlib

M = 160 # Number of bits in the identifier space. Ring size = 2**M.


def chord_hash(key, m=M):
    """Hash an arbitrary string (node address or username) into the ring space [0, 2**m)."""
    hashed_string = hashlib.sha1(key.encode("utf-8")).digest()
    full_int = int.from_bytes(hashed_string, byteorder="big")
    return full_int % (2 ** m)


def short_id(id_):
    """Last 6 hex digits of an identifier. Cosmetic only: m=160 ids are unreadable in full."""
    return format(id_, "x")[-6:] if id_ is not None else "None"


def in_interval(x, start, end, m=M, inclusive_end= False):
    """
    Check if x is in the circular interval (start, end] (or (start, end) if inclusive_end=False)
    on a ring of size 2**m. This handles wraparound.
    """
    ring_size = 2 ** m
    start %= ring_size
    end %= ring_size
    x %= ring_size

    if start == end:
        # full circle case - everything is "in" the interval except the boundary node itself
        return True if inclusive_end else x != start

    if start < end:
        if inclusive_end:
            return start < x <= end
        return start < x < end
    else:
        # zero is in the interval
        if inclusive_end:
            return x > start or x <= end
        return x > start or x < end
