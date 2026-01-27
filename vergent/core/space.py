import hashlib
import secrets
from typing import Generator


class HashSpace:
    """
    HashSpace defines a fixed 128-bit circular keyspace used for consistent hashing.

    This type provides:
    - deterministic hashing into the 128-bit space
    - circular arithmetic modulo 2^128
    - interval comparison on a wrap-around ring
    - generation of deterministic tokens in the space

    HashSpace does not know anything about nodes, vnodes, or cluster topology.
    It only models the mathematical properties of the keyspace.
    Higher-level components (Ring, VNode, Partitioner) build on top of it.
    """

    MAX = 2**128

    @staticmethod
    def hash(value: bytes) -> int:
        """
        Returns a Token representing a 128-bit hash of the input bytes.

        MD5 is used here purely as a deterministic 128-bit mapping function.
        No cryptographic guarantees are assumed at this layer.
        """
        h = hashlib.md5(value).digest()
        return int.from_bytes(h, "big")

    @classmethod
    def add(cls, x: int, delta: int) -> int:
        """
        Performs circular addition modulo 2^128.

        This is useful for computing offsets or distances in the ring.
        """
        return (x + delta) % cls.MAX

    @staticmethod
    def in_interval(x: int, a: int, b: int) -> bool:
        """
        Reports whether x ∈ (a, b] on a circular ring.

        Normal interval:
            a < b  →  a < x <= b

        Wrapped interval (ring wrap-around):
            a > b  →  x > a or x <= b
        """
        if a < b:
            return a < x <= b
        else:
            return x > a or x <= b

    @classmethod
    def token(cls, label: str, index: int) -> int:
        """
        Returns a deterministic Token derived from (label, index).

        The label has no semantic meaning at this level; it is simply a namespace
        used to generate reproducible token values.
        """
        return cls.hash(f"{label}-{index}".encode())

    @classmethod
    def random_token(cls, label: str) -> int:
        """
        Generates a uniformly distributed token in the 128‑bit hash space by hashing
        (label, random_index).

        We use a 64‑bit random index for the following reasons:

        - 64 bits provide ~1.8e19 possible values, which makes collisions
          astronomically unlikely even with hundreds of thousands of VNodes.
        - The final hash (MD5/SHA/xxHash) fully diffuses these 64 bits into the
          128‑bit token space, ensuring uniform distribution.
        - Using 128 bits of randomness would not improve uniformity in practice,
          but would increase entropy generation cost and storage overhead.
        - 32 bits would be too small: with many VNodes, the birthday paradox would
          make collisions more likely.

        In short: 64 bits is the optimal trade‑off between uniformity, safety,
        performance, and practicality.
        """
        random_index = secrets.randbits(64)
        return cls.token(label, random_index)

    @classmethod
    def generate_tokens(cls, label: str, count: int) -> Generator[int, None, None]:
        """
        Yields 'count' deterministic Tokens in the 128-bit keyspace.

        Higher-level components decide whether these tokens represent vnodes,
        shards, or any other logical entity.
        """
        for i in range(count):
            yield cls.token(label, i)
