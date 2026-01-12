from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Token:
    """
    Token represents a 128-bit position on the hash ring.

    It wraps an integer in the range [0, 2^128) and provides
    convenient formatting helpers (short, long, hex).
    """

    value: int

    _MAX = 2**128

    def __post_init__(self):
        if not (0 <= self.value < self._MAX):
            raise ValueError("Token must be a 128-bit integer")

    def short(self, length: int = 12) -> str:
        """Return a short hexadecimal representation."""
        return f"{self.value:032x}"[:length]

    def long(self) -> str:
        """Return the full 128-bit hexadecimal representation."""
        return f"{self.value:032x}"

    def __int__(self) -> int:
        return self.value

    def __eq__(self, other) -> bool:
        return isinstance(other, (Token, int)) and self.value == other.value

    def __lt__(self, other) -> bool:
        if not isinstance(other, (Token, int)):
            raise TypeError("'other' argument must be a Token or int")
        return self.value < other.value

    def __le__(self, other) -> bool:
        if  not isinstance(other, (Token, int)):
            raise TypeError("'other' argument must be a Token or int")
        return self.value <= other.value

    def __gt__(self, other) -> bool:
        if not isinstance(other, (Token, int)):
            raise TypeError("'other' argument must be a Token or int")
        return self.value > other.value

    def __ge__(self, other) -> bool:
        if  not isinstance(other, (Token, int)):
            raise TypeError("'other' argument must be a Token or int")
        return self.value >= other.value

    def __hash__(self) -> int:
        return hash(self.value)

    def __repr__(self) -> str:
        val = str(self.value)
        if len(val) >= 12:
            val = f"{val[:6]}…{val[-6:]}"
        return f"Token(value={val}, hex={self.short()})"
