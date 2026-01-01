import pytest

from vergent.core.model.token_ import Token
from vergent.core.space import HashSpace


@pytest.mark.ut
def test_hash_returns_128bit_integer():
    h = HashSpace.hash("hello".encode())
    assert isinstance(h, Token)
    assert 0 <= h.value < HashSpace.MAX


@pytest.mark.ut
def test_hash_is_deterministic():
    assert HashSpace.hash("key".encode()) == HashSpace.hash("key".encode())


@pytest.mark.ut
def test_add_without_wrap():
    assert HashSpace.add(Token(10), 5) == Token(15)


@pytest.mark.ut
def test_add_with_wrap():
    x = HashSpace.MAX - 3
    assert HashSpace.add(Token(x), 10) == Token(7)  # wrap-around expected


@pytest.mark.ut
def test_in_interval_normal():
    # interval (10, 20]
    assert HashSpace.in_interval(Token(15), Token(10), Token(20)) is True
    assert HashSpace.in_interval(Token(10), Token(10), Token(20)) is False
    assert HashSpace.in_interval(Token(21), Token(10), Token(20)) is False


@pytest.mark.ut
def test_in_interval_wrap():
    # interval (250, 20] wraps around
    assert HashSpace.in_interval(Token(300), Token(250), Token(20)) is True   # in upper segment
    assert HashSpace.in_interval(Token(10), Token(250), Token(20)) is True    # in lower segment
    assert HashSpace.in_interval(Token(200), Token(250), Token(20)) is False  # outside


@pytest.mark.ut
def test_position_is_deterministic():
    p1 = HashSpace.token("label-A", 3)
    p2 = HashSpace.token("label-A", 3)
    assert p1 == p2


@pytest.mark.ut
def test_position_changes_with_index():
    p1 = HashSpace.token("label-A", 1)
    p2 = HashSpace.token("label-A", 2)
    assert p1 != p2


@pytest.mark.ut
def test_generate_positions_count():
    positions = HashSpace.generate_tokens("label-A", 5)
    assert len(list(positions)) == 5


@pytest.mark.ut
def test_generate_positions_are_unique_for_same_label():
    positions = HashSpace.generate_tokens("label-A", 10)
    assert len(set(positions)) == 10


@pytest.mark.ut
def test_generate_positions_different_labels():
    p1 = HashSpace.generate_tokens("label-A", 3)
    p2 = HashSpace.generate_tokens("label-B", 3)
    assert p1 != p2
