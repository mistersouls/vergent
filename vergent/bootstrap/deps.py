import functools

from vergent.core.storage import LMDBStorage
from pathlib import Path

from vergent.core.types_ import Storage



@functools.lru_cache
def get_storage() -> Storage:
    base_dir = Path(__file__).resolve().parents[2]
    db_path = base_dir / "db" / "db1"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return LMDBStorage(str(db_path))
