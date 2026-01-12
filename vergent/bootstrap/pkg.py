# vergent/scan.py
import importlib
import pkgutil
from functools import wraps
from typing import Callable


def scan(package: str):
    """
    Decorator that triggers a component scan when the decorated function
    is imported.
    """
    def decorator(func: Callable):

        @wraps(func)
        def wrapper(*args, **kwargs):
            # Execute the scan BEFORE calling the function
            py_package = importlib.import_module(package)

            for module_info in pkgutil.iter_modules(py_package.__path__):
                module_name = f"{package}.{module_info.name}"
                importlib.import_module(module_name)

            # The function itself is usually a no-op, but we call it anyway
            return func(*args, **kwargs)

        return wrapper

    return decorator
