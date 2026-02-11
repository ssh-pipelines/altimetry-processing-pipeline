import sys
import os

# Add the unifier directory to sys.path so `from config.source_config import ...` resolves.
_unifier_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _unifier_dir not in sys.path:
    sys.path.insert(0, _unifier_dir)
