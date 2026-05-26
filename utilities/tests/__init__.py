import os
import sys

# Add the repo root so `from utilities.source_profile import ...` resolves.
_repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)
