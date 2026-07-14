import sys
import os

# Add the unifier directory to sys.path so `from config.source_config import ...` resolves.
_unifier_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _unifier_dir not in sys.path:
    sys.path.insert(0, _unifier_dir)

# Add the repo root so `from utilities.source_profile import ...` resolves
# (utilities/ is installed via pyproject.toml in the Docker image; not in the venv).
_repo_root = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "..")
)
if _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)
