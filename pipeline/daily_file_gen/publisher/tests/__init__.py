import os
import sys

# Add the publisher directory to sys.path so `from app import ...` resolves.
_publisher_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _publisher_dir not in sys.path:
    sys.path.insert(0, _publisher_dir)

# Add the repo root so `from utilities.source_profile import ...` resolves
# (utilities/ is installed via pyproject.toml in the Docker image; not in the venv).
_repo_root = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "..")
)
if _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)
