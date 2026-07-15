import os
import sys

# Ensure the utilities package (installed via pyproject.toml in Docker) is importable
# during local development by adding the repo root to sys.path.
_repo_root = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "..")
)
if _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)

# Mirror the Dockerfile PYTHONPATH so `from config.source_config import ...` resolves.
_finalization_dir = os.path.join(os.path.dirname(__file__), "..", "finalization")
_finalization_dir = os.path.abspath(_finalization_dir)
if _finalization_dir not in sys.path:
    sys.path.insert(0, _finalization_dir)
