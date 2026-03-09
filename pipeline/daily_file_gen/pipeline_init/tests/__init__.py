import sys
import os

# Ensure the utilities package and pipeline_init module are importable
# during local development by adding the repo root and module dir to sys.path.
_repo_root = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "..")
)
if _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)

_module_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _module_dir not in sys.path:
    sys.path.insert(0, _module_dir)
