import os
import sys

# Ensure the utilities package (installed via setup.py in Docker) is importable
# during local development by adding the repo root to sys.path.
_repo_root = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "..")
)
if _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)

# Mirror the Dockerfile PYTHONPATH so `import summarizer` / `import app` resolve:
# the lambda dir (parent of this tests/ dir) holds both.
_lambda_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _lambda_dir not in sys.path:
    sys.path.insert(0, _lambda_dir)
