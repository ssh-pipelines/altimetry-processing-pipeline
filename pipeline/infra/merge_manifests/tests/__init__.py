import os
import sys

os.environ.setdefault("AWS_REGION", "us-west-2")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-west-2")

# Add the Lambda directory to sys.path so `import app` resolves.
_module_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _module_dir not in sys.path:
    sys.path.insert(0, _module_dir)
