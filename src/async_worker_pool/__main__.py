"""python -m async_worker_pool → ``awpool`` CLI."""
import sys

from .cli import main

if __name__ == "__main__":
    sys.exit(main())