"""
Put the repository root on sys.path so tests can import the source packages.
"""


import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))