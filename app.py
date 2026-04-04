"""Streamlit entrypoint for the refactored AI analytics agent."""

from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from ecommerce_ai_agent.ui import run_streamlit_app


run_streamlit_app()
