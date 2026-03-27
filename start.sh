#!/usr/bin/env bash
set -e
streamlit run app.py --server.address 0.0.0.0 --server.port "${PORT:-8501}"
