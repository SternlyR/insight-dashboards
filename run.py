#!/usr/bin/env python3
import sys, uvicorn
uvicorn.run("dashboards.server:app", host="0.0.0.0", port=8000, reload="--reload" in sys.argv)
