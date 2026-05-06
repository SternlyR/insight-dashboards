#!/usr/bin/env python3
import sys, uvicorn
uvicorn.run("dashboards.server:app", host="0.0.0.0", port=8001, reload="--reload" in sys.argv)
