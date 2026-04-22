# tests/conftest.py
import sys
import types
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

# Stub out worker-only dependencies that are not installed locally.
# handler_video.py does `import runpod` and `import websocket` at module
# level, and calls `runpod.serverless.start(...)` at the bottom.
# Both deps are only available inside the RunPod Docker image.

_ws_mod = types.ModuleType("websocket")
sys.modules.setdefault("websocket", _ws_mod)

_runpod_mod = types.ModuleType("runpod")
_runpod_serverless = types.ModuleType("runpod.serverless")
_runpod_serverless.start = lambda *a, **kw: None  # no-op at import time
_runpod_mod.serverless = _runpod_serverless
sys.modules.setdefault("runpod", _runpod_mod)
sys.modules.setdefault("runpod.serverless", _runpod_serverless)
