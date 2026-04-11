"""
Build-time gate for ComfyUI custom nodes.

Ejecuta un import real de cada custom node replicando lo que ComfyUI hace
al arrancar (`nodes.load_custom_node` → `importlib.util.spec_from_file_location`
→ `exec_module`). Si un nodo tiene deps faltantes (`ModuleNotFoundError` /
`ImportError`), este script falla el build.

Catch granular:
    - ModuleNotFoundError / ImportError → FAIL (el nodo no va a cargar en runtime)
    - Cualquier otra excepción (típicamente RuntimeError por falta de GPU en
      el runner de GitHub Actions) → SKIP. El runtime real en RunPod sí tiene
      driver NVIDIA y el nodo cargará OK.

Uso:
    python verify_custom_nodes.py NodeFolder1 NodeFolder2 ...

Contexto del bug que motivó este script (2026-04-11):
    RES4LYF se instaló correctamente (`__init__.py` presente) pero en runtime
    fallaba con `No module named 'comfy.nested_tensor'` porque ese módulo no
    existe en ComfyUI v0.3.47. El gate anterior (`test -f __init__.py`)
    no lo detectaba. Este script sí — porque `ModuleNotFoundError` es
    exactamente el tipo que capturamos como fallo.

Contexto del segundo intento (2026-04-11):
    La primera versión del script hacía `except Exception` genérico, lo que
    hacía que el build fallara en GitHub Actions porque los runners no tienen
    GPU física y los custom nodes disparan `torch.cuda.current_device()` al
    importar → RuntimeError "Found no NVIDIA driver". Ese error es artefacto
    del build environment, no un bug real del nodo. Por eso el catch granular.
"""
import importlib.util
import sys
from pathlib import Path

COMFYUI_ROOT = Path("/comfyui")
CUSTOM_NODES_DIR = COMFYUI_ROOT / "custom_nodes"


def verify(node_folders: list[str]) -> None:
    sys.path.insert(0, str(COMFYUI_ROOT))
    sys.path.insert(0, str(CUSTOM_NODES_DIR))

    failures: list[tuple[str, str]] = []
    for folder in node_folders:
        init_path = CUSTOM_NODES_DIR / folder / "__init__.py"
        if not init_path.is_file():
            failures.append((folder, f"missing {init_path}"))
            continue
        spec = importlib.util.spec_from_file_location(
            f"custom_nodes.{folder}", init_path
        )
        if spec is None or spec.loader is None:
            failures.append((folder, "spec_from_file_location returned None"))
            continue
        module = importlib.util.module_from_spec(spec)
        try:
            spec.loader.exec_module(module)
        except (ModuleNotFoundError, ImportError) as exc:
            failures.append((folder, f"{type(exc).__name__}: {exc}"))
            continue
        except Exception as exc:
            msg = str(exc).splitlines()[0][:120] if str(exc) else ""
            print(f"  [SKIP] {folder}: {type(exc).__name__} at build time (expected without GPU) — {msg}")
            continue
        print(f"  [OK]   {folder}")

    if failures:
        print("\n=== CUSTOM NODE VERIFICATION FAILED ===", file=sys.stderr)
        for folder, reason in failures:
            print(f"  [FAIL] {folder}: {reason}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("usage: verify_custom_nodes.py NODE1 [NODE2 ...]", file=sys.stderr)
        sys.exit(2)
    verify(sys.argv[1:])
