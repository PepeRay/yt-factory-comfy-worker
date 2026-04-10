# YouTube Factory — ComfyUI Serverless Worker

Worker de RunPod Serverless para la YouTube Factory.
Custom build con soporte para audio (Qwen3-TTS, CosyVoice3, Whisper), imagen (Flux 2 Klein) y video (Wan 2.2).

## Arquitectura

```
GitHub (PepeRay/yt-factory-comfy-worker)
  → RunPod auto-build on push to main
    → Docker image pushed to RunPod registry
      → Workers pull image on startup
        → start.sh: link Network Volume + launch ComfyUI + handler
```

## Credenciales y Endpoints

> **API keys y secrets:** `.claude/settings.local.json` (raíz del vault)
> **Referencia completa:** `Automatización Youtube/.claude/credentials.md`

- **GitHub repo**: `PepeRay/yt-factory-comfy-worker`
- **RunPod Endpoint ID**: `94qcu1n3xnl03o`
- **Network Volume ID**: `29swi0udsr` (ComfyUI Audio, 150GB, US-IL-1)
- **Console URL**: https://console.runpod.io/serverless/user/endpoint/94qcu1n3xnl03o

### API Domains (IMPORTANTE)

| Propósito | Dominio | Ejemplo |
|-----------|---------|---------|
| REST API (jobs) | `api.runpod.ai` | `https://api.runpod.ai/v2/94qcu1n3xnl03o/run` |
| GraphQL (management) | `api.runpod.io` | `https://api.runpod.io/graphql?api_key=...` |

**NO usar `api.runpod.io` para REST** — devuelve 404 silencioso.

## Endpoint Config

- GPU: RTX 6000 Ada (48GB VRAM, 16 vCPUs, 94GB RAM)
- Min Workers: 0 (Flex — scale to zero)
- Max Workers: 3
- Idle Timeout: 5s
- Execution Timeout: 600s (10 min)
- Container Disk: 20GB

## Deploy

Push to `main` → RunPod auto-builds. Para verificar qué imagen está activa:

```bash
# Verificar imagen deployada
curl -s -X POST "https://api.runpod.io/graphql?api_key=API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"query":"{ myself { endpoints { id template { imageName } } } }"}'

# Health check
curl -s -H "Authorization: Bearer API_KEY" \
  "https://api.runpod.ai/v2/94qcu1n3xnl03o/health"
```

## Test

```bash
# Async job
curl -X POST "https://api.runpod.ai/v2/94qcu1n3xnl03o/run" \
  -H "Authorization: Bearer API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"input": {"workflow": <WORKFLOW_JSON>}}'

# Check status
curl -H "Authorization: Bearer API_KEY" \
  "https://api.runpod.ai/v2/94qcu1n3xnl03o/status/JOB_ID"
```

Test workflows disponibles:
- `test_flux_simple.json` — Solo nodos core de ComfyUI (Flux 2 Klein)

## Archivos

| Archivo | Propósito |
|---------|-----------|
| `Dockerfile` | Build completo: ComfyUI + custom nodes + deps + handler |
| `handler.py` | RunPod handler: WS → queue prompt → collect outputs |
| `start.sh` | Startup: link Network Volume → ComfyUI → handler |
| `extra_model_paths.yaml` | Mapeo de modelos a Network Volume |
| `test_flux_simple.json` | Workflow de test (13 nodos core) |

## Estructura del Dockerfile

```
1. Base: nvidia/cuda:12.6.3-cudnn-runtime-ubuntu24.04
2. System deps (python, git, ffmpeg, etc.)
3. uv + venv
4. ComfyUI via comfy-cli
5. PyTorch cu126 (force-reinstall)
6. ComfyUI requirements (minus torch)
7. Custom nodes (comfy registry + git clone)
8. Pre-install deps de Network Volume nodes (VibeVoice, TTS-Audio-Suite, RES4LYF)
9. Restore: force-reinstall PyTorch + numpy
10. Restore: re-install ComfyUI deps
11. Install comfy-aimdo (VRAM allocator, separado de ComfyUI)
12. Verification layer (import test)
13. Handler (runpod, requests, websocket-client)
14. COPY handler.py + start.sh
```

## Network Volume

```
/runpod-volume/
  ComfyUI/
    models/           ← Legacy (post-migracion Phase 2 los modelos viven dentro de las imagenes base)
    custom_nodes/     ← Solo whitelisted: VibeVoice, TTS-Audio-Suite, RES4LYF
    input/            ← Archivos de referencia (Voz_Dominion.wav, etc.)
    output/           ← Output default de ComfyUI
  jobs/               ← Outputs por job_id (creado por handler)
```

### Reglas de Network Volume

- **Modelos**: Siempre en Network Volume (demasiado grandes para imagen)
- **Custom nodes**: Whitelisted only (VibeVoice, TTS-Audio-Suite, RES4LYF)
- **NUNCA pip install en runtime** — destruye el entorno Python
- **NUNCA linkear todos los nodos** — solo los del whitelist

## API Input/Output

### Input
```json
{
  "input": {
    "workflow": { ... ComfyUI API format ... },
    "input_images": { "scene_01.png": "<base64>" },
    "input_audio": { "voice.wav": "<base64>" }
  }
}
```

### Output
```json
{
  "output": {
    "images": [{ "filename": "...", "base64": "...", "s3_key": "..." }],
    "audio": [{ "filename": "...", "base64": "...", "s3_key": "..." }],
    "video": [{ "filename": "...", "s3_key": "jobs/{id}/video/..." }],
    "text": [{ "content": "SRT content here..." }]
  },
  "summary": { "images": 1, "audio": 1, "video": 0, "text": 1 }
}
```

## Lecciones Aprendidas

1. **`api.runpod.ai` NO `api.runpod.io`** para REST API
2. **comfy-aimdo** es paquete pip separado que ComfyUI importa pero no declara en requirements.txt
3. **--force-reinstall de PyTorch** borra comfy-aimdo, torchsde, y otros paquetes dependientes
4. **Runtime pip installs** destruyen el entorno (numpy downgrade 2.4→2.2 rompe todo)
5. **--no-deps** no previene downgrades directos, solo sub-dependencias
6. **Whitelist de nodos** es obligatorio — linkear todo inyecta deps que destrozan el env
7. **Capa de verificación** en Dockerfile previene deployar imágenes rotas (13 imports)
8. **Build timeout** de RunPod es ~30 min — imagen actual tarda ~22 min
9. **`uv pip install -r requirements.txt`** NO restaura todo después de force-reinstall — uv considera paquetes "satisfechos" aunque estén rotos
10. **Paquetes fantasma de ComfyUI** que requieren install explícito: comfy-aimdo, torchsde, comfyui-frontend-package, comfy-kitchen, blake3
