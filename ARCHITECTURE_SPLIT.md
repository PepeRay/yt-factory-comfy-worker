# Arquitectura de 3 Endpoints — YouTube Factory

## Por qué el split

El handler monolítico carga TODOS los modelos (Flux, TTS, Whisper, video) en un solo worker.
Problema: cada cold start paga el costo de cargar modelos que no necesita. Un job de TTS no necesita Flux.

## Los 3 endpoints

| Endpoint | Job Types | Modelos | GPU recomendada | Dockerfile (CI/CD) | Dockerfile (base) |
|----------|-----------|---------|-----------------|--------------------|-------------------|
| **Audio** | `txt-voice`, `voice-srt` | Qwen3-TTS, CosyVoice3, Whisper | RTX 4000 Ada (20GB) | `Dockerfile.audio` | `Dockerfile.base.audio` |
| **Images** | `txt-img` | Flux 2 Klein, LoRAs | RTX 4000 Ada (20GB) | `Dockerfile.images` | `Dockerfile.base.images` |
| **Video** | `img-vid`, `compose` | Wan 2.2 I2V, FFmpeg | RTX 6000 Ada (48GB) | — (base-only) | `Dockerfile.base.video` |

## Estructura de archivos

```
serverless/
├── handler.py              # Monolítico (legacy, sigue funcionando)
├── handler_audio.py        # Solo audio jobs
├── handler_images.py       # Solo image jobs
├── handler_video.py        # Video + compose jobs
├── Dockerfile              # Monolítico (legacy)
├── Dockerfile.audio        # CI/CD layer audio (FROM base.audio)
├── Dockerfile.images       # CI/CD layer images (FROM base.images)
├── Dockerfile.base.audio   # Base audio con modelos (rebuild manual)
├── Dockerfile.base.images  # Base images con modelos (rebuild manual)
├── Dockerfile.base.video   # Base video con modelos Wan 2.2 (rebuild manual)
├── start.sh                # Compartido (parametrizable)
├── start_audio.sh          # Whitelist solo audio nodes
├── start_images.sh         # Sin whitelist (todo en Docker)
├── start_video.sh          # Whitelist solo video nodes
└── extra_model_paths.yaml  # Compartido
```

## RunPod: 3 endpoints independientes

Cada endpoint en RunPod console:
- **yt-factory-audio** → Docker image: `peperay/yt-factory-audio:latest`
- **yt-factory-images** → Docker image: `peperay/yt-factory-images:latest`
- **yt-factory-video** → Docker image: `peperay/yt-factory-video-base:v1` (base-only, sin CI/CD layer)

Post-migracion Phase 2: los modelos viven DENTRO de las imagenes base (no en Network Volume).
El Network Volume `29swi0udsr` fue eliminado — cold starts ahora dependen 100% del pull de imagen.

## Ahorro estimado

- Audio jobs: GPU más barata (~$0.19/hr vs $0.76/hr)
- Image jobs: GPU más barata (~$0.19/hr vs $0.76/hr)
- Video jobs: Mantiene RTX 6000 Ada ($0.76/hr) pero cold start más rápido
- Docker images más pequeñas → cold starts más rápidos → menos tiempo facturado

## API (sin cambios en el formato)

Cada endpoint recibe el mismo formato de payload:
```json
{
  "input": {
    "job_type": "txt-voice",
    "channel": "dominion",
    "content_id": "001_The_Global_Debt",
    "workflow": { ... },
    "prefix": "chunk",
    "index": 1
  }
}
```

La diferencia es que cada endpoint SOLO acepta sus job_types asignados.

## n8n: Orquestación

n8n llama al endpoint correcto según la fase del pipeline:
1. Audio pipeline → `yt-factory-audio` (txt-voice → voice-srt)
2. Image pipeline → `yt-factory-images` (txt-img × N escenas)
3. Video pipeline → `yt-factory-video` (img-vid × N escenas → compose)
