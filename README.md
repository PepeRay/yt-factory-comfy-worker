# YouTube Factory — ComfyUI Serverless Worker

Worker de RunPod Serverless para la YouTube Factory.
Extiende blib-la/runpod-worker-comfy para soportar audio + video.

## Build & Deploy

### 1. Build local (necesitas Docker)
```bash
docker build -t raymundo/yt-factory-comfy:latest .
```

### 2. Push a DockerHub
```bash
docker login
docker push raymundo/yt-factory-comfy:latest
```

### 3. Crear endpoint en RunPod
- Template: Custom
- Docker Image: `raymundo/yt-factory-comfy:latest`
- Network Volume: `29swi0udsr` (ComfyUI Audio, 150GB)
- GPU: 48GB+ (RTX A6000, L40, A100)
- Min Workers: 0, Max Workers: 3
- Idle Timeout: 5s
- Execution Timeout: 600s (10 min para video largo)

### 4. Probar
```bash
curl -X POST https://api.runpod.ai/v2/{ENDPOINT_ID}/runsync \
  -H "Authorization: Bearer {RUNPOD_API_KEY}" \
  -H "Content-Type: application/json" \
  -d @test_workflow.json
```

## API

### Input
```json
{
  "input": {
    "workflow": { ... ComfyUI workflow JSON ... },
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

### Recuperar archivos grandes (video) via S3
```bash
aws s3 cp --region us-il-1 \
  --endpoint-url https://s3api-us-il-1.runpod.io \
  s3://29swi0udsr/jobs/{JOB_ID}/video/output.mp4 ./
```

## Network Volume Structure
```
/runpod-volume/
  ComfyUI/           ← ComfyUI completo con modelos
    models/           ← Todos los modelos (Flux, LTX, VibeVoice, Whisper)
    custom_nodes/     ← Nodos custom (se sincronizan al arranque)
    input/            ← Archivos de referencia (Voz_Dominion.wav, etc.)
    output/           ← Output default de ComfyUI
  jobs/               ← Outputs organizados por job_id (creado por handler)
    {job_id}/
      images/
      audio/
      video/
```
