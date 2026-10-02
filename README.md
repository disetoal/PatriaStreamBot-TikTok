# PatriaStreamBot TikTok Bridge — Render Free

Web Service Node.js para detectar TikTok LIVE y enviar eventos al Worker de PatriaStreamBot.

## Despliegue recomendado

1. Sube el contenido de esta carpeta a la raíz de tu repositorio GitHub `PatriaStreamBot-TikTok`.
2. En Render crea un **Blueprint** desde ese repositorio (Render leerá `render.yaml`).
3. Cuando Render pida `TIKTOK_BRIDGE_TOKEN`, pega el token generado por `PREPARAR-TOKEN-RENDER.bat`.
4. El plan definido es `free` y el health check es `/health`.

El Worker de Cloudflare recibe el `RENDER_EXTERNAL_HOSTNAME` por heartbeat y comprueba periódicamente `/health`. El bridge mantiene la detección TikTok mientras el servicio está activo.

## Variables

- `PATRIABOT_URL=https://patria-stream-bot.disetoal.workers.dev`
- `TIKTOK_BRIDGE_TOKEN=<mismo token guardado en Cloudflare>`
- `DEPLOYMENT_LABEL=render`
- `HOST_LABEL=Render`
- `POLL_SECONDS=60`
- `SYNC_SECONDS=60`
- `SAMPLE_SECONDS=60`
- `CHECK_CONCURRENCY=2`

Render define automáticamente `PORT` y `RENDER_EXTERNAL_HOSTNAME`.
