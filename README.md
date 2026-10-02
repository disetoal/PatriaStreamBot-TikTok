# PatriaStreamBot TikTok Bridge — Northflank

Servicio Node.js persistente para detectar TikTok LIVE y enviar eventos al Worker de PatriaStreamBot.

## Variables de entorno obligatorias

- `PATRIABOT_URL=https://patria-stream-bot.disetoal.workers.dev`
- `TIKTOK_BRIDGE_TOKEN=<mismo token guardado en Cloudflare>`

## Variables recomendadas

- `DEPLOYMENT_LABEL=northflank`
- `HOST_LABEL=Northflank`
- `POLL_SECONDS=30`
- `SYNC_SECONDS=60`
- `SAMPLE_SECONDS=30`
- `CHECK_CONCURRENCY=5`
- `PORT=8788`

## Northflank

1. Crea un proyecto en Developer Sandbox.
2. Crea un **Combined Service** desde un repositorio GitHub/GitLab/Bitbucket que contenga esta carpeta.
3. Build type: **Dockerfile**.
4. Si el repo contiene más carpetas, usa esta carpeta como build context y `northflank-tiktok/Dockerfile` como Dockerfile.
5. Añade las variables anteriores en Runtime Environment.
6. Usa 1 instancia y el plan gratuito disponible en tu Sandbox.
7. No necesitas un puerto público. El proceso solo realiza conexiones salientes a TikTok y Cloudflare.

El contenedor expone `/health` en el puerto 8788 para comprobaciones internas.
