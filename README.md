# PatriaStreamBot Bridge v0.9

Servicio Node para Render que ejecuta dos motores en el mismo contenedor:

- TikTok Live Connector para detección LIVE/viewers.
- Discord Bot para aprobar compras por reacción y enviarlas al dashboard.

## Variables de entorno

- `PATRIABOT_URL`: URL del Worker de Cloudflare.
- `TIKTOK_BRIDGE_TOKEN`: secreto compartido Render ↔ Cloudflare.
- `DISCORD_BOT_TOKEN`: token privado del bot Discord.
- `DEPLOYMENT_LABEL=render`
- `HOST_LABEL=Render`
- `POLL_SECONDS=60`
- `SYNC_SECONDS=60`
- `SAMPLE_SECONDS=60`
- `CHECK_CONCURRENCY=2`

Render asigna `PORT` automáticamente.

## Discord

El bot requiere estos Gateway Intents:

- Guilds
- Guild Messages
- Guild Message Reactions
- Message Content

En Discord Developer Portal activa **Message Content Intent**.

Permisos recomendados en el servidor:

- View Channels
- Read Message History
- Add Reactions

La configuración del canal, emoji y modo de aprobación se descarga desde Cloudflare cada minuto.

## Compras

En el canal configurado, cuando alguien reacciona con el emoji de aprobación, el bot:

1. Obtiene el mensaje/embed.
2. Comprueba que sea una solicitud de compra PatriaCraft.
3. Extrae Producto, Tipo, Duración, Cantidad, Precio total, Minecraft y Discord.
4. Envía la aprobación al Worker.
5. Cloudflare registra la compra una sola vez usando el ID del mensaje.
6. El bot añade `💰` al mensaje cuando ya está contabilizado.
