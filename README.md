# Discord bot that reads alerts from your channels, has Claude work out why they happen from your Grafana logs, and suggests a fix.

Watches the channels you list. For each new message/embed it opens a thread, lets Claude
query your Loki logs through Grafana, explains **why** it is happening, and suggests one fix
as text. **v1 is read-only: the bot never connects to your machines and never runs anything.**

## Setup

Never commit `.env` or paste tokens into chat; `.env` is already in `.gitignore`.

### 1. Discord bot

1. https://discord.com/developers/applications -> New Application. Name, description, icon and
   banner are optional and cosmetic. Ignore "Application Test Mode" (that is for Embedded Apps).
2. **Bot** page: click **Reset Token**, copy it (shown once) -> `DISCORD_TOKEN`.
   Turn on **Message Content Intent** and save.
3. **OAuth2 -> URL Generator**: scope `bot`, permissions View Channels, Read Message History,
   Send Messages, Create Public Threads, Send Messages in Threads, Embed Links.
   Open the generated URL to add the bot to your server.
4. Turn on Developer Mode (Settings -> Advanced), right-click each alert channel ->
   **Copy Channel ID** -> `ALERT_CHANNEL_IDS` (comma-separated).

### 2. Grafana (Loki)

1. **Administration -> Users and access -> Service accounts** -> Add service account, role **Viewer**.
2. On it, **Add service account token** -> Generate, copy it (shown once) -> `GRAFANA_TOKEN`.
3. `GRAFANA_URL` is your Grafana address, e.g. `http://10.13.0.20:3000`. Use an address the
   container can reach: not `localhost`.
4. `LOKI_DATASOURCE_UID`: **Connections -> Data sources -> Loki**, the UID is the last part of the
   page URL (`.../connections/datasources/edit/<uid>`). Or list them:
   ```bash
   curl -H "Authorization: Bearer <token>" <GRAFANA_URL>/api/datasources
   ```
   and copy the `uid` of the entry with `"type":"loki"` (it can be simply `loki`).
5. Check the connection (a JSON list of labels = working, 401 = bad token, 404 = bad UID):
   ```bash
   curl -H "Authorization: Bearer <token>" <GRAFANA_URL>/api/datasources/proxy/uid/<uid>/loki/api/v1/labels
   ```

### 3. Anthropic API key

https://console.anthropic.com -> **Settings -> API Keys** -> Create Key (starts with `sk-ant-`, shown
once) -> `ANTHROPIC_API_KEY`. The API needs credit (Settings -> Billing); a Claude.ai subscription does
not cover it. Set a monthly spend limit under Settings -> Limits.

### 4. Run it

On the home server (it must be able to reach `GRAFANA_URL`), fill in `.env` with no quotes around values:
```bash
cp .env.example .env
docker compose up -d --build
docker compose logs -f
```
Test: post a message like "nginx is down" in an alert channel; the bot opens a thread, queries the
logs and replies.

## Safety model

- No SSH, no command execution, no buttons. The worst the bot can do is read logs and post text.
- Grafana access is a **Viewer** service account, GET requests to the Loki datasource only.
- Log queries are capped (max 24h back, 200 lines) and labels are validated before use.
- Alerts and logs are treated as untrusted data (prompt injection can at most change the text
  Claude writes), and replies cannot ping `@everyone`/roles.
- Anyone who can post in an alert channel can steer Claude's text and burn API credit. Keep those
  channels private.
- Log lines go to Anthropic's API and into Discord threads. Don't point it at logs containing secrets.

## Notes / next ideas

- Requires a Loki datasource in Grafana; other datasources are not queried.
- Easy extensions: reply-in-thread follow-up questions, dedupe repeated alerts.
