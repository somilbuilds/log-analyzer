#!/usr/bin/env bash
set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
ENV_FILE="$ROOT/.env.render"

# ── 1. Start Tunnel & Capture URL ─────────────────────────────
echo "==> Step 1: Ensuring Cloudflare Tunnel is active..."
"$ROOT/scripts/tunnel.sh" start

TUNNEL_URL="$("$ROOT/scripts/tunnel.sh" url)"

if [[ -z "$TUNNEL_URL" ]]; then
    echo "ERROR: Failed to retrieve active Cloudflare Tunnel URL."
    exit 1
fi

echo
echo "Active Backend URL: $TUNNEL_URL"
echo "(Already copied to your clipboard!)"
echo

# ── 2. Load Render credentials if present ─────────────────────
if [[ -f "$ENV_FILE" ]]; then
    echo "==> Step 2: Loading Render configuration from .env.render..."
    # shellcheck disable=SC1090
    set -a
    source "$ENV_FILE"
    set +a
else
    echo "==> Step 2: No .env.render file found."
fi

RENDER_KEY="${RENDER_API_KEY:-}"
SERVICE_ID="${RENDER_SERVICE_ID:-}"
DEPLOY_HOOK="${RENDER_DEPLOY_HOOK:-}"

# Filter out placeholder text
if [[ "$RENDER_KEY" == *"your_render_api_key"* ]]; then
    RENDER_KEY=""
fi
if [[ "$SERVICE_ID" == *"your_service_id"* ]]; then
    SERVICE_ID=""
fi

# ── 3. Update Render Environment & Trigger Deploy ─────────────
if [[ -n "$RENDER_KEY" && -n "$SERVICE_ID" ]]; then
    echo "==> Step 3: Updating VITE_API_URL on Render ($SERVICE_ID)..."

    UPDATE_RESPONSE="$(
        curl -s -w "\n%{http_code}" -X PUT \
            "https://api.render.com/v1/services/$SERVICE_ID/env-vars" \
            -H "Authorization: Bearer $RENDER_KEY" \
            -H "Content-Type: application/json" \
            -d "[{\"key\": \"VITE_API_URL\", \"value\": \"$TUNNEL_URL\"}]"
    )"

    HTTP_STATUS="$(echo "$UPDATE_RESPONSE" | tail -n 1)"
    BODY="$(echo "$UPDATE_RESPONSE" | sed '$d')"

    if [[ "$HTTP_STATUS" =~ ^20[0-9]$ ]]; then
        echo "✓ Successfully updated VITE_API_URL = $TUNNEL_URL on Render!"
    else
        echo "WARNING: Failed to update environment variable on Render (HTTP $HTTP_STATUS)."
        echo "Response: $BODY"
        echo "Please paste $TUNNEL_URL manually into Render -> Environment -> VITE_API_URL"
    fi

    echo
    echo "==> Step 4: Triggering deployment on Render..."
    DEPLOY_RESPONSE="$(
        curl -s -w "\n%{http_code}" -X POST \
            "https://api.render.com/v1/services/$SERVICE_ID/deploys" \
            -H "Authorization: Bearer $RENDER_KEY" \
            -H "Content-Type: application/json" \
            -d "{\"clearCache\": \"clear\"}"
    )"

    DEPLOY_HTTP="$(echo "$DEPLOY_RESPONSE" | tail -n 1)"
    DEPLOY_BODY="$(echo "$DEPLOY_RESPONSE" | sed '$d')"

    if [[ "$DEPLOY_HTTP" =~ ^20[0-9]$ ]]; then
        DEPLOY_ID="$(echo "$DEPLOY_BODY" | python3 -c "import sys, json; print(json.load(sys.stdin).get('id', 'unknown'))" 2>/dev/null || echo "triggered")"
        echo "✓ New deployment triggered successfully! Deploy ID: $DEPLOY_ID"
        echo
        echo "Deployment is now building on Render with the live Cloudflare Tunnel URL."
    else
        echo "WARNING: Failed to trigger deploy via Render API (HTTP $DEPLOY_HTTP)."
        echo "Response: $DEPLOY_BODY"
    fi

elif [[ -n "$DEPLOY_HOOK" ]]; then
    echo "==> Step 3: Triggering Render deployment via Deploy Hook..."
    HOOK_RES="$(curl -s -w "\n%{http_code}" -X POST "$DEPLOY_HOOK")"
    HOOK_HTTP="$(echo "$HOOK_RES" | tail -n 1)"
    if [[ "$HOOK_HTTP" =~ ^20[0-9]$ ]]; then
        echo "✓ Render Deploy Hook triggered successfully!"
    else
        echo "WARNING: Deploy hook returned HTTP $HOOK_HTTP: $HOOK_RES"
    fi

else
    echo "============================================================"
    echo "  RENDER CI/CD INSTRUCTIONS"
    echo "============================================================"
    echo "  The tunnel URL is ready and copied to your clipboard:"
    echo "    $TUNNEL_URL"
    echo
    echo "  To complete deployment on Render:"
    echo "  1. Open your Render Static Site dashboard: https://dashboard.render.com"
    echo "  2. Go to 'Environment' tab"
    echo "  3. Set: VITE_API_URL = $TUNNEL_URL"
    echo "  4. Click 'Save Changes' and then 'Manual Deploy' -> 'Deploy latest commit'"
    echo
    echo "  To automate this completely for future runs:"
    echo "  1. cp .env.render.example .env.render"
    echo "  2. Fill in RENDER_API_KEY and RENDER_SERVICE_ID"
    echo "  3. Next time, just run: ./scripts/deploy_render.sh"
    echo "============================================================"
    echo
fi
