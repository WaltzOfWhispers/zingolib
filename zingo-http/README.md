# zingo-http

Axum-based HTTP sidecar that wraps the Zingolib light client and exposes a minimal wallet API for sending and inspecting Zcash activity.

## Run
```
cd zingo-http
LIGHTWALLETD_ENDPOINT=https://lightwalletd.testnet.z.cash:9067 \
ZCASH_MNEMONIC="twenty four word seed goes here" \
cargo run
```

## Configuration
- Required: `LIGHTWALLETD_ENDPOINT` and one credential (`ZCASH_MNEMONIC`, `ZCASH_SPENDING_KEY`, `ZCASH_USK`, or `ZCASH_UFVK`).
- Network: `ZCASH_NETWORK` (`testnet` default, `mainnet` supported).
- Optional: `ZCASH_LIGHT_CLIENT_API_KEY` (sent as `x-api-key`), `BIND_ADDR`/`PORT` (`127.0.0.1:8787` default), `ZCASH_DATA_DIR` (`.zingo-http` default), `ZCASH_SYNC_INTERVAL_SECS`, `ZCASH_MIN_CONFIRMATIONS`.

## API
- `GET /health` wallet/network status plus last sync error.
- `GET /address` unified address for account 0.
- `GET /balance` orchard/sapling/transparent balances and shielded total.
- `GET /sync-height` wallet, fully scanned, and latest heights with sync mode.
- `GET /transactions` most recent three transaction summaries.
- `POST /send-shielded-tx` send from account 0 (view-only wallets return 403):
  ```
  curl -X POST http://127.0.0.1:8787/send-shielded-tx \
    -H "content-type: application/json" \
    -H "x-api-key: $ZCASH_LIGHT_CLIENT_API_KEY" \
    -d '{"toAddress":"u123...","amountZat":10000,"memo":"hi from zingo-http"}'
  ```
