use anyhow::{anyhow, Context, Result};
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use axum::{
    body::Body,
    extract::State,
    http::{Request, StatusCode},
    middleware::{self, Next},
    response::Response,
    routing::{get, post},
    Json, Router,
};
use bip0039::Mnemonic;
use pepper_sync::config::{PerformanceLevel, SyncConfig, TransparentAddressDiscovery};
use serde::{Deserialize, Serialize};
use std::{env, net::SocketAddr, num::NonZeroU32, sync::Arc};
use tokio::{
    net::TcpListener,
    sync::{Mutex, RwLock},
    time::{interval, Duration},
};
use tower_http::cors::CorsLayer;
use tracing_subscriber::{EnvFilter, FmtSubscriber};
use zingolib::{
    config::ChainType,
    data::receivers::{transaction_request_from_receivers, Receiver},
    lightclient::LightClient,
    wallet::{
        keys::unified::UnifiedKeyStore,
        summary::data::TransactionSummary,
        LightWallet, WalletBase, WalletSettings,
    },
};
use zip32::AccountId;
use zcash_address::ZcashAddress;
use zcash_primitives::memo::MemoBytes;
use zcash_protocol::value::Zatoshis;

const DEFAULT_PORT: &str = "8787";
const DEFAULT_BIND_ADDR: &str = "127.0.0.1";
const DEFAULT_DATA_DIR: &str = ".zingo-http";
const DEFAULT_SYNC_INTERVAL_SECS: u64 = 30;
const DEFAULT_MIN_CONFIRMATIONS: u32 = 3;

#[derive(Clone)]
struct AppState {
    client: Arc<RwLock<LightClient>>,
    api_key: Option<String>,
    last_sync_error: Arc<Mutex<Option<String>>>,
}

#[derive(Clone)]
struct Settings {
    lightwalletd_uri: String,
    data_dir: String,
    chain_type: ChainType,
    api_key: Option<String>,
    bind_addr: String,
    port: String,
    sync_interval_secs: u64,
    min_confirmations: NonZeroU32,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    FmtSubscriber::builder().with_env_filter(filter).init();

    let settings = Settings::from_env()?;
    let client = initialize_lightclient(&settings)?;
    let sync_error = Arc::new(Mutex::new(None));

    let app_state = AppState {
        client: client.clone(),
        api_key: settings.api_key.clone(),
        last_sync_error: sync_error.clone(),
    };

    spawn_sync_loop(app_state.clone(), settings.sync_interval_secs);

    let addr: SocketAddr = format!("{}:{}", settings.bind_addr, settings.port)
        .parse()
        .context("invalid bind addr/port")?;
    tracing::info!("Starting zingo-http sidecar on {addr}");

    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, build_router(app_state)).await?;
    Ok(())
}

fn build_router(app_state: AppState) -> Router {
    let api_key = app_state.api_key.clone();

    Router::new()
        .route("/health", get(health))
        .route("/address", get(unified_address))
        .route("/balance", get(balance))
        .route("/sync-height", get(sync_height))
        .route("/transactions", get(transactions))
        .route("/send-shielded-tx", post(send_shielded_tx))
        .with_state(app_state)
        .layer(CorsLayer::permissive())
        .route_layer(middleware::from_fn(move |req, next| {
            let api_key = api_key.clone();
            async move { auth(req, next, api_key).await }
        }))
}

async fn auth(
    req: Request<Body>,
    next: Next,
    api_key: Option<String>,
) -> Result<Response, StatusCode> {
    if let Some(api_key) = api_key {
        let headers = req.headers();
        if let Some(sent_key) = headers.get("x-api-key").and_then(|v| v.to_str().ok()) {
            if sent_key == api_key {
                return Ok(next.run(req).await);
            }
        }
        Err(StatusCode::UNAUTHORIZED)
    } else {
        Ok(next.run(req).await)
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SendShieldedTxRequest {
    to_address: String,
    amount_zat: u64,
    memo: Option<String>,
    spending_key: Option<String>,
    mnemonic: Option<String>,
    fee: Option<u64>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SendShieldedTxResponse {
    tx_id: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PoolBalance {
    confirmed: Option<u64>,
    unconfirmed: Option<u64>,
    total: Option<u64>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct BalanceResponse {
    orchard: PoolBalance,
    sapling: PoolBalance,
    transparent: PoolBalance,
    shielded_total: Option<u64>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct AddressResponse {
    unified_address: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct HealthResponse {
    status: &'static str,
    network: String,
    lightwalletd: String,
    sync_mode: String,
    wallet_height: Option<u64>,
    fully_scanned_height: Option<u64>,
    last_sync_error: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SyncHeightResponse {
    wallet_height: Option<u64>,
    fully_scanned_height: Option<u64>,
    latest_height: Option<u64>,
    sync_mode: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TransactionItem {
    tx_id: String,
    status: String,
    kind: String,
    value: u64,
    fee: Option<u64>,
    block_height: u64,
    datetime: u32,
    memos: Vec<String>,
}

#[derive(Serialize)]
struct TransactionsResponse {
    transactions: Vec<TransactionItem>,
}

async fn send_shielded_tx(
    State(state): State<AppState>,
    Json(payload): Json<SendShieldedTxRequest>,
) -> Result<Json<SendShieldedTxResponse>, StatusCode> {
    if payload.spending_key.is_some() || payload.mnemonic.is_some() || payload.fee.is_some() {
        tracing::warn!("Per-request key or fee overrides are ignored; configure keys via environment");
    }

    let has_spend = {
        let client = state.client.read().await;
        let wallet = client.wallet.read().await;
        matches!(
            wallet.unified_key_store.get(&AccountId::ZERO),
            Some(UnifiedKeyStore::Spend(_))
        )
    };
    if !has_spend {
        return Err(StatusCode::FORBIDDEN);
    }

    let amount = Zatoshis::from_u64(payload.amount_zat)
        .map_err(|_| StatusCode::BAD_REQUEST)?;

    let recipient_address =
        ZcashAddress::try_from_encoded(&payload.to_address).map_err(|_| StatusCode::BAD_REQUEST)?;

    let memo_bytes = payload
        .memo
        .as_ref()
        .map(|memo| MemoBytes::from_bytes(memo.as_bytes()).map_err(|_| StatusCode::BAD_REQUEST))
        .transpose()?;

    let receivers = vec![Receiver {
        recipient_address,
        amount,
        memo: memo_bytes,
    }];

    let request =
        transaction_request_from_receivers(receivers).map_err(|_| StatusCode::BAD_REQUEST)?;

    let txid = {
        let mut client = state.client.write().await;
        if let Err(e) = client.sync_and_await().await {
            tracing::warn!("Sync attempt before send failed: {e:?}");
        }

        client
            .quick_send(request, AccountId::ZERO)
            .await
            .map_err(|e| {
                tracing::error!("Error sending transaction: {e}");
                StatusCode::INTERNAL_SERVER_ERROR
            })?
    };

    Ok(Json(SendShieldedTxResponse {
        tx_id: txid[0].to_string(),
    }))
}

async fn unified_address(State(state): State<AppState>) -> Result<Json<AddressResponse>, StatusCode> {
    let client = state.client.read().await;
    let addresses = client.unified_addresses_json().await;
    let unified_address = addresses
        .members()
        .find_map(|addr| addr["encoded_address"].as_str().map(|s| s.to_string()))
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(AddressResponse { unified_address }))
}

async fn balance(State(state): State<AppState>) -> Result<Json<BalanceResponse>, StatusCode> {
    let client = state.client.read().await;
    let balances = client
        .account_balance(AccountId::ZERO)
        .await
        .map_err(|e| {
            tracing::error!("Failed to fetch balance: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let orchard = PoolBalance {
        confirmed: zatoshis_to_u64(balances.confirmed_orchard_balance),
        unconfirmed: zatoshis_to_u64(balances.unconfirmed_orchard_balance),
        total: zatoshis_to_u64(balances.total_orchard_balance),
    };
    let sapling = PoolBalance {
        confirmed: zatoshis_to_u64(balances.confirmed_sapling_balance),
        unconfirmed: zatoshis_to_u64(balances.unconfirmed_sapling_balance),
        total: zatoshis_to_u64(balances.total_sapling_balance),
    };
    let transparent = PoolBalance {
        confirmed: zatoshis_to_u64(balances.confirmed_transparent_balance),
        unconfirmed: zatoshis_to_u64(balances.unconfirmed_transparent_balance),
        total: zatoshis_to_u64(balances.total_transparent_balance),
    };

    let shielded_total = sum_options(orchard.total, sapling.total);

    Ok(Json(BalanceResponse {
        orchard,
        sapling,
        transparent,
        shielded_total,
    }))
}

async fn sync_height(
    State(state): State<AppState>,
) -> Result<Json<SyncHeightResponse>, StatusCode> {
    let client = state.client.read().await;
    let wallet = client.wallet.read().await;
    let wallet_height = wallet.sync_state.wallet_height().map(block_height_to_u64);
    let fully_scanned_height = wallet
        .sync_state
        .fully_scanned_height()
        .map(block_height_to_u64);
    let latest_height = wallet.sync_state.wallet_height().map(block_height_to_u64);
    let sync_mode = client.sync_mode();
    drop(wallet);

    Ok(Json(SyncHeightResponse {
        wallet_height,
        fully_scanned_height,
        latest_height,
        sync_mode: format!("{sync_mode:?}"),
    }))
}

async fn health(State(state): State<AppState>) -> Result<Json<HealthResponse>, StatusCode> {
    let client = state.client.read().await;
    let wallet = client.wallet.read().await;
    let wallet_height = wallet.sync_state.wallet_height().map(block_height_to_u64);
    let fully_scanned_height = wallet
        .sync_state
        .fully_scanned_height()
        .map(block_height_to_u64);
    let lightwalletd = client.server_uri().to_string();
    let network = format!("{:?}", client.config.chain);
    let sync_mode = client.sync_mode();
    drop(wallet);

    let last_sync_error = state.last_sync_error.lock().await.clone();

    Ok(Json(HealthResponse {
        status: "ok",
        network,
        lightwalletd,
        sync_mode: format!("{sync_mode:?}"),
        wallet_height,
        fully_scanned_height,
        last_sync_error,
    }))
}

async fn transactions(
    State(state): State<AppState>,
) -> Result<Json<TransactionsResponse>, StatusCode> {
    let client = state.client.read().await;
    let summaries = client
        .transaction_summaries(true)
        .await
        .map_err(|e| {
            tracing::error!("Failed to fetch transactions: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let txs = summaries
        .0
        .iter()
        .take(3)
        .map(transaction_item_from_summary)
        .collect();

    Ok(Json(TransactionsResponse { transactions: txs }))
}

fn initialize_lightclient(settings: &Settings) -> Result<Arc<RwLock<LightClient>>> {
    let wallet_base = wallet_base_from_env()?;

    let wallet_settings = WalletSettings {
        sync_config: SyncConfig {
            transparent_address_discovery: TransparentAddressDiscovery::minimal(),
            performance_level: PerformanceLevel::High,
        },
        min_confirmations: settings.min_confirmations,
    };

    let config = zingolib::config::load_clientconfig(
        settings
            .lightwalletd_uri
            .parse()
            .context("invalid LIGHTWALLETD_ENDPOINT")?,
        Some(settings.data_dir.clone().into()),
        settings.chain_type,
        wallet_settings.clone(),
        NonZeroU32::new(1).expect("nonzero"),
        "".to_string(),
    )?;

    let client = LightClient::create_from_wallet(
        LightWallet::new(config.chain, wallet_base, 0.into(), wallet_settings)?,
        config,
        false,
    )?;

    Ok(Arc::new(RwLock::new(client)))
}

fn wallet_base_from_env() -> Result<WalletBase> {
    if let Some(raw_mnemonic) = first_env(&["ZCASH_MNEMONIC", "ZCASH_SEED", "ZCASH_SPENDING_KEY"])
    {
        let mnemonic = Mnemonic::from_phrase(raw_mnemonic)
            .context("failed to parse mnemonic from env")?;
        return Ok(WalletBase::Mnemonic {
            mnemonic,
            no_of_accounts: NonZeroU32::new(1).expect("nonzero"),
        });
    }

    if let Some(raw_usk) = first_env(&["ZCASH_USK"]) {
        let usk_bytes = parse_usk_bytes(&raw_usk)
            .context("failed to parse ZCASH_USK as hex or base64")?;
        return Ok(WalletBase::Usk(usk_bytes));
    }

    if let Some(viewing_key) = first_env(&["ZCASH_UFVK", "ZCASH_VIEWING_KEY"]) {
        return Ok(WalletBase::Ufvk(viewing_key));
    }

    Err(anyhow!(
        "Set ZCASH_MNEMONIC, ZCASH_SPENDING_KEY, ZCASH_USK, or ZCASH_UFVK"
    ))
}

fn first_env(keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|key| env::var(key).ok().filter(|v| !v.trim().is_empty()))
}

fn parse_usk_bytes(raw: &str) -> Option<Vec<u8>> {
    hex::decode(raw).ok().or_else(|| BASE64_STANDARD.decode(raw).ok())
}

fn spawn_sync_loop(state: AppState, interval_secs: u64) {
    tokio::spawn(async move {
        let mut ticker = interval(Duration::from_secs(interval_secs));
        loop {
            ticker.tick().await;

            let sync_result = {
                let mut client = state.client.write().await;
                let has_spend = {
                    let wallet = client.wallet.read().await;
                    matches!(
                        wallet.unified_key_store.get(&AccountId::ZERO),
                        Some(UnifiedKeyStore::Spend(_))
                    )
                };
                if !has_spend {
                    tracing::info!("Skipping sync for view-only wallet");
                    continue;
                }
                client.sync_and_await().await
            };

            let mut last_error = state.last_sync_error.lock().await;
            match sync_result {
                Ok(_) => *last_error = None,
                Err(e) => {
                    tracing::warn!("Background sync failed: {e}");
                    *last_error = Some(e.to_string());
                }
            }
        }
    });
}

impl Settings {
    fn from_env() -> Result<Self> {
        let lightwalletd_uri = env::var("LIGHTWALLETD_ENDPOINT")
            .context("LIGHTWALLETD_ENDPOINT environment variable must be set")?;
        let data_dir = env::var("ZCASH_DATA_DIR").unwrap_or_else(|_| DEFAULT_DATA_DIR.to_string());
        let api_key = env::var("ZCASH_LIGHT_CLIENT_API_KEY")
            .ok()
            .filter(|v| !v.trim().is_empty());
        let bind_addr = env::var("BIND_ADDR").unwrap_or_else(|_| DEFAULT_BIND_ADDR.to_string());
        let port = env::var("PORT").unwrap_or_else(|_| DEFAULT_PORT.to_string());
        let sync_interval_secs = env::var("ZCASH_SYNC_INTERVAL_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_SYNC_INTERVAL_SECS);
        let min_confirmations = env::var("ZCASH_MIN_CONFIRMATIONS")
            .ok()
            .and_then(|v| v.parse().ok())
            .and_then(NonZeroU32::new)
            .unwrap_or_else(|| NonZeroU32::new(DEFAULT_MIN_CONFIRMATIONS).expect("nonzero"));

        let network = env::var("ZCASH_NETWORK").unwrap_or_else(|_| "testnet".to_string());
        let chain_type = match network.as_str() {
            "mainnet" => ChainType::Mainnet,
            "testnet" => ChainType::Testnet,
            _ => return Err(anyhow!("Invalid ZCASH_NETWORK specified. Use 'mainnet' or 'testnet'.")),
        };

        Ok(Self {
            lightwalletd_uri,
            data_dir,
            chain_type,
            api_key,
            bind_addr,
            port,
            sync_interval_secs,
            min_confirmations,
        })
    }
}

fn zatoshis_to_u64(value: Option<Zatoshis>) -> Option<u64> {
    value.map(|z| z.into_u64())
}

fn sum_options(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    match (left, right) {
        (Some(a), Some(b)) => Some(a + b),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
}

fn block_height_to_u64(height: zcash_primitives::consensus::BlockHeight) -> u64 {
    u32::from(height) as u64
}

fn transaction_item_from_summary(summary: &TransactionSummary) -> TransactionItem {
    let mut memos = Vec::new();
    for note in summary
        .orchard_notes
        .iter()
        .chain(summary.sapling_notes.iter())
    {
        if let Some(memo) = &note.memo {
            memos.push(memo.clone());
        }
    }
    for note in summary
        .outgoing_orchard_notes
        .iter()
        .chain(summary.outgoing_sapling_notes.iter())
    {
        if let Some(memo) = &note.memo {
            memos.push(memo.clone());
        }
    }

    TransactionItem {
        tx_id: summary.txid.to_string(),
        status: summary.status.to_string(),
        kind: summary.kind.to_string(),
        value: summary.value,
        fee: summary.fee,
        block_height: block_height_to_u64(summary.blockheight),
        datetime: summary.datetime,
        memos,
    }
}
