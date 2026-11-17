
use std::collections::{HashMap, HashSet};

use axum::{
    Extension, Json,
    extract::State,
    http::{HeaderMap, Uri},
    response::Response,
};
use sqlx_pool_router::PoolProvider;

use crate::{
    AppState,
    api::{
        handlers::ai_models::{customer_priced_models, database_error, presented_key_hash, resolve_discovery_key},
        models::{cache_pricing::CachePricingResponse, tariffs::TariffResponse},
    },
    db::{
        handlers::Deployments,
        models::{api_keys::ApiKeyPurpose, deployments::ModelType},
    },
    inference::rejections::{Origin, Recorder},
    openrouter::{
        document::{ModelDocument, ModelFeed},
        feed::{CacheMultipliers, ListedModel, Prices, model_document},
        listing::OpenRouterListing,
    },
};

#[allow(clippy::result_large_err)]
#[tracing::instrument(skip_all)]
pub async fn list_openrouter_models<P: PoolProvider>(
    State(state): State<AppState<P>>,
    recorder: Option<Extension<Recorder>>,
    presented: Option<Extension<onwards::auth::PresentedKey>>,
    uri: Uri,
    headers: HeaderMap,
) -> Result<Json<ModelFeed>, Response> {
    let origin = Origin::new(recorder.as_ref().map(|Extension(recorder)| recorder), &headers, uri.path());
    let key_hash = presented_key_hash(presented.as_ref().map(|Extension(key)| key), &headers, origin)?;
    let user_id = resolve_discovery_key(&state, &key_hash, origin).await?;

    let mut conn = state
        .db
        .read()
        .acquire()
        .await
        .map_err(|e| database_error("acquire_read_connection", e))?;
    let rows = Deployments::new(&mut conn)
        .list_openrouter_listings(user_id)
        .await
        .map_err(|e| database_error("list_openrouter_listings", e))?;
    if rows.is_empty() {
        return Ok(Json(ModelFeed { data: Vec::new() }));
    }
    let mut listings = HashMap::with_capacity(rows.len());
    for row in rows {
        let listing = serde_json::from_value::<OpenRouterListing>(row.listing)
            .map_err(|e| database_error("parse_openrouter_listing", format!("{}: {e}", row.alias)))?;
        listings.insert(row.id, listing);
    }

    let model_ids = listings.keys().copied().collect();
    let mut models = customer_priced_models(&state, conn, model_ids, user_id).await?;
    models.sort_by(|a, b| a.alias.cmp(&b.alias));

    let config = state.current_config();
    let datacenters = &config.integrations.openrouter.datacenters;
    let data: Vec<ModelDocument> = models
        .iter()
        .filter_map(|model| {
            let listing = listings.get(&model.id)?;
            let metadata = model.metadata.as_ref();
            let refused = refused_params(
                &config.onwards.rejected_params,
                config.onwards.model_allowed_params.get(&model.alias),
            );
            model_document(
                &ListedModel {
                    alias: &model.alias,
                    name: model.display_name.as_deref().unwrap_or(&model.alias),
                    created: model.created_at.timestamp(),
                    model_type: model.model_type.clone().unwrap_or(ModelType::Chat),
                    reasoning: model
                        .capabilities
                        .as_deref()
                        .is_some_and(|capabilities| capabilities.iter().any(|capability| capability == "reasoning")),
                    context_window: metadata.and_then(|metadata| metadata.context_window),
                    quantization: metadata.and_then(|metadata| metadata.quantization.as_deref()),
                    listing,
                    prices: realtime_prices(model.tariffs.as_deref(), model.cache_pricing.as_ref()),
                },
                &refused,
                datacenters,
            )
        })
        .collect();
    Ok(Json(ModelFeed { data }))
}

fn refused_params<'a>(rejected: &'a [String], allowed: Option<&Vec<String>>) -> HashSet<&'a str> {
    rejected
        .iter()
        .filter(|name| !allowed.is_some_and(|allowed| allowed.contains(name)))
        .map(String::as_str)
        .collect()
}

fn realtime_prices(tariffs: Option<&[TariffResponse]>, cache: Option<&CachePricingResponse>) -> Option<Prices> {
    let tariff = tariffs?
        .iter()
        .find(|tariff| tariff.is_active && tariff.api_key_purpose == Some(ApiKeyPurpose::Realtime))?;
    let cache = cache.filter(|cache| cache.enabled).and_then(|cache| {
        Some(CacheMultipliers {
            read: cache.read_multiplier?,
            write_5m: cache.write_multiplier_5m?,
            write_1h: cache.write_multiplier_1h?,
            write_24h: cache.write_multiplier_24h?,
        })
    });
    Some(Prices {
        input: tariff.input_price_per_token,
        output: tariff.output_price_per_token,
        cache,
    })
}
