use anyhow::{anyhow, Result};
use architect_api::symbology::*;
use log::{info, warn};
use rust_decimal::prelude::*;
use std::collections::BTreeMap;
use vertex_sdk::prelude::*;

pub struct VertexSymbology {
    pub products: BTreeMap<u32, Product>,
    pub tradable_products: BTreeMap<u32, TradableProduct>,
    pub execution_info:
        BTreeMap<TradableProduct, BTreeMap<ExecutionVenue, ExecutionInfo>>,
}

pub async fn load_symbology(client: &VertexClient) -> Result<VertexSymbology> {
    let venue: ExecutionVenue = "VERTEX".into();

    info!("loading assets...");
    let usdc = Product::crypto("USDC")?;
    let mut products = BTreeMap::new();
    let mut tradable_products = BTreeMap::new();
    let mut assets = BTreeMap::new();
    let all_assets = client.get_assets().await.map_err(|e| anyhow!(e))?;
    for asset in all_assets {
        match asset.market_type.as_deref() {
            Some("spot") | None => {
                products.insert(asset.product_id, Product::crypto(&asset.symbol)?);
            }
            Some("perp") | _ => {
                // NB: these are considered later
            }
        }
        assets.insert(asset.product_id, asset);
    }
    info!("{} assets loaded", assets.len());

    info!("loading tradable products and building symbology...");
    let mut execution_info = BTreeMap::new();
    let all_products = client.get_all_products().await.map_err(|e| anyhow!(e))?;
    for item in &all_products.perp_products {
        let asset = match assets.get(&item.product_id) {
            Some(asset) => asset,
            None => {
                warn!("no asset found for product_id={}, skipping", item.product_id);
                continue;
            }
        };
        let base = match asset.ticker_id.as_ref() {
            Some(ticker_id) => match ticker_id.strip_suffix("-PERP_USDC") {
                Some(raw_perp) => {
                    Product::perpetual(&format!("{raw_perp}-USDC"), Some("VERTEX"))?
                }
                None => {
                    warn!(
                        "unexpected quote asset for product_id={}, skipping",
                        item.product_id
                    );
                    continue;
                }
            },
            None => {
                warn!("no ticker_id found for product_id={}, skipping", item.product_id);
                continue;
            }
        };
        let tradable_product = TradableProduct::new(&base, Some(&usdc))?;
        tradable_products.insert(item.product_id, tradable_product.clone());
        let tick_size = match Decimal::try_from_i128_with_scale(
            item.book_info.price_increment_x18,
            18,
        ) {
            Ok(tick_size) => tick_size.normalize(),
            Err(_) => {
                warn!(
                    "{}: price_increment_x18 {} cannot convert to decimal",
                    asset.symbol, item.book_info.price_increment_x18
                );
                continue;
            }
        };
        let step_size =
            match Decimal::try_from_i128_with_scale(item.book_info.size_increment, 18) {
                Ok(step_size) => step_size.normalize(),
                Err(_) => {
                    warn!(
                        "{}: size_increment {} cannot convert to decimal",
                        asset.symbol, item.book_info.size_increment
                    );
                    continue;
                }
            };
        let min_size =
            match Decimal::try_from_i128_with_scale(item.book_info.min_size, 18) {
                Ok(min_size) => min_size.normalize(),
                Err(_) => {
                    warn!(
                        "{}: min_size {} cannot convert to decimal",
                        asset.symbol, item.book_info.min_size
                    );
                    continue;
                }
            };
        let info = ExecutionInfo {
            execution_venue: venue.clone(),
            exchange_symbol: Some(item.product_id.to_string()),
            tick_size: TickSize::Simple(tick_size),
            step_size,
            min_order_quantity: min_size,
            min_order_quantity_unit: MinOrderQuantityUnit::Base,
            is_delisted: false,
            initial_margin: None,
            maintenance_margin: None,
        };
        execution_info
            .insert(tradable_product, BTreeMap::from_iter([(venue.clone(), info)]));
    }
    for _item in &all_products.spot_products {
        // TODO
    }
    info!("{} tradable products loaded", execution_info.len());

    Ok(VertexSymbology { products, tradable_products, execution_info })
}
