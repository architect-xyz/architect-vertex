use super::VertexService;
use anyhow::{anyhow, Result};
use architect_api::{cpty::CptyResponse, folio::AccountPosition, AccountIdOrName};
use chrono::Utc;
use log::{debug, error, warn};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::BTreeMap;
use vertex_sdk::prelude::*;

impl VertexService {
    pub async fn update_account_summary(&self, client: &VertexClient) -> Result<()> {
        let now = Utc::now();
        let subaccount = client.subaccount().map_err(|e| anyhow!(e))?;
        let subaccount_info =
            client.get_subaccount_info(subaccount).await.map_err(|e| anyhow!(e))?;
        let mut balances = BTreeMap::new();
        let mut positions = BTreeMap::new();
        for item in subaccount_info.spot_balances {
            let product = match self.vertex_symbology.products.get(&item.product_id) {
                Some(product) => product,
                None => {
                    warn!("unknown product_id {}", item.product_id);
                    continue;
                }
            };
            let quantity =
                match Decimal::try_from_i128_with_scale(item.balance.amount, 18) {
                    Ok(quantity) => quantity,
                    Err(_) => {
                        error!(
                            "unable to cast amount {} for product_id {}",
                            item.balance.amount, item.product_id
                        );
                        continue;
                    }
                };
            if quantity > dec!(0) {
                balances.insert(product.clone(), quantity);
            }
        }
        for item in subaccount_info.perp_balances {
            let tradable_product =
                match self.vertex_symbology.tradable_products.get(&item.product_id) {
                    Some(product) => product,
                    None => {
                        warn!("unknown product_id {}", item.product_id);
                        continue;
                    }
                };
            let quantity =
                match Decimal::try_from_i128_with_scale(item.balance.amount, 18) {
                    Ok(quantity) => quantity,
                    Err(_) => {
                        error!(
                            "unable to cast amount {} for product_id {}",
                            item.balance.amount, item.product_id
                        );
                        continue;
                    }
                };
            if quantity > dec!(0) {
                positions.insert(
                    tradable_product.clone(),
                    vec![AccountPosition { quantity, ..Default::default() }],
                );
            }
        }
        debug!("account balances: {:?}", balances);
        debug!("account positions: {:?}", positions);
        let _ = self.cpty_res_tx.send(CptyResponse::UpdateAccountSummary {
            account: AccountIdOrName::Id(self.account_id),
            timestamp: now.timestamp(),
            timestamp_ns: now.timestamp_subsec_nanos(),
            balances: Some(balances),
            positions: Some(positions),
            statistics: None,
            is_snapshot: true,
        });
        Ok(())
    }
}
