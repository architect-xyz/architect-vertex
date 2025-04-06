use super::VertexService;
use anyhow::{anyhow, bail, Result};
use architect_api::orderflow::*;
use rust_decimal::prelude::*;
use vertex_sdk::{prelude::*, vertex_utils::math::f64_to_x18};

impl VertexService {
    pub async fn place_order(&self, client: &VertexClient, order: Order) -> Result<()> {
        let info = match self
            .vertex_symbology
            .execution_info
            .get(&order.symbol)
            .and_then(|i| i.get(&order.execution_venue))
        {
            Some(info) => info,
            None => {
                let _ = self.orderflow_tx.send(Orderflow::OrderReject(OrderReject {
                    order_id: order.id,
                    reason: OrderRejectReason::Unknown,
                    message: Some(format!("no execution info for symbol")),
                }));
                return Ok(());
            }
        };
        let product_id: u32 = match info.exchange_symbol.as_ref() {
            Some(product_id_str) => product_id_str.parse()?,
            None => {
                bail!("unexpected no product id for symbol");
            }
        };
        let quantity_f64 = match order.quantity.to_f64() {
            Some(quantity) => quantity,
            None => {
                let _ = self.orderflow_tx.send(Orderflow::OrderReject(OrderReject {
                    order_id: order.id,
                    reason: OrderRejectReason::Unknown,
                    message: Some(format!("unable to cast quantity")),
                }));
                return Ok(());
            }
        };
        let limit_price = match order.order_type {
            OrderType::Limit(limit) => {
                if limit.post_only {
                    let _ = self.orderflow_tx.send(Orderflow::OrderReject(OrderReject {
                        order_id: order.id,
                        reason: OrderRejectReason::UnsupportedOrderType,
                        message: Some(format!("unsupported post-only flag")),
                    }));
                    return Ok(());
                }
                limit.limit_price
            }
            _ => {
                let _ = self.orderflow_tx.send(Orderflow::OrderReject(OrderReject {
                    order_id: order.id,
                    reason: OrderRejectReason::UnsupportedOrderType,
                    message: Some(format!("unsupported order type")),
                }));
                return Ok(());
            }
        };
        let price_f64 = match limit_price.to_f64() {
            Some(price) => price,
            None => {
                let _ = self.orderflow_tx.send(Orderflow::OrderReject(OrderReject {
                    order_id: order.id,
                    reason: OrderRejectReason::Unknown,
                    message: Some(format!("unable to cast price")),
                }));
                return Ok(());
            }
        };
        let res = match client
            .place_order_builder()
            .product_id(product_id)
            .amount(f64_to_x18(quantity_f64))
            .price_x18(f64_to_x18(price_f64))
            .execute()
            .await
            .map_err(|e| anyhow!(e))
        {
            Ok(Some(res)) => res,
            Ok(None) => {
                let _ = self.orderflow_tx.send(Orderflow::OrderReject(OrderReject {
                    order_id: order.id,
                    reason: OrderRejectReason::Unknown,
                    message: Some(format!("unable to place order")),
                }));
                return Ok(());
            }
            Err(e) => {
                let _ = self.orderflow_tx.send(Orderflow::OrderReject(OrderReject {
                    order_id: order.id,
                    reason: OrderRejectReason::Unknown,
                    message: Some(format!("unable to place order: {}", e)),
                }));
                return Ok(());
            }
        };
        let exchange_order_id = format!("0x{}", hex::encode(res.digest));
        let _ = self.orderflow_tx.send(Orderflow::OrderAck(OrderAck {
            order_id: order.id,
            exchange_order_id: Some(exchange_order_id),
        }));
        Ok(())
    }

    pub async fn cancel_order(
        &self,
        client: &VertexClient,
        cancel: Cancel,
        original_order: Option<Order>,
    ) -> Result<()> {
        macro_rules! reject {
            ($message:expr) => {
                let _ = self.orderflow_tx.send(Orderflow::CancelReject(CancelReject {
                    cancel_id: cancel.cancel_id,
                    order_id: cancel.order_id,
                    message: Some(format!($message)),
                }));
                return Ok(());
            };
        }
        let original_order = match original_order {
            Some(order) => order,
            None => {
                reject!("no original order");
            }
        };
        let digest_s = match original_order.exchange_order_id.as_ref() {
            Some(xoid) => match xoid.strip_suffix("0x") {
                Some(digest_s) => digest_s,
                None => {
                    reject!("invalid exchange order id");
                }
            },
            None => {
                reject!("no exchange order id");
            }
        };
        let mut digest = [0u8; 32];
        if let Err(_) = hex::decode_to_slice(digest_s, &mut digest) {
            reject!("invalid exchange order id");
        }
        let res = match client
            .cancellation_builder()
            .digests(vec![digest])
            .product_ids(vec![])
            .execute()
            .await
            .map_err(|e| anyhow!(e))
        {
            Ok(Some(res)) => res,
            Ok(None) | Err(_) => {
                reject!("unable to cancel order");
            }
        };
        for co in res.cancelled_orders {
            if co.digest == digest {
                let _ = self.orderflow_tx.send(Orderflow::OrderCanceled(OrderCanceled {
                    order_id: original_order.id,
                    cancel_id: Some(cancel.cancel_id),
                }));
                return Ok(());
            }
        }
        reject!("order didn't cancel");
    }
}
