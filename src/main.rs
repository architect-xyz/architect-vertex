use anyhow::{anyhow, Context, Result};
use architect_api::{
    cpty::*,
    grpc::{
        json_service::{cpty_server::CptyServer, orderflow_server::OrderflowServer},
        SubscriptionStream,
    },
    orderflow::*,
    AccountId,
};
use clap::Parser;
use futures::{pin_mut, select_biased, FutureExt, TryStreamExt};
use log::{debug, error, info};
use serde::{Deserialize, Serialize};
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio::{
    sync::{broadcast, mpsc},
    time::{interval, MissedTickBehavior},
};
use tokio_stream::{
    wrappers::{errors::BroadcastStreamRecvError, BroadcastStream},
    StreamExt,
};
use tonic::{transport::Server, Request, Response, Status, Streaming};
use vertex_sdk::{prelude::*, utils::private_key::private_key};

mod order_entry;
mod positions;
mod symbology;

pub struct VertexService {
    pub account_id: AccountId,
    pub cpty_req_tx: mpsc::UnboundedSender<CptyRequest>,
    pub cpty_res_tx: broadcast::Sender<CptyResponse>,
    pub orderflow_tx: broadcast::Sender<Orderflow>,
    pub vertex_symbology: symbology::VertexSymbology,
}

fn map_broadcast_stream_err(e: BroadcastStreamRecvError) -> Status {
    match e {
        BroadcastStreamRecvError::Lagged(_) => Status::data_loss("lagged"),
    }
}

#[tonic::async_trait]
impl architect_api::grpc::json_service::cpty_server::Cpty for VertexService {
    type CptyStream = SubscriptionStream<CptyResponse>;

    async fn cpty(
        &self,
        request: Request<Streaming<CptyRequest>>,
    ) -> Result<Response<Self::CptyStream>, Status> {
        let conn_name = match request.remote_addr() {
            Some(addr) => addr.to_string(),
            None => "unknown".to_string(),
        };
        let mut in_stream = request.into_inner();
        let cpty_req_tx = self.cpty_req_tx.clone();
        tokio::spawn(async move {
            debug!("{conn_name}: cpty stream started");
            loop {
                match in_stream.message().await {
                    Ok(Some(msg)) => {
                        debug!("received cpty request: {:?}", msg);
                        let _ = cpty_req_tx.send(msg);
                    }
                    Ok(None) => info!("{conn_name}: cpty stream closed"),
                    Err(e) => error!("{conn_name}: cpty stream error: {:?}", e),
                }
            }
        });
        // TODO: reconcile open orders
        let out_stream = tokio_stream::iter([Ok(CptyResponse::Symbology {
            execution_info: self.vertex_symbology.execution_info.clone(),
        })])
        .chain(
            BroadcastStream::new(self.cpty_res_tx.subscribe())
                .map_err(map_broadcast_stream_err),
        );
        Ok(Response::new(Box::pin(out_stream)))
    }

    async fn cpty_status(
        &self,
        _request: Request<CptyStatusRequest>,
    ) -> Result<Response<CptyStatus>, Status> {
        Err(Status::unimplemented(""))
    }

    async fn cptys(
        &self,
        _request: Request<CptysRequest>,
    ) -> Result<Response<CptysResponse>, Status> {
        Err(Status::unimplemented(""))
    }
}

#[tonic::async_trait]
impl architect_api::grpc::json_service::orderflow_server::Orderflow for VertexService {
    type OrderflowStream = SubscriptionStream<Orderflow>;
    type SubscribeOrderflowStream = SubscriptionStream<Orderflow>;
    type DropcopyStream = SubscriptionStream<Dropcopy>;

    async fn orderflow(
        &self,
        _request: Request<Streaming<OrderflowRequest>>,
    ) -> Result<Response<Self::OrderflowStream>, Status> {
        Err(Status::unimplemented(""))
    }

    async fn subscribe_orderflow(
        &self,
        _request: Request<SubscribeOrderflowRequest>,
    ) -> Result<Response<Self::SubscribeOrderflowStream>, Status> {
        let out_stream = BroadcastStream::new(self.orderflow_tx.subscribe())
            .map_err(map_broadcast_stream_err);
        Ok(Response::new(Box::pin(out_stream)))
    }

    async fn dropcopy(
        &self,
        _request: Request<DropcopyRequest>,
    ) -> Result<Response<Self::DropcopyStream>, Status> {
        Err(Status::unimplemented(""))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Config {
    account_id: AccountId,
}

/// >_ Architect / Vertex
///
/// Set environment variable (via `.env` file or otherwise)
/// RUST_SDK_PRIVATE_KEY=0x...
#[derive(Debug, Parser)]
struct Args {
    #[arg(long, short)]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let args = Args::parse();
    let config_str = std::fs::read_to_string(&args.config)?;
    let config: Config = serde_json::from_str(&config_str)?;

    let client = VertexClient::new(ClientMode::Prod)
        .with_signer(private_key())
        .await
        .map_err(|e| anyhow!(e))?;
    let vertex_symbology = symbology::load_symbology(&client).await?;

    let (cpty_req_tx, mut cpty_req_rx) = mpsc::unbounded_channel();
    let (cpty_res_tx, _) = broadcast::channel(100);
    let (orderflow_tx, _) = broadcast::channel(100);

    let service = Arc::new(VertexService {
        account_id: config.account_id,
        cpty_req_tx,
        cpty_res_tx,
        orderflow_tx,
        vertex_symbology,
    });

    let server_fut = Server::builder()
        .add_service(CptyServer::from_arc(service.clone()))
        .add_service(OrderflowServer::from_arc(service.clone()))
        .serve("0.0.0.0:50051".parse()?);
    pin_mut!(server_fut);

    let mut poll_account_interval = interval(Duration::from_secs(3));
    poll_account_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        select_biased! {
            msg = cpty_req_rx.recv().fuse() => {
                if let Some(msg) = msg {
                    handle_cpty_request(&service, &client, msg).await?;
                }
            }
            res = (&mut server_fut).fuse() => {
                res.context("grpc server")?;
            }
            _ = poll_account_interval.tick().fuse() => {
                service.update_account_summary(&client).await?;
            }
        }
    }
}

async fn handle_cpty_request(
    service: &VertexService,
    client: &VertexClient,
    msg: CptyRequest,
) -> Result<()> {
    debug!("processing cpty request: {:?}", msg);
    match msg {
        CptyRequest::Login(_) => {}
        CptyRequest::Logout(_) => {}
        CptyRequest::PlaceOrder(order) => {
            service.place_order(client, order).await?;
        }
        CptyRequest::CancelOrder { cancel, original_order } => {
            service.cancel_order(client, cancel, original_order).await?;
        }
    }
    Ok(())
}
