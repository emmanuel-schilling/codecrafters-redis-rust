use anyhow::Result;
use clap::Parser;
use codecrafters_redis::{
    app,
    app::{
        aof, pubsub::PubSubHub, rdb, replication::ReplicationState, store::Store,
        wait::WaiterRegistry,
    },
    config::Config,
    foundation::{net, shutdown},
    logging,
};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::signal;
use tracing::{error, info, warn};

#[tokio::main]
async fn main() -> Result<()> {
    let config = Arc::new(Config::parse());
    logging::init(config.log_level.as_str());

    info!(
        "process ID: {}. Starting server on {}",
        std::process::id(),
        config.listen_addr()
    );

    aof::prepare(&config)?;

    let store = Arc::new(Store::new());
    match rdb::load(&config) {
        Ok(rdb_store) => {
            info!(
                "RDB file loaded successfully, {} keys found.",
                rdb_store.len()
            );
            store.load_from_rdb(rdb_store);
        }
        Err(e) => {
            warn!(
                "Failed to load RDB file: {}. Starting with an empty state.",
                e
            );
        }
    }

    let replication_state = Arc::new(ReplicationState::new_from_config(&config));
    let waiters = Arc::new(WaiterRegistry::new());
    let pubsub_hub = Arc::new(PubSubHub::new());

    let ctx = Arc::new(app::AppContext::new(
        Arc::clone(&store),
        Arc::clone(&config),
        replication_state,
        pubsub_hub,
        waiters,
    ));

    if ctx.replication.role() == app::replication::Role::Replica {
        let master_addr = ctx.config.replicaof.clone().unwrap();
        info!("Running in replica mode, will connect to master at {master_addr}");
        let repl_task =
            app::replication::start_replica_mode(master_addr, ctx.config.port, Arc::clone(&ctx));
        tokio::spawn(repl_task);
    }

    let listener = TcpListener::bind(ctx.config.listen_addr()).await?;

    let shutdown_signal = async {
        signal::ctrl_c()
            .await
            .expect("failed to install CTRL+C signal handler");
    };

    let connection_handler = {
        let ctx = Arc::clone(&ctx);
        move |stream| {
            let ctx = Arc::clone(&ctx);
            app::handle_connection(stream, ctx)
        }
    };

    let server = net::run(
        listener,
        shutdown::watch_for_signal(shutdown_signal),
        connection_handler,
    );

    info!("server running");

    if let Err(e) = server.await {
        error!(error = %e, "server failed");
    }

    info!("server has shut down");

    Ok(())
}
