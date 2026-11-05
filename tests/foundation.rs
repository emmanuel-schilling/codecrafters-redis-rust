use codecrafters_redis::foundation::{net, shutdown};
use tokio::net::{TcpListener, TcpStream};

#[tokio::test]
async fn test_server_starts_and_accepts_connection() {
    // Arrange: Bind a listener to a random available port.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    // Arrange: Create a shutdown future that never completes.
    let shutdown_future = std::future::pending::<()>();

    // Arrange: Define a no-op connection handler for this foundation-level test.
    // It accepts the stream and immediately completes, doing no work.
    let no_op_handler = |_stream: TcpStream| async {};

    // Act: Run the server in a background task, providing the no-op handler.
    // This now correctly calls the three-argument version of `net::run`.
    tokio::spawn(net::run(
        listener,
        shutdown::watch_for_signal(shutdown_future),
        no_op_handler,
    ));

    // No wait is needed: the listener is already bound, so the connection is
    // queued until the server task accepts it.
    // Act & Assert: Attempt to connect to the server.
    match TcpStream::connect(addr).await {
        Ok(_) => {
            // Success! The server accepted our connection.
        }
        Err(e) => {
            panic!("failed to connect to server: {e}");
        }
    }
}
