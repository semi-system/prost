use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test(start_paused = true)]
async fn disabled_idle_keeps_silent_bidirectional_sessions_and_half_close() -> Result<()> {
    let (mut client, front) = tokio::io::duplex(64);
    let (back, mut service) = tokio::io::duplex(64);
    let relay = tokio::spawn(relay_with_timeouts(
        Box::new(front),
        Box::new(back),
        Duration::ZERO,
        None,
    ));
    tokio::time::advance(Duration::from_secs(601)).await;
    client.write_all(b"IDLE\r\n").await?;
    let mut request = [0; 6];
    service.read_exact(&mut request).await?;
    assert_eq!(&request, b"IDLE\r\n");
    service.write_all(b"+ idling\r\n").await?;
    let mut response = [0; 10];
    client.read_exact(&mut response).await?;
    assert_eq!(&response, b"+ idling\r\n");
    client.shutdown().await?;
    service.shutdown().await?;
    assert_eq!(relay.await??, 16);
    Ok(())
}
