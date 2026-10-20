//! Fragmented client frames must survive event delivery during their read.

use std::pin::Pin;
use std::task::{Context, Poll};

use pa_types::platform::transport::{AsyncReadHalf, AsyncWriteHalf};
use tokio::io::{AsyncRead, ReadBuf};

use super::*;
use crate::supervisor::SupervisorOptions;

struct ObservedStream {
    stream: tokio::net::UnixStream,
    prefix_bytes: usize,
    consumed: tokio::sync::oneshot::Sender<()>,
}

impl TransportStream for ObservedStream {
    fn split(self: Box<Self>) -> (Box<dyn AsyncReadHalf>, Box<dyn AsyncWriteHalf>) {
        let (reader, writer) = self.stream.into_split();
        (
            Box::new(ObservedReader {
                reader,
                prefix_bytes: self.prefix_bytes,
                bytes_read: 0,
                consumed: Some(self.consumed),
            }),
            Box::new(writer),
        )
    }
}

struct ObservedReader {
    reader: tokio::net::unix::OwnedReadHalf,
    prefix_bytes: usize,
    bytes_read: usize,
    consumed: Option<tokio::sync::oneshot::Sender<()>>,
}

impl AsyncRead for ObservedReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let before = buf.filled().len();
        let result = Pin::new(&mut self.reader).poll_read(cx, buf);
        self.bytes_read += buf.filled().len() - before;
        let at_fragment_boundary = result.is_pending() && self.bytes_read >= self.prefix_bytes;
        if let Some(consumed) = self.consumed.take_if(|_| at_fragment_boundary) {
            let _ = consumed.send(());
        }
        result
    }
}

#[tokio::test]
async fn fragmented_client_frame_survives_interleaved_broadcast() {
    let dir = tempfile::tempdir().unwrap();
    let supervisor = Arc::new(
        Supervisor::new(SupervisorOptions {
            socket_path: dir.path().join("daemon.sock"),
            agent_dir: dir.path().join("agent"),
        })
        .unwrap(),
    );
    let envelope = json!({
        "type":"command", "id":"large-frame",
        "protocol":{"name":"prime-agent.daemon", "version":7},
        "command":{"type":"list", "all":true, "padding":"x".repeat(120_000)}
    });
    let mut bytes = serde_json::to_vec(&envelope).unwrap();
    bytes.push(b'\n');
    let prefix_bytes = 60_000;
    let (consumed_tx, consumed_rx) = tokio::sync::oneshot::channel();
    let (server_side, client_side) = tokio::net::UnixStream::pair().unwrap();
    let connection = {
        let supervisor = Arc::clone(&supervisor);
        tokio::spawn(async move {
            supervisor
                .handle_client(Box::new(ObservedStream {
                    stream: server_side,
                    prefix_bytes,
                    consumed: consumed_tx,
                }))
                .await
        })
    };
    let (reader, mut writer) = client_side.into_split();
    let mut client = BufReader::new(reader).lines();
    let hello = tokio::time::timeout(Duration::from_secs(5), client.next_line())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&hello).unwrap()["type"],
        "daemon_hello"
    );
    writer.write_all(&bytes[..prefix_bytes]).await.unwrap();
    // This readiness barrier proves the server consumed the prefix and is
    // waiting for the suffix; no scheduling sleep can hide a lost frame.
    tokio::time::timeout(Duration::from_secs(5), consumed_rx)
        .await
        .unwrap()
        .unwrap();
    let broadcast = json!({"type":"model_catalog_changed", "marker":"between-fragments"});
    supervisor
        .events
        .send((ClientRouting::Broadcast, Arc::new(broadcast.clone())))
        .unwrap();
    let event = tokio::time::timeout(Duration::from_secs(5), client.next_line())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(serde_json::from_str::<Value>(&event).unwrap(), broadcast);
    // Reading the broadcast proves its select arm cancelled the incomplete read.
    writer.write_all(&bytes[prefix_bytes..]).await.unwrap();
    for expected in ["large-frame", "next-frame"] {
        if expected == "next-frame" {
            let next = json!({"type":"command", "id":expected,
                "protocol":{"name":"prime-agent.daemon", "version":7},
                "command":{"type":"list", "all":true}});
            writer
                .write_all((next.to_string() + "\n").as_bytes())
                .await
                .unwrap();
        }
        let response = tokio::time::timeout(Duration::from_secs(5), client.next_line())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let response: Value = serde_json::from_str(&response).unwrap();
        assert_eq!(
            response["id"], expected,
            "client frame correlation was lost: {response}"
        );
        assert_eq!(response["success"], true, "{response}");
        assert_eq!(response["data"]["sessions"], json!([]));
    }
    writer.shutdown().await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), connection)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}
