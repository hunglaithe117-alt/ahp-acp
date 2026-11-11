//! Demo AHP client — connects two clients (Alice and Bob) to the AHP host,
//! subscribes to channels, dispatches actions, and observes broadcasts.
//!
//! Prerequisites: start the AHP host first:
//!   cargo run -p ahp-host

use ahp::{Client, ClientConfig, SubscriptionEvent};
use ahp_types::{
    actions::{
        ChatDeltaAction, ChatTurnCompleteAction, ChatTurnStartedAction, StateAction,
    },
    common::ROOT_RESOURCE_URI,
    state::{Message, MessageKind, MessageOrigin},
    PROTOCOL_VERSION,
};
use ahp_ws::WebSocketTransport;

const WS_URL: &str = "ws://localhost:3001/ws/ahp";
const SESSION_URI: &str = "ahp-session:/demo";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "ahp_client_demo=info,ahp=info".into()),
        )
        .init();

    tracing::info!("Connecting Alice...");
    let alice = run_client("alice", "Alice").await?;

    tracing::info!("Connecting Bob...");
    let bob = run_client("bob", "Bob").await?;

    // Give the server a moment to process.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Alice dispatches a turn-started action on the session channel.
    tracing::info!("[Alice] dispatching chat/turnStarted...");
    let turn_id = "turn-001";
    let action = StateAction::ChatTurnStarted(ChatTurnStartedAction {
        turn_id: turn_id.into(),
        started_at: "2025-01-01T00:00:00Z".into(),
        message: Message {
            text: "Hello from Alice!".into(),
            origin: MessageOrigin {
                kind: MessageKind::User,
            },
            attachments: None,
            model: None,
            agent: None,
            meta: None,
        },
        queued_message_id: None,
        meta: None,
    });
    alice.client.dispatch(SESSION_URI.into(), action).await?;

    // Alice sends a delta.
    tracing::info!("[Alice] dispatching chat/delta...");
    let action = StateAction::ChatDelta(ChatDeltaAction {
        turn_id: turn_id.into(),
        part_id: "part-1".into(),
        content: "I'm working on the AHP protocol demo.".into(),
        meta: None,
    });
    alice.client.dispatch(SESSION_URI.into(), action).await?;

    // Alice completes the turn.
    tracing::info!("[Alice] dispatching chat/turnComplete...");
    let action = StateAction::ChatTurnComplete(ChatTurnCompleteAction {
        turn_id: turn_id.into(),
        duration: 1500,
        meta: None,
    });
    alice.client.dispatch(SESSION_URI.into(), action).await?;

    // Wait for events to propagate.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    tracing::info!("Demo complete. Shutting down...");
    alice.client.shutdown().await;
    bob.client.shutdown().await;

    Ok(())
}

struct DemoClient {
    client: Client,
}

async fn run_client(
    client_id: &str,
    display_name: &str,
) -> Result<DemoClient, Box<dyn std::error::Error>> {
    let transport = WebSocketTransport::connect(WS_URL).await?;
    let client = Client::connect(transport, ClientConfig::default()).await?;

    let init = client
        .initialize(
            client_id.into(),
            vec![PROTOCOL_VERSION.to_string()],
            vec![ROOT_RESOURCE_URI.to_string(), SESSION_URI.into()],
        )
        .await?;

    tracing::info!(
        name = display_name,
        protocol = %init.protocol_version,
        server_seq = init.server_seq,
        "initialized"
    );

    // Subscribe to root channel.
    let (_root_snap, _root_sub) = client.subscribe(ROOT_RESOURCE_URI.into()).await?;
    tracing::info!(name = display_name, "subscribed to root");

    // Subscribe to session channel.
    let (snap, mut session_sub) = client.subscribe(SESSION_URI.into()).await?;
    tracing::info!(
        name = display_name,
        snapshot = ?snap.snapshot.as_ref().map(|s| &s.resource),
        "subscribed to session"
    );

    // Spawn a task to receive events.
    let name = display_name.to_string();
    tokio::spawn(async move {
        while let Some(ev) = session_sub.recv().await {
            match ev {
                SubscriptionEvent::Action(env) => {
                    tracing::info!(
                        client = %name,
                        seq = env.server_seq,
                        action = ?env.action,
                        "received action"
                    );
                }
                SubscriptionEvent::SessionAdded(p) => {
                    tracing::info!(client = %name, session = %p.summary.resource, "session added");
                }
                SubscriptionEvent::SessionRemoved(p) => {
                    tracing::info!(client = %name, session = %p.session, "session removed");
                }
                _ => {
                    tracing::debug!(client = %name, "other subscription event");
                }
            }
        }
        tracing::info!(client = %name, "subscription ended");
    });

    Ok(DemoClient {
        client,
    })
}
