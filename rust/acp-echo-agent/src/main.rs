use agent_client_protocol::schema::v1::{
    AgentCapabilities, ContentBlock, ContentChunk, InitializeRequest, InitializeResponse,
    NewSessionRequest, NewSessionResponse, PromptRequest, PromptResponse, SessionId,
    SessionNotification, SessionUpdate, StopReason, TextContent,
};
use agent_client_protocol::{Agent, Stdio};
use tracing::info;

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "acp_echo_agent=info".into()),
        )
        .init();

    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(run())
}

async fn run() -> anyhow::Result<()> {
    info!("Starting ACP echo agent (Rust)");

    Agent
        .builder()
        .name("acp-echo-agent")
        .on_receive_request(
            async |req: InitializeRequest, responder, _connection| {
                info!(
                    protocol_version = ?req.protocol_version,
                    "Received initialize request"
                );
                responder.respond(
                    InitializeResponse::new(req.protocol_version)
                        .agent_capabilities(AgentCapabilities::new()),
                )
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async |_req: NewSessionRequest, responder, _connection| {
                let session_id = SessionId::new("session-1");
                info!(?session_id, "Created new session");
                responder.respond(NewSessionResponse::new(session_id))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async |req: PromptRequest, responder, connection| {
                info!(
                    session_id = ?req.session_id,
                    blocks = req.prompt.len(),
                    "Received prompt"
                );

                // Stream a header chunk
                connection.send_notification(SessionNotification::new(
                    req.session_id.clone(),
                    SessionUpdate::AgentMessageChunk(ContentChunk::new(
                        ContentBlock::Text(TextContent::new("Echo: ")),
                    )),
                ))?;

                // Echo each text content block back to the client
                for block in &req.prompt {
                    if let ContentBlock::Text(text_content) = block {
                        connection.send_notification(SessionNotification::new(
                            req.session_id.clone(),
                            SessionUpdate::AgentMessageChunk(ContentChunk::new(
                                ContentBlock::Text(TextContent::new(&text_content.text)),
                            )),
                        ))?;
                    }
                }

                responder.respond(PromptResponse::new(StopReason::EndTurn))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .connect_to(Stdio::new())
        .await?;

    Ok(())
}
