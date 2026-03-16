use std::future::Future;

tokio::task_local! {
    static TURN_SOURCE_MESSAGE_ID: String;
}

pub async fn with_turn_source_message_id<F, T>(source_message_id: String, future: F) -> T
where
    F: Future<Output = T>,
{
    TURN_SOURCE_MESSAGE_ID
        .scope(source_message_id, future)
        .await
}

pub fn current_turn_source_message_id() -> Option<String> {
    TURN_SOURCE_MESSAGE_ID.try_with(Clone::clone).ok()
}
