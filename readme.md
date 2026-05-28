# Clickhouse API
And various scripts 


CREATE TABLE msg
(
    msg_id               UInt64,
    chat_id              Int64,
    chat_name            String,
    username             String,
    sender_chat_id       Int64,
    title                String,
    date_utc             DateTime('UTC'),
    insert_date_utc      DateTime('UTC'),
    document_present     UInt8,
    document_name        String,
    document_type        String,
    document_size        UInt64,
    msg_fwd              UInt8,
    msg_fwd_username     String,
    msg_fwd_title        String,
    msg_fwd_id           UInt64,
    text                 String,
    lang                 String,
    urls                 Array(String),
    hashtags             Array(String)
)
ENGINE = MergeTree
ORDER BY msg_id;
