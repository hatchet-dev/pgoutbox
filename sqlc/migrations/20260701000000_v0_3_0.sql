-- +goose Up
-- +goose StatementBegin
CREATE TABLE consumer_sessions (
    consumer_id UUID        NOT NULL,
    expires_at  TIMESTAMPTZ NOT NULL,
    CONSTRAINT consumer_sessions_pkey PRIMARY KEY (consumer_id)
);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS consumer_sessions;
-- +goose StatementEnd
