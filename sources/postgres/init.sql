CREATE TABLE IF NOT EXISTS accounts (
    account_id      UUID PRIMARY KEY,
    holder_name     TEXT NOT NULL,
    email           TEXT NOT NULL,
    country         CHAR(2) NOT NULL DEFAULT 'CR',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS transactions (
    transaction_id  UUID PRIMARY KEY,
    account_id      UUID NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
    merchant        TEXT NOT NULL,
    amount          NUMERIC(12, 2) NOT NULL,
    currency        CHAR(3) NOT NULL,
    status          TEXT NOT NULL DEFAULT 'confirmed',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_transactions_account_id ON transactions(account_id);
CREATE INDEX IF NOT EXISTS idx_transactions_created_at ON transactions(created_at);

CREATE TABLE IF NOT EXISTS fraud_labels (
    transaction_id  UUID PRIMARY KEY REFERENCES transactions(transaction_id) ON DELETE CASCADE,
    is_fraud        BOOLEAN NOT NULL,
    fraud_pattern   TEXT,
    labeled_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
