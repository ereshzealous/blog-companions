-- Budget ledger for the bidder. All amounts are integer micros (1 USD = 1,000,000) so pacing
-- arithmetic never touches floating point.

CREATE TABLE IF NOT EXISTS campaign_budget (
    campaign_id                   BIGINT      PRIMARY KEY,
    target_country                CHAR(2)     NOT NULL,
    max_bid_micros                BIGINT      NOT NULL CHECK (max_bid_micros > 0),
    total_cap_micros              BIGINT      NOT NULL CHECK (total_cap_micros >= 0),
    remaining_micros              BIGINT      NOT NULL CHECK (remaining_micros >= 0),
    daily_pacing_threshold_micros BIGINT      NOT NULL CHECK (daily_pacing_threshold_micros >= 0),
    spent_today_micros            BIGINT      NOT NULL DEFAULT 0 CHECK (spent_today_micros >= 0),
    updated_at                    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS campaign_budget_country_idx ON campaign_budget (target_country);

-- One row per settled auction. The primary key makes win-notice debits idempotent: exchanges
-- retry win URLs, and Kafka redelivers after a consumer restart.
CREATE TABLE IF NOT EXISTS win_ledger (
    auction_id    VARCHAR(64) PRIMARY KEY,
    campaign_id   BIGINT      NOT NULL REFERENCES campaign_budget (campaign_id),
    price_micros  BIGINT      NOT NULL CHECK (price_micros > 0),
    settled_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
