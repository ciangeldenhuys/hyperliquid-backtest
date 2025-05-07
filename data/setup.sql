CREATE TYPE TRADETYPE AS ENUM ('spot', 'futures', 'options');

CREATE TABLE coin_pair (
    coin_id SERIAL PRIMARY KEY,
    symbol TEXT UNIQUE NOT NULL
);

CREATE TABLE trades (
    trade_id BIGINT PRIMARY KEY,
    coin_id INTEGER NOT NULL REFERENCES coin_pair(coin_id),
    trade_time TIMESTAMP NOT NULL,
    price NUMERIC NOT NULL,
    quantity NUMERIC NOT NULL,
    side BOOLEAN NOT NULL,
    best_match BOOLEAN NOT NULL,
    trade_type TRADETYPE NOT NULL
);

CREATE INDEX trades_index ON trades (coin_id, trade_time DESC);