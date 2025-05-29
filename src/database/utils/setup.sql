DROP TABLE IF EXISTS trades;
DROP TABLE IF EXISTS coin_pair;
DROP TYPE IF EXISTS TRADETYPE;

CREATE TYPE TRADETYPE AS ENUM ('spot', 'futures', 'options');

CREATE TABLE coin_pair (
    coin_id SERIAL PRIMARY KEY,
    symbol TEXT UNIQUE NOT NULL
);

CREATE TABLE trades (
    trade_id BIGINT,
    coin_id INTEGER NOT NULL,
    trade_time TIMESTAMP NOT NULL,
    price NUMERIC NOT NULL,
    quantity NUMERIC NOT NULL,
    side BOOLEAN NOT NULL,
    trade_type TRADETYPE NOT NULL,
    PRIMARY KEY (trade_id, coin_id),
    CONSTRAINT coin_fk FOREIGN KEY (coin_id) REFERENCES coin_pair(coin_id)
);

CREATE INDEX trades_index ON trades (coin_id, trade_type, trade_time, trade_id);