CREATE TABLE items (
    item_id SERIAL PRIMARY KEY,
    name VARCHAR(256) NOT NULL,
    type VARCHAR(64) NOT NULL
);

CREATE TABLE price_data(
    entry_id SERIAL PRIMARY KEY,
    item_id SERIAL NOT NULL,
    volume INTEGER NOT NULL,
    price REAL NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE
);