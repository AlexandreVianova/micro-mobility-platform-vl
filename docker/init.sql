-- Create user, db and tables if needed
CREATE USER ingest WITH password 'ingest';
CREATE DATABASE mobility_data;
GRANT ALL PRIVILEGES ON DATABASE mobility_data TO ingest;
\connect mobility_data;
CREATE SCHEMA IF NOT EXISTS stations;
GRANT USAGE ON SCHEMA stations TO ingest;

CREATE TABLE IF NOT EXISTS stations.status_history (
	id serial PRIMARY KEY,
	station_id VARCHAR(255) NOT NULL,
    timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    bikes INTEGER NOT NULL,
    free INTEGER NOT NULL,
    ebikes INTEGER NOT NULL,
    renting INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS stations.inventory (
    id VARCHAR(255) UNIQUE NOT NULL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    latitude NUMERIC NOT NULL,
    longitude NUMERIC NOT NULL,
    payment VARCHAR(255) ARRAY,
    payment_terminal BOOLEAN,
    has_ebikes BOOLEAN NOT NULL,
    slots INTEGER NOT NULL,
    last_updated INTEGER NOT NULL
);

GRANT ALL PRIVILEGES  ON ALL TABLES IN SCHEMA stations TO ingest;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA stations TO ingest;
