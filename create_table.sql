CREATE TABLE responses (
    agency VARCHAR(255),
    km DOUBLE PRECISION,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    longname VARCHAR(255),
    number INTEGER,
    shortname VARCHAR(255),
    uuid VARCHAR(255) PRIMARY KEY,
    water_longname VARCHAR(255),
    water_shortname VARCHAR(255)
);

create table timeseries_item(
	item_id VARCHAR(255) PRIMARY KEY,
	uuid VARCHAR(255)
);

create table timeseries_attribute(
	attribute VARCHAR(1000) PRIMARY KEY
);

create table timeseries_value (
	id SERIAL PRIMARY KEY,
	item_id VARCHAR(255),
	attribute VARCHAR(255),
	value TEXT
);