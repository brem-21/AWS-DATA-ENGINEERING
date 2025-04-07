CREATE TABLE raw_data.raw_apartment_attr (
    id BIGINT,
    category VARCHAR(255),
    body VARCHAR(MAX),
    amenities VARCHAR(MAX),
    bathrooms BIGINT,
    bedrooms BIGINT,
    fee DOUBLE PRECISION,
    has_photo BIGINT,
    pets_allowed VARCHAR(100),
    price_display VARCHAR(100),
    price_type VARCHAR(100),
    square_feet BIGINT,
    address VARCHAR(255),
    cityname VARCHAR(100),
    state VARCHAR(100),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION
);


CREATE TABLE raw_data.raw_apartments (
    id BIGINT,
    title VARCHAR(255),
    source VARCHAR(100),
    price DOUBLE PRECISION,
    currency VARCHAR(10),
    listing_created_on VARCHAR(50),
    is_active BIGINT,
    last_modified_timestamp VARCHAR(50)
);


CREATE TABLE raw_data.raw_bookings (
    booking_id BIGINT,
    user_id BIGINT,
    apartment_id BIGINT,
    booking_date VARCHAR(50),
    checkin_date VARCHAR(50),
    checkout_date VARCHAR(50),
    total_price DOUBLE PRECISION,
    currency VARCHAR(10),
    booking_status VARCHAR(50)
);

CREATE TABLE raw_data.raw_user_viewing (
    user_id BIGINT,
    apartment_id BIGINT,
    viewed_at VARCHAR(50),
    is_wishlisted BIGINT,
    call_to_action VARCHAR(100)
);


SELECT * FROM stl_load_errors 
ORDER BY starttime DESC 
LIMIT 1;

SELECT * FROM raw_data.raw_apartments limit 5;