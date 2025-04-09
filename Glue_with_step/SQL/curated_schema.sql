CREATE TABLE IF NOT EXISTS curated.apartments (
    apartment_id VARCHAR(255) PRIMARY KEY,
    title VARCHAR(255),
    source VARCHAR(100),
    price DECIMAL(10,2),
    currency VARCHAR(3),
    listing_date DATE,
    is_active BOOLEAN,
    last_modified_timestamp TIMESTAMP,
    category VARCHAR(100),
    body TEXT,
    amenities TEXT,
    bathrooms INTEGER,
    bedrooms INTEGER,
    fee DECIMAL(10,2),
    has_photo BOOLEAN,
    pets_allowed BOOLEAN,
    price_display VARCHAR(100),
    price_type VARCHAR(50),
    square_feet INTEGER,
    address VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(100),
    latitude DECIMAL(9,6),
    longitude DECIMAL(9,6)
);

CREATE TABLE IF NOT EXISTS curated.bookings (
    booking_id VARCHAR(255) PRIMARY KEY,
    user_id VARCHAR(255),
    apartment_id VARCHAR(255),
    booking_date DATE,
    checkin DATE,
    checkout DATE,
    total_price DECIMAL(10,2),
    currency VARCHAR(3),
    status VARCHAR(50),
    FOREIGN KEY (apartment_id) REFERENCES curated.apartments(apartment_id)
);

CREATE TABLE IF NOT EXISTS curated.users (
    user_id VARCHAR(255),
    apartment_id VARCHAR(255),
    viewed_at TIMESTAMP,
    is_wishlisted BOOLEAN,
    call_to_action VARCHAR(100),
    FOREIGN KEY (apartment_id) REFERENCES curated.apartments(apartment_id)
);

SELECT * from curated.dim_users limit 5;