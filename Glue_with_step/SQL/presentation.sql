create schema presentation;

CREATE TABLE presentation.average_listing_price (
    week_start_date DATE,
    average_price DOUBLE PRECISION
);

INSERT INTO presentation.average_listing_price (week_start_date, average_price)
SELECT 
    DATE_TRUNC('week', TO_DATE(listing_created_on, 'YYYY-MM-DD')) AS week_start_date,
    AVG(price) AS average_price
FROM 
    raw_data.raw_apartments
WHERE 
    is_active = 1
GROUP BY 
    DATE_TRUNC('week', TO_DATE(listing_created_on, 'YYYY-MM-DD'));







CREATE TABLE presentation.occupancy_rate (
    month DATE,
    occupancy_rate DOUBLE PRECISION
);


INSERT INTO presentation.occupancy_rate (month, occupancy_rate)
SELECT 
    DATE_TRUNC('month', TO_DATE(checkin_date, 'YYYY-MM-DD')) AS month,
    (SUM(DATEDIFF(day, TO_DATE(checkin_date, 'YYYY-MM-DD'), TO_DATE(checkout_date, 'YYYY-MM-DD'))) /
     (COUNT(*) * 30.0)) * 100 AS occupancy_rate
FROM 
    raw_data.raw_bookings
WHERE 
    booking_status = 'confirmed'
GROUP BY 
    DATE_TRUNC('month', TO_DATE(checkin_date, 'YYYY-MM-DD'));



CREATE TABLE presentation.most_popular_locations (
    week_start_date DATE,
    cityname VARCHAR(100),
    total_bookings BIGINT
);

INSERT INTO presentation.most_popular_locations (week_start_date, cityname, total_bookings)
SELECT 
    DATE_TRUNC('week', TO_DATE(booking_date, 'YYYY-MM-DD')) AS week_start_date,
    cityname,
    COUNT(*) AS total_bookings
FROM 
    raw_data.raw_bookings b
JOIN 
    raw_data.raw_apartment_attr a ON b.apartment_id = a.id
WHERE 
    booking_status = 'confirmed'
GROUP BY 
    DATE_TRUNC('week', TO_DATE(booking_date, 'YYYY-MM-DD')), cityname
ORDER BY 
    week_start_date, total_bookings DESC;


CREATE TABLE presentation.top_performing_listings (
    week_start_date DATE,
    apartment_id BIGINT,
    total_revenue DOUBLE PRECISION
);

INSERT INTO presentation.top_performing_listings (week_start_date, apartment_id, total_revenue)
SELECT 
    DATE_TRUNC('week', TO_DATE(booking_date, 'YYYY-MM-DD')) AS week_start_date,
    apartment_id,
    SUM(total_price) AS total_revenue
FROM 
    raw_data.raw_bookings
WHERE 
    booking_status = 'confirmed'
GROUP BY 
    DATE_TRUNC('week', TO_DATE(booking_date, 'YYYY-MM-DD')), apartment_id
ORDER BY 
    week_start_date, total_revenue DESC;


CREATE TABLE presentation.total_bookings_per_user (
    week_start_date DATE,
    user_id BIGINT,
    total_bookings BIGINT
);

INSERT INTO presentation.total_bookings_per_user (week_start_date, user_id, total_bookings)
SELECT 
    DATE_TRUNC('week', TO_DATE(booking_date, 'YYYY-MM-DD')) AS week_start_date,
    user_id,
    COUNT(*) AS total_bookings
FROM 
    raw_data.raw_bookings
WHERE 
    booking_status = 'confirmed'
GROUP BY 
    DATE_TRUNC('week', TO_DATE(booking_date, 'YYYY-MM-DD')), user_id;


CREATE TABLE presentation.average_booking_duration (
    month DATE,
    average_duration DOUBLE PRECISION
);



INSERT INTO presentation.average_booking_duration (month, average_duration)
SELECT 
    DATE_TRUNC('month', TO_DATE(checkin_date, 'YYYY-MM-DD')) AS month,
    AVG(DATEDIFF(day, TO_DATE(checkin_date, 'YYYY-MM-DD'), TO_DATE(checkout_date, 'YYYY-MM-DD'))) AS average_duration
FROM 
    raw_data.raw_bookings
WHERE 
    booking_status = 'confirmed'
GROUP BY 
    DATE_TRUNC('month', TO_DATE(checkin_date, 'YYYY-MM-DD'));


CREATE TABLE presentation.repeat_customer_rate (
    rolling_30_day_period_start DATE,
    repeat_customer_rate DOUBLE PRECISION
);

INSERT INTO presentation.repeat_customer_rate (rolling_30_day_period_start, repeat_customer_rate)
SELECT 
    rolling_30_day_period_start,
    (COUNT(DISTINCT CASE WHEN bookings_per_user > 1 THEN user_id END) * 100.0) / COUNT(DISTINCT user_id) AS repeat_customer_rate
FROM (
    SELECT 
        user_id,
        COUNT(*) AS bookings_per_user,
        DATE_TRUNC('day', TO_DATE(booking_date, 'YYYY-MM-DD')) AS rolling_30_day_period_start
    FROM 
        raw_data.raw_bookings
    WHERE 
        booking_status = 'confirmed'
    GROUP BY 
        user_id, DATE_TRUNC('day', TO_DATE(booking_date, 'YYYY-MM-DD'))
) AS subquery
GROUP BY 
    rolling_30_day_period_start;


