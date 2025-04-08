import psycopg2
import os
import logging
from datetime import datetime, timedelta

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    # Redshift connection parameters
    host = "redshift-cluster-1.corgdzmbeq2a.us-east-1.redshift.amazonaws.com"
    port = "5439"
    dbname = "dev"
    user = "awsuser"
    password = "Brempong123"
    
    # Calculate date parameters
    today = datetime.now().date()
    week_start = today - timedelta(days=today.weekday())
    month_start = today.replace(day=1)
    
    # Define all queries as variables
    avg_listing_price_query = f"""
        INSERT INTO presentation.weekly_avg_listing_price
        SELECT 
            DATE('{week_start}') AS week_start_date,
            AVG(price) AS avg_price,
            currency
        FROM 
            raw_data.raw_apartments
        WHERE 
            is_active = 1
            AND TO_DATE(listing_created_on, 'YYYY-MM-DD') >= DATEADD(day, -7, DATE('{week_start}'))
        GROUP BY 
            currency
        ON CONFLICT (week_start_date, currency) 
        DO UPDATE SET 
            avg_price = EXCLUDED.avg_price;
    """
    
    occupancy_rate_query = f"""
        WITH available_nights AS (
            SELECT 
                DATE('{month_start}') AS month_start_date,
                COUNT(*) * 30 AS total_available_nights
            FROM 
                raw_data.raw_apartments
            WHERE 
                is_active = 1
        ),
        booked_nights AS (
            SELECT 
                DATE('{month_start}') AS month_start_date,
                SUM(DATEDIFF(day, TO_DATE(checkin_date, 'YYYY-MM-DD'), TO_DATE(checkout_date, 'YYYY-MM-DD'))) AS booked_nights
            FROM 
                raw_data.raw_bookings
            WHERE 
                booking_status = 'confirmed'
                AND TO_DATE(checkin_date, 'YYYY-MM-DD') >= DATE('{month_start}')
                AND TO_DATE(checkout_date, 'YYYY-MM-DD') < DATEADD(month, 1, DATE('{month_start}'))
        )
        INSERT INTO presentation.monthly_occupancy_rate
        SELECT 
            a.month_start_date,
            a.total_available_nights,
            COALESCE(b.booked_nights, 0) AS booked_nights,
            (COALESCE(b.booked_nights, 0) * 100.0 / NULLIF(a.total_available_nights, 0)) AS occupancy_rate
        FROM 
            available_nights a
        LEFT JOIN 
            booked_nights b ON a.month_start_date = b.month_start_date
        ON CONFLICT (month_start_date) 
        DO UPDATE SET 
            total_available_nights = EXCLUDED.total_available_nights,
            booked_nights = EXCLUDED.booked_nights,
            occupancy_rate = EXCLUDED.occupancy_rate;
    """
    
    popular_locations_query = f"""
        WITH weekly_bookings AS (
            SELECT 
                a.cityname,
                a.state,
                COUNT(*) AS booking_count
            FROM 
                raw_data.raw_bookings b
            JOIN 
                raw_data.raw_apartment_attr a ON b.apartment_id = a.id
            WHERE 
                b.booking_status = 'confirmed'
                AND TO_DATE(b.booking_date, 'YYYY-MM-DD') >= DATE('{week_start}')
                AND TO_DATE(b.booking_date, 'YYYY-MM-DD') < DATEADD(day, 7, DATE('{week_start}'))
            GROUP BY 
                a.cityname, a.state
        )
        INSERT INTO presentation.weekly_popular_locations
        SELECT 
            DATE('{week_start}') AS week_start_date,
            cityname,
            state,
            booking_count,
            RANK() OVER (ORDER BY booking_count DESC) AS rank
        FROM 
            weekly_bookings
        ORDER BY 
            booking_count DESC
        LIMIT 10
        ON CONFLICT (week_start_date, cityname, state) 
        DO UPDATE SET 
            booking_count = EXCLUDED.booking_count,
            rank = EXCLUDED.rank;
    """
    
    top_performing_query = f"""
        WITH weekly_revenue AS (
            SELECT 
                b.apartment_id,
                a.title,
                attr.cityname,
                attr.state,
                SUM(b.total_price) AS total_revenue,
                b.currency,
                RANK() OVER (PARTITION BY b.currency ORDER BY SUM(b.total_price) DESC) AS rank
            FROM 
                raw_data.raw_bookings b
            JOIN 
                raw_data.raw_apartments a ON b.apartment_id = a.id
            JOIN 
                raw_data.raw_apartment_attr attr ON b.apartment_id = attr.id
            WHERE 
                b.booking_status = 'confirmed'
                AND TO_DATE(b.booking_date, 'YYYY-MM-DD') >= DATE('{week_start}')
                AND TO_DATE(b.booking_date, 'YYYY-MM-DD') < DATEADD(day, 7, DATE('{week_start}'))
            GROUP BY 
                b.apartment_id, a.title, attr.cityname, attr.state, b.currency
        )
        INSERT INTO presentation.weekly_top_performing_listings
        SELECT 
            DATE('{week_start}') AS week_start_date,
            apartment_id,
            title,
            cityname,
            state,
            total_revenue,
            currency,
            rank
        FROM 
            weekly_revenue
        WHERE 
            rank <= 10
        ON CONFLICT (week_start_date, apartment_id) 
        DO UPDATE SET 
            title = EXCLUDED.title,
            cityname = EXCLUDED.cityname,
            state = EXCLUDED.state,
            total_revenue = EXCLUDED.total_revenue,
            currency = EXCLUDED.currency,
            rank = EXCLUDED.rank;
    """
    
    user_bookings_query = f"""
        INSERT INTO presentation.weekly_user_bookings
        SELECT 
            DATE('{week_start}') AS week_start_date,
            user_id,
            COUNT(*) AS booking_count
        FROM 
            raw_data.raw_bookings
        WHERE 
            booking_status = 'confirmed'
            AND TO_DATE(booking_date, 'YYYY-MM-DD') >= DATE('{week_start}')
            AND TO_DATE(booking_date, 'YYYY-MM-DD') < DATEADD(day, 7, DATE('{week_start}'))
        GROUP BY 
            user_id
        ON CONFLICT (week_start_date, user_id) 
        DO UPDATE SET 
            booking_count = EXCLUDED.booking_count;
    """
    
    avg_duration_query = f"""
        INSERT INTO presentation.avg_booking_duration
        SELECT 
            DATE('{today}') AS calculation_date,
            AVG(DATEDIFF(day, TO_DATE(checkin_date, 'YYYY-MM-DD'), TO_DATE(checkout_date, 'YYYY-MM-DD'))) AS avg_duration_days
        FROM 
            raw_data.raw_bookings
        WHERE 
            booking_status = 'confirmed'
            AND TO_DATE(booking_date, 'YYYY-MM-DD') >= DATEADD(day, -30, DATE('{today}'))
        ON CONFLICT (calculation_date) 
        DO UPDATE SET 
            avg_duration_days = EXCLUDED.avg_duration_days;
    """
    
    repeat_customer_query = f"""
        WITH customer_bookings AS (
            SELECT 
                user_id,
                COUNT(*) AS booking_count
            FROM 
                raw_data.raw_bookings
            WHERE 
                booking_status = 'confirmed'
                AND TO_DATE(booking_date, 'YYYY-MM-DD') >= DATEADD(day, -30, DATE('{today}'))
            GROUP BY 
                user_id
        )
        INSERT INTO presentation.repeat_customer_rate
        SELECT 
            DATE('{today}') AS calculation_date,
            COUNT(*) AS total_customers,
            SUM(CASE WHEN booking_count > 1 THEN 1 ELSE 0 END) AS repeat_customers,
            (SUM(CASE WHEN booking_count > 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) AS repeat_rate
        FROM 
            customer_bookings
        ON CONFLICT (calculation_date) 
        DO UPDATE SET 
            total_customers = EXCLUDED.total_customers,
            repeat_customers = EXCLUDED.repeat_customers,
            repeat_rate = EXCLUDED.repeat_rate;
    """
    
    # Create a list of all queries with their names for logging
    queries = [
        ("weekly_avg_listing_price", avg_listing_price_query),
        ("monthly_occupancy_rate", occupancy_rate_query),
        ("weekly_popular_locations", popular_locations_query),
        ("weekly_top_performing_listings", top_performing_query),
        ("weekly_user_bookings", user_bookings_query),
        ("avg_booking_duration", avg_duration_query),
        ("repeat_customer_rate", repeat_customer_query)
    ]
    
    try:
        # Connect to Redshift
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password,
            connect_timeout=10
        )
        cursor = conn.cursor()
        
        # Execute all queries in sequence
        for query_name, query in queries:
            try:
                logger.info(f"Executing {query_name} query...")
                cursor.execute(query)
                conn.commit()
                logger.info(f"Successfully executed {query_name} query")
            except Exception as e:
                logger.error(f"Error executing {query_name} query: {str(e)}")
                conn.rollback()  # Rollback the current transaction if there's an error
                # Continue to next query even if one fails
        
        logger.info("All metric calculations completed")
        print(f)
        
    except Exception as e:
        logger.error(f"Database connection error: {str(e)}")
        raise e
    finally:
        if 'conn' in locals():
            conn.close()
    
    return {
        'statusCode': 200,
        'body': 'All metric calculations completed'
    }