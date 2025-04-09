import boto3
import os
import logging
from datetime import datetime, timedelta

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize the Redshift Data API client
client = boto3.client('redshift-data')

def lambda_handler(event, context):
    # Redshift connection parameters (from Lambda environment variables)
    cluster_id = os.environ['REDSHIFT_CLUSTER_ID']  # Your Redshift cluster name
    database = os.environ['REDSHIFT_DATABASE']      # Your Redshift database name
    db_user = os.environ['REDSHIFT_USER']           # Your Redshift username
    # (No password needed when using direct credentials with Data API)

    # Calculate date parameters
    today = datetime.now().date()
    week_start = today - timedelta(days=today.weekday())
    month_start = today.replace(day=1)

    # Define all queries (same as before)
    queries = [
            ("weekly_avg_listing_price", f"""
                -- Lambda would run this weekly
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
            """),
            
            ("monthly_occupancy_rate", f"""
                -- Lambda would run this monthly
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
            """),
            
            ("weekly_popular_locations", f"""
                                -- Lambda would run this weekly
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
            """),
            
            ("weekly_top_performing_listings", f"""
                -- Lambda would run this weekly
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
            """),
            
            ("weekly_user_bookings", f"""
                -- Lambda would run this weekly
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
                                """),
            
            ("avg_booking_duration", f"""
                -- Lambda would run this daily/weekly
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
            """),
            
            ("repeat_customer_rate", f"""
                -- Lambda would run this daily
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
            """)
    ]
    try:
        # Execute all queries in sequence
        for query_name, query in queries:
            try:
                logger.info(f"Executing {query_name} query...")
                
                response = client.execute_statement(
                    ClusterIdentifier=cluster_id,  # Your cluster name
                    Database=database,            # Your DB name
                    DbUser=db_user,               # Your Redshift username
                    Sql=query,
                    StatementName=query_name
                )
                
                statement_id = response['Id']
                logger.info(f"Started execution of {query_name} (Statement ID: {statement_id})")
                
            except Exception as e:
                logger.error(f"Error executing {query_name}: {str(e)}")
                continue  # Proceed to next query even if one fails
        
        logger.info("All queries executed successfully (async).")
        
    except Exception as e:
        logger.error(f"Redshift Data API error: {str(e)}")
        raise e
    
    return {
        'statusCode': 200,
        'body': 'All metric calculations initiated'
    }