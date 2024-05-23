import os
import asyncio
from datetime import datetime, timedelta
import random
from dotenv import load_dotenv
from faker import Faker
from sqlalchemy import MetaData, Table, Column, Integer, String, Float, DateTime, ForeignKey, Index, Numeric, and_, cast, insert, inspect, select, func, text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.sql import exists

from validation import validate_populate_table_params, validate_query_for_user_params, validate_query_users_params

# Load environment variables from .env file
load_dotenv()

# Read the environment variable
DATABASE_URL = os.getenv('DATABASE_URL')

if not DATABASE_URL:
    raise ValueError("No DATABASE_URL set")

metadata = MetaData()

# Define users table
users_table = Table(
    'users',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('name', String(16)),
    Column('gender', String(1)),
    Column('age', Integer)
)

# Define heart rates table
heart_rates_table = Table(
    'heart_rates',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('user_id', Integer, ForeignKey('users.id'), index=True),
    Column('timestamp', DateTime),
    Column('heart_rate', Float),
)

# Create a multi-column index on id, age, and gender in the users table
Index('ix_users_id_age_gender', users_table.c.id, users_table.c.age, users_table.c.gender)

# Create tables if don't exist
async def check_and_create_tables(engine):
    async with engine.connect() as conn:
        if 'users' not in metadata.tables or 'heart_rates' not in metadata.tables:
            await conn.run_sync(metadata.create_all)

# Populate tables couroutine
async def populate_tables(engine, num_of_users, num_of_hr_records, age_from, age_to):
    """
    Populate the users and heart_rates tables with sample data.
    """

    # Validate query parameters from user input
    validate_populate_table_params(num_of_users, num_of_hr_records, age_to, age_from)
    
    async with engine.connect() as conn:
        # Array to store users data before inserting into the db
        users = []
        # Initialize Faker for generating dummy usernames
        fake = Faker()
        for _ in range(num_of_users):
            # Generating dummy values for each record in 'users' table 
            user = {
                'name': fake.name(),
                'gender': random.choice(['M', 'F']),
                'age': random.randint(age_from, age_to)
            }
            users.append(user)
        # Insert users
        await conn.execute(insert(users_table).values(users))

        # Fetch the inserted user ids
        query_ids = select(users_table.c.id)
        user_ids = (await conn.execute(query_ids)).scalars().all()
        
        # Array to store heart rates data before inserting into the db
        heart_rates = []
        # Create the initial timestamp
        start_date = datetime.now() - timedelta(days=31)  # Last 31 days
        for user_id in user_ids:
            for i in range(num_of_hr_records):  # number of records per user
                current_timestamp = start_date + timedelta(seconds=30 * i)  # Increment by 30 seconds
                heart_rate_record = {
                    'user_id': user_id,
                    'timestamp': current_timestamp,  # Take a record every 30 sec
                    'heart_rate': round(random.uniform(40, 180), 1)  # Random heart rate between 40 and 180 rounded to 1 decimal
                }
                heart_rates.append(heart_rate_record)
        # Insert heart rate records for each user        
        await conn.execute(insert(heart_rates_table).values(heart_rates))
        await conn.commit()

async def database_exists(engine):
    """
    Check if the database exists by querying both of the tables.
    """

    async with engine.connect() as conn:
        # Check if any records exist in the users table
        users_exist_query = select(exists().where(users_table.c.id != None))
        users_exist = (await conn.execute(users_exist_query)).scalar()

        # Check if any records exist in the heart_rates table
        heart_rates_exist_query = select(exists().where(heart_rates_table.c.id != None))
        heart_rates_exist = (await conn.execute(heart_rates_exist_query)).scalar()

        # If records exist in both tables, return True (database exists)
        return users_exist and heart_rates_exist


async def query_users(engine, min_age, gender, min_avg_heart_rate, date_from, date_to):
    """
    Query to return all users older than 'min_age' and having an average heart rate 
    higher than 'min_avg_heart_rate' within a certain time period.
    """
    # Validate query parameters from user input
    validate_query_users_params(min_age, gender, min_avg_heart_rate, date_from, date_to)
    
    async with engine.connect() as conn:
        # Filter users by age and gender first
        filtered_users = select(users_table.c.id).where(
            and_(
                users_table.c.age > min_age,
                users_table.c.gender == gender.upper()
            )
        ).alias('filtered_users')
        
        # Subquery to calculate average heart rate for filtered users
        subquery = (
            select(
                heart_rates_table.c.user_id,
                cast(func.round(cast(func.avg(heart_rates_table.c.heart_rate), Numeric), 2), Float).label('avg_heart_rate')
            )
            .where(
                and_(
                    heart_rates_table.c.timestamp >= date_from,
                    heart_rates_table.c.timestamp <= date_to,
                    heart_rates_table.c.user_id.in_(select(filtered_users.c.id))
                )
            )
            .group_by(heart_rates_table.c.user_id)
            # Filter avg heart_rates
            .having(func.avg(heart_rates_table.c.heart_rate) > min_avg_heart_rate)
            .alias('avg_heart_rates')
        )

        # Main query to get user names from the filtered query
        query = (
            select(users_table.c.name)
            .select_from(users_table.join(
                subquery, 
                users_table.c.id == subquery.c.user_id)
            )
        )

        result = await conn.execute(query)
        return result.fetchall()

async def query_for_user(engine,user_id,date_from,date_to):
    """
    Query that returns the top 10 highest average heart rate readings
    over hourly intervals within a specified period.
    """
    # Validate query parameters from user input
    validate_query_for_user_params(user_id, date_from, date_to)
    
    async with engine.connect() as conn:
        # Subquery to calculate average heart rate per hourly slot
        subquery = (
            select(
                heart_rates_table.c.user_id,
                # Truncate timestamp to hour to group by hourly slots
                func.date_trunc('hour', heart_rates_table.c.timestamp).label('hour'),
                # Calculate average heart rate using window function
                func.avg(heart_rates_table.c.heart_rate).over(
                    partition_by=[
                        heart_rates_table.c.user_id,
                        func.date_trunc('hour', heart_rates_table.c.timestamp)
                    ]
                ).label('avg_heart_rate')
            )
            # Filter records by user_id and the specified date range
            .where(
                heart_rates_table.c.user_id == user_id,
                heart_rates_table.c.timestamp >= date_from,
                heart_rates_table.c.timestamp <= date_to
            )
            # Ensure unique rows by using distinct
            .distinct()
        ).alias('hourly_avg_heart_rates')

        main_query = (
            select(subquery.c.avg_heart_rate)
            .order_by(subquery.c.avg_heart_rate.desc())
            .limit(10)
        )

        result = await conn.execute(main_query)
        return result.fetchall()

        
async def main():
    # Parameters
    # DB seeding parameters
    days_of_monitoring = 31
    hr_records_per_hour = 60 * 2
    hr_records = days_of_monitoring * 24 * hr_records_per_hour
    populate_db_params = {
        'users': 10,
        'days_of_monitoring': days_of_monitoring,
        'hr_records_per_hour': hr_records_per_hour,
        'hr_records': hr_records,
        'age_from': 35,
        'age_to': 50
    }
    # Query parameters
    query_users_params = {
        'date_from' : datetime(2023, 1, 1),
        'date_to' : datetime(2024, 5, 18),
        'min_avg_heart_rate': 70,
        'min_age': 40,
        'gender': 'm'
    }
    query_for_user_params = {
        'user_id': 1,
        'date_from' : datetime(2023, 1, 1),
        'date_to' :datetime(2024, 5, 18)   
    }

    # Create async engine
    engine = create_async_engine(DATABASE_URL, echo=False)

    # Create tables if they don't exist
    await check_and_create_tables(engine)

    # Check if the database is accessible and if tables are empty
    if not await database_exists(engine):
        await populate_tables(engine, **populate_db_params)

    # Execute query_users with the provided parameters
    query_users_results = await query_users(engine, **query_users_params)
    print(f"\n{query_users.__name__.capitalize()} results:")
    for row in query_users_results:
        print(row[0])

    query_for_user_results = await query_for_user(engine, **query_for_user_params)
    print(f"\n{query_for_user.__name__.capitalize()} results:")
    for row in query_for_user_results:
        print(round(row[0],2))

if __name__ == "__main__":
    asyncio.run(main())
