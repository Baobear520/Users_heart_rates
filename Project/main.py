from datetime import datetime, timedelta
from psycopg2 import OperationalError
from sqlalchemy import FLOAT, Numeric, and_, cast, create_engine, desc, insert, select, type_coerce, values,func
from sqlalchemy import MetaData, Table, Column, Integer, String, Float, DateTime, ForeignKey
import random
from faker import Faker

# Setup the PostgreSQL engine
engine = create_engine('postgresql+psycopg2://aldmikon@localhost:5432/template1')
metadata = MetaData()

# Define users table
users_table = Table(
    'users',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('name', String(32)),
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
    Column('heart_rate', Float(1)),
)

def create_tables():
    metadata.create_all(engine)

# Initialize Faker
fake = Faker()

def populate_tables(num_of_users,num_of_hr_records):
    """
    Populate the users and heart_rates tables with sample data.
    - 10 users
    - 892 800 heart rate records 
    (2880 records per user per day within 31 days)
    """
    with engine.connect() as connection:
        # Insert 50 users
        users = []
        for i in range(num_of_users):
            user = {
                'name': fake.name(),
                'gender': random.choice(['M', 'F']),
                'age': random.randint(35, 50)
            }
            users.append(user)
        connection.execute(insert(users_table).values(users))

        # Fetch the inserted user ids
        query_ids = select(users_table.c.id)
        user_ids = connection.execute(query_ids).fetchall()
        user_ids = [row[0] for row in user_ids]

        # Insert 223000 heart rate records
        heart_rates = []
        start_date = datetime.now() - timedelta(days=31)  # Last 365 days
        for user_id in user_ids:
            for i in range(num_of_hr_records): #number of records per user
                current_timestamp = start_date + timedelta(seconds=30 * i)  # Increment by 30 seconds
                heart_rate_record = {
                    'user_id': user_id,
                    'timestamp': current_timestamp,  # Take a record every 30 sec
                    'heart_rate': round(random.uniform(50, 100), 1)  # Random heart rate between 50 and 100 rounded to 1 decimal
                }
                heart_rates.append(heart_rate_record)
                
        connection.execute(insert(heart_rates_table).values(heart_rates))
        connection.commit()

def is_database_empty():
    """
    Check if the database is empty by querying one of the tables.
    """
    with engine.connect() as connection:
        result = connection.execute(select(func.count(users_table.c.id)))
        count = result.scalar()
        return count == 0

def query_users(min_age, gender, min_avg_heart_rate, date_from, date_to):
    """
    Запрос, который возвращает всех пользователей, которые старше'min_age' и 
    имеют средний пульс выше, чем 'min_avg_heart_rate', на определенном промежутке времени
    """

    with engine.connect() as connection:
        # Filter users by age and gender first
        filtered_users = select(users_table.c.id).where(
            and_(
                users_table.c.age > min_age,
                users_table.c.gender == gender
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
            .having(func.avg(heart_rates_table.c.heart_rate) > min_avg_heart_rate)
            .alias('avg_heart_rates')
        )

        # Main query to get user details
        query = (
            select(
                users_table.c.id,
                users_table.c.name,
                users_table.c.age,
                subquery.c.avg_heart_rate
                )
            .select_from(users_table.join(subquery, users_table.c.id == subquery.c.user_id))
        )

        result = connection.execute(query)
        return result.fetchall()
    
def query_for_user(user_id, date_from, date_to):
    """ 
    Запрос, который возвращает топ 10 самых высоких средних показателей 'heart_rate' 
    за часовые промежутки в указанном периоде 'date_from' и 'date_to'
    """
    
    with engine.connect() as connection:
        subquery = (
            select(
                heart_rates_table.c.user_id.label('user_id'),
                func.date_trunc('hour', heart_rates_table.c.timestamp).label('hour'),
                cast(func.round(cast(func.avg(heart_rates_table.c.heart_rate), Numeric), 2), Float).label('avg_heart_rate')
            )
            .where(
                and_(
                    heart_rates_table.c.user_id == user_id,
                    heart_rates_table.c.timestamp >= date_from,
                    heart_rates_table.c.timestamp <= date_to
                )
            )
            .group_by(heart_rates_table.c.user_id, func.date_trunc('hour', heart_rates_table.c.timestamp))
            .alias('hourly_avg_heart_rates')
        )

        query = (
            select(
                subquery.c.user_id,
                subquery.c.avg_heart_rate)
            .order_by(subquery.c.avg_heart_rate.desc())
            .limit(10)
        )
        result = connection.execute(query)
        return result.fetchall()


if __name__ == "__main__":
    #Parameters
    #DB seeding parameters
    users = 10
    days_of_monitoring = 31
    hr_records_per_hour = 60 * 2
    hr_records = users * days_of_monitoring * 24 * hr_records_per_hour
    
    #Query parameters
    date_from = datetime(2023, 1, 1)
    date_to = datetime(2024, 5, 18)
    min_avg_heart_rate = 70.0
    min_age = 40
    gender = 'M'
    user_id = 1

    # Check if the tables exist and if they are empty
    try:
        with engine.connect() as connection:
            # Try a simple query to check if the database is accessible
            connection.execute(select(1))
    except OperationalError:
        # If the connection fails, the database does not exist
        create_tables()
        populate_tables(num_of_users=users,num_of_hr_records=hr_records)
    
    # If the database is accessible, check if the tables are empty
    if is_database_empty():
        populate_tables(num_of_users=users,num_of_hr_records=hr_records)


    #Execute query_users with the provided parameters
    query_users_results = query_users(min_age, gender, min_avg_heart_rate, date_from, date_to)
    print("\nQuery Results:")
    for row in query_users_results:
        print(row)

    # query_for_user_results = query_for_user(user_id, date_from, date_to)
    # print("\nQuery Results:")
    # for row in query_for_user_results:
    #     print(row)

    