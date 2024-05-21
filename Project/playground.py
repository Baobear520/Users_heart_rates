from datetime import datetime, timedelta
from sqlalchemy import and_, create_engine, desc, insert, select, func
from sqlalchemy import MetaData, Table, Column, Integer, String, Float, DateTime, ForeignKey
import random
from faker import Faker

# Setup the SQLite engine
engine = create_engine(
    "sqlite+pysqlite:///:memory:", 
    echo=True,
    pool_size=10, 
    max_overflow=20
)
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

def populate_tables():
    """
    Populate the users and heart_rates tables with sample data.
    - 10 users
    - 223200 heart rate records 
    (approximately 720 records per user per day within 31 days)
    """
    with engine.connect() as connection:
        # Insert 50 users
        users = []
        for _ in range(10):
            user = {
                'name': fake.name(),
                'gender': random.choice(['M', 'F']),
                'age': random.randint(35, 50)
            }
            users.append(user)
        connection.execute(insert(users_table), users)

        # Fetch the inserted user ids
        query_ids = select(users_table.c.id)
        user_ids = connection.execute(query_ids).fetchall()
        user_ids = [row[0] for row in user_ids]

        # Insert 223000 heart rate records
        heart_rates = []
        start_date = datetime.now() - timedelta(days=31)  # Last 365 days
        for user_id in user_ids:
            for _ in range(22320):
                heart_rate_record = {
                    'user_id': user_id,
                    'timestamp': start_date + timedelta(seconds=30),  # Take a record every 30 sec
                    'heart_rate': round(random.uniform(50, 100), 1)  # Random heart rate between 50 and 100 rounded to 1 decimal
                }
                heart_rates.append(heart_rate_record)
                start_date = heart_rate_record['timestamp']
                
        connection.execute(insert(heart_rates_table), heart_rates)
        connection.commit()

def query_users(min_age, gender, min_avg_heart_rate, date_from, date_to):
    """
    Запрос, который возвращает всех пользователей, которые старше'min_age' и 
    имеют средний пульс выше, чем 'min_avg_heart_rate', на определенном промежутке времени
    """

    # Subquery for average heart rates
    avg_heart_rates = select(
        heart_rates_table.c.user_id,
        func.avg(heart_rates_table.c.heart_rate).label('avg_hr')
    ).where(
        and_(
            heart_rates_table.c.timestamp > date_from,
            heart_rates_table.c.timestamp < date_to
        )
    ).group_by(
        heart_rates_table.c.user_id
    ).having(
        func.avg(heart_rates_table.c.heart_rate) > min_avg_heart_rate
    ).alias('avg_heart_rates')
    
    # Main query
    main_query = select(
        users_table.c.id,
        users_table.c.name,
        avg_heart_rates.c.avg_hr
    ).join(
        avg_heart_rates,
        users_table.c.id == avg_heart_rates.c.user_id
    ).where(
        and_(
            users_table.c.age > min_age,
            users_table.c.gender == gender
        )
    )

    with engine.connect() as connection:
        result = connection.execute(main_query)
        return result.fetchall()
    
def query_for_user(user_id, date_from, date_to):
    """ 
    Запрос, который возвращает топ 10 самых высоких средних показателей 'heart_rate' 
    за часовые промежутки в указанном периоде 'date_from' и 'date_to'
    """
    
    # Subquery to create hourly intervals using PostgreSQL's date_trunc function
    hourly_interval = func.strftime('%Y-%m-%d %H:00:00', heart_rates_table.c.timestamp).label('hour')
    
    # Query to get top 10 highest average heart rates over hourly intervals
    query = select(
        heart_rates_table.c.user_id,
        hourly_interval,
        func.avg(heart_rates_table.c.heart_rate).label('avg_hr')
    ).where(
        and_(
            heart_rates_table.c.user_id == user_id,
            heart_rates_table.c.timestamp > date_from,
            heart_rates_table.c.timestamp < date_to
        )
    ).group_by(
        heart_rates_table.c.user_id,
        hourly_interval
    ).order_by(
        desc(func.avg(heart_rates_table.c.heart_rate))
    ).limit(10)

    with engine.connect() as connection:
        result = connection.execute(query)
        return result.fetchall()


if __name__ == "__main__":
    
    create_tables()
    populate_tables()

    #Parameters
    date_from = datetime.now() - timedelta(days=31)
    date_to = datetime.now()
    min_avg_heart_rate = 70.0
    min_age = 40
    gender = 'M'
    user_id = 1

    # Execute query_users with the provided parameters
    # query_users_results = query_users(min_age, gender, min_avg_heart_rate, date_from, date_to)
    # print("\nQuery Results:")
    # for row in query_users_results:
    #     print(row)

    query_for_user_results = query_for_user(user_id, date_from, date_to)
    print("\nQuery Results:")
    for row in query_for_user_results:
        print(row)

    