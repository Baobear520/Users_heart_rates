from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, DateTime, ForeignKey

engine = create_engine('postgresql://username:password@host:port/database_name')
metadata = MetaData()

users = Table(
    'users',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('name', String),
    Column('gender', String),
    Column('age', String)
)

heart_rates = Table(
    'heart_rates',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('user_id', Integer, ForeignKey('users.id'), index=True),
    Column('timestamp', DateTime),
    Column('heart_rate', Float),
)

metadata.create_all(engine)

def query_users(min_age, gender, min_avg_heart_rate, date_from, date_to):
    # Напишите здесь запрос, который возвращает всех пользователей, которые старше'min_age' и 
    # имеют средний пульс выше, чем 'min_avg_heart_rate', на определенном промежутке времени
        # min_age: минимальный возраст пользователей
        # gender: пол пользователей
        # min_avg_heart_rate: минимальный средний пульс
        # date_from: начало временного промежутка
        # date_to: конец временного промежутка
    return
    
def query_for_user(user_id, date_from, date_to):
    # Напишите здесь запрос, который возвращает топ 10 самых высоких средних показателей 'heart_rate' 
    # за часовые промежутки в указанном периоде 'date_from' и 'date_to'
        # user_id: ID пользователя
        # date_from: начало временного промежутка
        # date_to: конец временного промежутка
    return
