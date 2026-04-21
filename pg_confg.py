import os

class Config:
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'sua-chave-super-secreta'
    DB_HOST = 'poisonously-literary-bonito.data-1.use1.tembo.io'
    DB_PORT = '5432'
    DB_NAME = 'postgres'
    DB_USER = 'postgres'
    DB_PASSWORD = '04IOt7PzJQcRshbq'
    SQLALCHEMY_DATABASE_URI = f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'