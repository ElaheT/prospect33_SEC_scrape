from dotenv import load_dotenv
import sqlalchemy as sa
import os

class Connection(object):
    def __init__(self):
        '''
        Load the environment variables by calling the load_dotenv method.
        The database connection details are set as environment variables.
        '''
        load_dotenv()
    
    def connect_to_database(self, use_postgres=True, return_as_string):
        '''
        Connect to the mysql database and create the engine object
        using sqlalchemy.
        '''
        if use_postgres:
            conn_string = f'postgres://{os.getenv("user")}:{os.getenv("password")}@{os.getenv("host")}:{os.getenv("port")}/{os.getenv("database")}'
        else:
            conn_string = f'mysql://{os.getenv("user")}:{os.getenv("password")}@{os.getenv("host")}:{os.getenv("port")}/{os.getenv("database")}'
        if return_as_string:
            return conn_string
        engine = sa.create_engine(conn_string)
        return engine

