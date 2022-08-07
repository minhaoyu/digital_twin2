from sqlalchemy import *
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import ForeignKey
from sqlalchemy import Column, String, Integer, Text, CHAR, BigInteger
from sqlalchemy.orm import sessionmaker, relationship, scoped_session
from sqlalchemy.exc import IntegrityError
from contextlib import contextmanager
from traceback import print_exc
import os

def create_session(engine):
    """create session   only use once when the program start"""

    # create sessionFactory() and create session object, but no thread isolation for this object
    SessionFactory = sessionmaker(bind=engine)

    # create thread isolated session object
    session = scoped_session(SessionFactory)


    return session

class DatabaseError(Exception):
    pass


class AdminError(Exception):
    pass

Errors = (
    "(pymysql.err.OperationalError)(1060, 'Too many connections')"
)

class SQL_Edit:
    def __init__(self,index=None):
        self.engine = create_engine(
            f'mysql+pymysql://root:951017@localhost:3306/digital_twin?charset=utf8',
            max_overflow=20)

        self.session = create_session(self.engine)

    # give the name of table
    def clear_table(self, table):
        self.execute('truncate table {table}')
        return True

    # use sql query
    def fetch_all(self, cmd, args=None):
        result = self.execute(cmd, args).fetchall()
        return result

    def fetch_one(self, cmd, args=None):
        result = self.execute(cmd, args).first()
        return result

    def execute(self, cmd, args=None):
        try:
            if args:
                result = self.session.execute(text(cmd), args)
            else:
                result = self.session.execute(text(cmd))
            self.session.commit()

        except Exception as e:
            self.session.rollback()
            print(e)
        finally:
            self.session.close()
            self.session.remove()  # self.session complete, destory session, release memory
            self.engine.dispose()
            if locals().get("result"):
                return result



if __name__ == '__main__':
    app = SQL_Edit()
