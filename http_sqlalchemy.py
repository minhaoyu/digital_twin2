from traceback import print_exc

import pymysql
import sqlalchemy

username = "root"
password = "951017"
database = "digital_twin"

SQLALCHEMY_DATABASE_URI = f"mysql+pymysql://{username}:{password}@127.0.0.1:3306/{database}?charset=utf8"


def create_tables(db, app):
    class Battery(db.Model):
        __tablename__ = "battery"
        id = db.Column(db.Integer, primary_key=True, autoincrement=True, unique=True)

        measurement_time = db.Column(db.CHAR(100), unique=True)

        FCAS_Event = db.Column(db.CHAR(100))
        full_charge_energy = db.Column(db.FLOAT)
        nominal_energy = db.Column(db.FLOAT)
        expected_energy = db.Column(db.FLOAT)
        charge_p_max = db.Column(db.FLOAT)
        discharge_p_max = db.Column(db.FLOAT)
        available_blocks = db.Column(db.FLOAT)
        _3_phase_voltage = db.Column(db.FLOAT)
        _3_phase_current = db.Column(db.FLOAT)
        _3_phase_power = db.Column(db.FLOAT)
        _3_phase_reactive_power = db.Column(db.FLOAT)
        _3_phase_apparent_power = db.Column(db.FLOAT)
        power_factor = db.Column(db.FLOAT)
        frequency = db.Column(db.FLOAT)
        real_energy_imported = db.Column(db.FLOAT)
        real_energy_exported = db.Column(db.FLOAT)
        reactive_energy_imported = db.Column(db.FLOAT)
        reactive_energy_exported = db.Column(db.FLOAT)
        apparent_energy = db.Column(db.FLOAT)
        energy_price = db.Column(db.FLOAT)
        raise_6_sec_price = db.Column(db.FLOAT)
        raise_60_sec_price = db.Column(db.FLOAT)
        raise_5_min_price = db.Column(db.FLOAT)
        date_time = db.Column(db.DATETIME, unique=True)

    class Bldng(db.Model):
        __tablename__ = "bldng"

        id = db.Column(db.Integer, primary_key=True, autoincrement=True, unique=True)
        date = db.Column(db.DATE, index=True)
        time = db.Column(db.TIME, index=True)
        date_time = db.Column(db.DATETIME, unique=True)
        pv_w = db.Column(db.BigInteger)
        pv_wh = db.Column(db.BigInteger)

        __table_args__ = (
            db.UniqueConstraint(date, time, name="unique_date_time"),
        )

    with app.app_context():
        db.create_all()


class Flask_SQL:
    def __init__(self, db, app):
        self.db = db
        self.app = app
        create_tables(db, app)

    # give the table's name
    def clear_table(self, table):
        with self.app.app_context():
            self.execute(f"truncate table {table}")
            return True

    # using sql query
    def fetch_all(self, sql, args=None, database=None):
        with self.app.app_context():
            if database:
                return self.db.session.execute(sql, args, bind=self.db.get_engine(self.app, bind=database)).all()
            else:
                return self.db.session.execute(sql, args).all()

    def fetch_one(self, sql, args=None, database=None):
        with self.app.app_context():
            if database:
                return self.db.session.execute(sql, args, bind=self.db.get_engine(self.app, bind=database)).first()
            else:
                return self.db.session.execute(sql, args).first()

    def execute(self, sql, args=None, database=None):

        with self.app.app_context():
            try:
                sql = self.db.text(sql)
                if args and database:
                    self.db.session.execute(sql, args, bind=self.db.get_engine(self.app, bind=database))
                elif args and not database:
                    self.db.session.execute(sql, args)
                elif not args and database:
                    self.db.session.execute(sql, bind=self.db.get_engine(self.app, bind=database))
                elif not args and not database:
                    self.db.session.execute(sql)
                self.db.session.commit()
                flag = True
            except (sqlalchemy.exc.IntegrityError, pymysql.err.IntegrityError, pymysql.err.OperationalError,
                    sqlalchemy.exc.OperationalError) as e:
                flag = False
            except Exception:
                print_exc()
                self.db.session.rollback()
                flag = False
            finally:
                self.db.session.close()
                self.db.engine.dispose()

        return flag

