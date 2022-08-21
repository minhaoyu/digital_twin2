import sys, os
sys.path.insert(0, "cores")

import gvar as gvar
gvar.root = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
print("gvar.root",gvar.root)

from flask import Flask, jsonify, request
from flask_sqlalchemy import SQLAlchemy
from http_sqlalchemy import Flask_SQL, SQLALCHEMY_DATABASE_URI
from flask_cors import*

# Initialize
app = Flask(__name__)
CORS(app,supports_credentials=True)
db = SQLAlchemy()

app.config["SQLALCHEMY_DATABASE_URI"] = SQLALCHEMY_DATABASE_URI
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = True

db.init_app(app)

sql_editer = Flask_SQL(db, app)
gvar.sql_editer = sql_editer

import battery as battery
import bldng as bldng
from utils import *

import serv_tools as serv_tools
import Commands

def realtime_read_data(name):
    if name == "battery":
        battery.realtime_read_data()
    else:
        bldng.realtime_read_data(name)

gvar.realtime_read_data = realtime_read_data

serv_tools.create_datatables()
serv_tools.check_dir_change()

@app.route("/")
def index():
    return jsonify(to_jsonable(gvar.realtime_result))

@app.route("/filter")
def _filter():
    result = []
    jdata = request.json
    pprint(jdata)

    if jdata["folder"].casefold() not in gvar.bldngs and jdata["folder"].casefold() != "battery":
        return jsonify({"message": "no this folder！"})

    if jdata["method"] == "single time":
        try:
            result = sql_editer.fetch_all("""
            select * from `digital_twin`.{}
            where locate(:time,`date_time`)""".format(jdata["folder"]), {
                "time": jdata["time"]
            })
        except:
            pass

    elif jdata["method"] == "time period":
        ids = sql_editer.fetch_all("select `id`,`date_time` from `digital_twin`.`{}`".format(jdata["folder"]))
        if ids:
            for row in ids:
                try:
                    date, _time = row.date_time.__str__().split(" ")
                    if check_middle(to_time(_time, "%H:%M:%S"),
                                    to_time(jdata["time1"], "%H:%M:%S"),
                                    to_time(jdata["time2"], "%H:%M:%S")):
                        data = sql_editer.fetch_one(Commands.get_datas.format(jdata["folder"]), {"id": row.id})
                        result.append(data)
                except:
                    pass

    elif jdata["method"] == "single date":
        try:
            result = sql_editer.fetch_all("""
            select * from `digital_twin`.{}
            where locate(:date,`date_time`)""".format(jdata["folder"]), {
                "date": jdata["date"]
            })
        except:
            pass

    elif jdata["method"] == "date period":
        result = sql_editer.fetch_all("""
        select * from `digital_twin`.{}
        where `date_time` between :begin and :end
        """.format(jdata["folder"]),{
            "begin": jdata["date1"] + " 00:00:00",
            "end": jdata["date2"] + " 23:59:59",
        })

    elif jdata["method"] == "single date with single time":
        try:
            result = sql_editer.fetch_all("""
            select * from `digital_twin`.`{}`
            where `date_time`=:date_time
            """.format(jdata["folder"]),{
                "date_time":jdata["datetime"],
            })
        except:
            pass

    elif jdata["method"] == "period of date with single time":
        ids = sql_editer.fetch_all("select `id`,`date_time` from `digital_twin`.`{}`".format(jdata["folder"]))
        if ids:
            for row in ids:
                try:
                    date, _time = row.date_time.__str__().split(" ")
                    if check_middle(to_time(date, "%Y-%m-%d"),
                                    to_time(jdata["date1"], "%Y-%m-%d"),
                                    to_time(jdata["date2"], "%Y-%m-%d")) and _time == jdata["time"]:
                        data = sql_editer.fetch_one(Commands.get_datas.format(jdata["folder"]), {"id": row.id})
                        result.append(data)
                except:
                    pass

    elif jdata["method"] == "date time period":
        result = sql_editer.fetch_all("""
        select * from `digital_twin`.`{}`
        where `date_time` between :begin and :end
        """.format(jdata["folder"]),{
            "begin": jdata["datetime1"],
            "end": jdata["datetime2"],
        })

    result = to_jsonable([dict(each) for each in result])
    return jsonify(result)

@app.route("/max_min")
def max_min():
    try:
        return jsonify(to_jsonable(gvar.max_min))
    except Exception as e:
        print(e)
        pprint(gvar.max_min)
        return jsonify(gvar.max_min)

@app.route("/test")
def test():
    return jsonify(to_jsonable(gvar.temp_compare_mean_values))



if __name__ == "__main__":
    app.run(
        "0.0.0.0",
        808,
        debug=False,
    )

