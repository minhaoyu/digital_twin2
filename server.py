import datetime
import os
import sys
import time
from traceback import print_exc
from pprint import pprint

from flask import Flask, jsonify, request
from threading import Thread
import csv
from flask_sqlalchemy import SQLAlchemy
from http_sqlalchemy import Flask_SQL, SQLALCHEMY_DATABASE_URI

# 初始化
app = Flask(__name__)
db = SQLAlchemy()

app.config["SQLALCHEMY_DATABASE_URI"] = SQLALCHEMY_DATABASE_URI
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = True

db.init_app(app)

sql_editer = Flask_SQL(db, app)

table_name = [
    "battery",
    "bldng_049",
    "bldng_078",
]

root = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
data_dirs = os.listdir(root)

print(data_dirs)

class Commands:
    get_datas = """
    select * from `digital_twin`.{}
    where id=:id
    """

    get_ids = """
    select `id` from `digital_twin`.{}
    """

    create_table = """
    create table `digital_twin`.`{}`(
        `id` int(11) NOT NULL PRIMARY KEY AUTO_INCREMENT,
        `date` date NOT NULL,
        `time` time NOT NULL,
        `pv_w` BIGINT,
        `pv_wh` BIGINT,
        `date_time` DATETIME,
         UNIQUE INDEX `datetime_UNIQUE` (`date` ASC, `time` ASC) VISIBLE
    );
    """

    insert_other_data = """
    insert into `digital_twin`.`{}`(`date`,`time`,`pv_w`,`pv_wh`,`date_time`)
    values(:date,:time,:pv_w,:pv_wh,:date_time);
    """

    insert_battery_data = """
    insert into `digital_twin`.`battery`(
    `measurement_time`,
    `FCAS_Event`,
    `full_charge_energy`,
    `nominal_energy`,
    `expected_energy`,
    `charge_p_max`,
    `discharge_p_max`,
    `available_blocks`,
    `_3_phase_voltage`,
    `_3_phase_current`,
    `_3_phase_power`,
    `_3_phase_reactive_power`,
    `_3_phase_apparent_power`,
    `power_factor_frequency`,
    `real_energy_imported`,
    `real_energy_exported`,
    `reactive_energy_imported`,
    `reactive_energy_exported`,
    `apparent_energy`,
    `energy_price`,
    `raise_6_sec_price`,
    `raise_60_sec_price`,
    `raise_5_min_price`,
    `date_time`)
    values(
    :measurement_time,
    :FCAS_Event,
    :full_charge_energy,
    :nominal_energy,
    :expected_energy,
    :charge_p_max,
    :discharge_p_max,
    :available_blocks,
    :_3_phase_voltage,
    :_3_phase_current,
    :_3_phase_power,
    :_3_phase_reactive_power,
    :_3_phase_apparent_power,
    :power_factor_frequency,
    :real_energy_imported,
    :real_energy_exported,
    :reactive_energy_imported,
    :reactive_energy_exported,
    :apparent_energy,
    :energy_price,
    :raise_6_sec_price,
    :raise_60_sec_price,
    :raise_5_min_price,
    :date_time);
    """


def check_middle(num, begin, end):
    try:
        if begin <= num <= end:
            return True
    except:
        pass

def to_time(timestr, _format):
    """%Y/%m/%d %H:%M:%S"""
    try:
        return time.strptime(timestr, _format)
    except:
        pass

def to_jsonable(data):
    temp = dict(data)
    data = {}
    for key, value in temp.items():
        if type(value) == datetime.timedelta or type(value) == datetime.date or type(value) == datetime.datetime:
            data[key] = value.__str__()
        else:
            data[key] = value

    return data

def load_data(index):
    first_flag = True # load data for the first time

    datas = []
    # create table by folder's name
    try:
        sql_editer.execute(Commands.create_table.format(data_dirs[index]))
    except:
        pass

    # load csv data
    second_folder = os.path.join(root,data_dirs[index])
    for each in os.listdir(second_folder):
        path = os.path.join(second_folder, each)
        # print("path",path)
        with open(path, "r") as file:
            data = csv.reader(file)
            next(data)
            datas.extend(list(data))

    while True:
        for each in datas:
            if first_flag: # execute when first time load
                if data_dirs[index] == "battery": # battery data
                    _date,_time = each[0].split("+")[0].split("T")
                else: # other data
                    _date,_time = each[0],each[1]

                _date = _date.replace("-","/")

                _date_time = to_time("1970/1/1 "+_time,"%Y/%m/%d %H:%M:%S")
                cdate_time = to_time("1970/1/1 "+time.strftime("%H:%M:%S",time.localtime()),"%Y/%m/%d %H:%M:%S")

                # print("cdate_time",cdate_time)
                # print("_date_time",_date_time)

                if _date_time < cdate_time: # if the time less than current time, then pass
                    continue
                elif _date_time > cdate_time: # if the time greater than current time then jump out

                    while _date_time > cdate_time:
                        time.sleep(0.5)
                        cdate_time = to_time("1970/1/1 "+time.strftime("%H:%M:%S",time.localtime()),"%Y/%m/%d %H:%M:%S")
                        # print("cdate_time",cdate_time)
                        # print("_date_time",_date_time)


                        _begin_time = time.mktime(cdate_time)
                        _end_time = time.mktime(_date_time)
                        # print("_begin_time",_begin_time)
                        # print("_end_time",_end_time)

                        print(f"\rneed to wait {_end_time-_begin_time} seconds to see the data！", end="")
                else:
                    first_flag = False
                print()

            result[index] = each # deal with data

            if data_dirs[index] == "battery":
                _date,_time = result[index][0].split("+")[0].split("T")
                date_time = "{} {}".format(_date,_time)
                kwargs = {
                    "measurement_time":result[index][0],
                    "FCAS_Event":result[index][1] if result[index][1] else 0,
                    "full_charge_energy":result[index][2] if result[index][2] else 0,
                    "nominal_energy":result[index][3] if result[index][3] else 0,
                    "expected_energy":result[index][4] if result[index][4] else 0,
                    "charge_p_max":result[index][5] if result[index][5] else 0,
                    "discharge_p_max":result[index][6] if result[index][6] else 0,
                    "available_blocks":result[index][7] if result[index][7] else 0,
                    "_3_phase_voltage":result[index][8] if result[index][8] else 0,
                    "_3_phase_current":result[index][9] if result[index][9] else 0,
                    "_3_phase_power":result[index][10] if result[index][10] else 0,
                    "_3_phase_reactive_power":result[index][11] if result[index][11] else 0,
                    "_3_phase_apparent_power":result[index][12] if result[index][12] else 0,
                    "power_factor_frequency":result[index][13] if result[index][13] else 0,
                    "real_energy_imported":result[index][14] if result[index][14] else 0,
                    "real_energy_exported":result[index][15] if result[index][15] else 0,
                    "reactive_energy_imported":result[index][16] if result[index][16] else 0,
                    "reactive_energy_exported":result[index][17] if result[index][17] else 0,
                    "apparent_energy":result[index][18] if result[index][18] else 0,
                    "energy_price":result[index][19] if result[index][19] else 0,
                    "raise_6_sec_price":result[index][20] if result[index][20] else 0,
                    "raise_60_sec_price":result[index][21] if result[index][21] else 0,
                    "raise_5_min_price":result[index][22] if result[index][22] else 0,
                    "date_time": date_time,
                }
                sql_editer.execute(Commands.insert_battery_data, kwargs)
            else:
                kwargs = {
                    "date":result[index][0],
                    "time":result[index][1],
                    "pv_w":result[index][2] if result[index][2] else 0,
                    "pv_wh":result[index][3] if result[index][3] else 0,
                    "date_time": f"{result[index][0]} {result[index][1]}"
                }
                sql = Commands.insert_other_data.format(data_dirs[index])
                sql_editer.execute(sql,kwargs)

            print(f"data update，from thread{index}")
            print(result[index])
            time.sleep(60)


def process_data(data):
    # processing data here

    return data


@app.route("/")
def index():
    return jsonify(result)


@app.route("/filter")
def _filter():
    result = []
    jdata = request.json
    pprint(jdata)

    if jdata["folder"].casefold() not in table_name:
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
                    if check_middle(to_time(_time, "%H:%M:%S"), to_time(jdata["time1"], "%H:%M:%S"),
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

    result = [to_jsonable(each) for each in result]
    return jsonify(result)

if __name__ == "__main__":
    print("server is loading data！")

    result = [0] * len(table_name)

    threads = []

    for i in range(len(table_name)):
        thread = Thread(target=load_data, args=(i,))
        thread.start()
        threads.append(thread)
    print("data loaded！")
    print("server started！")
    app.run(
        "0.0.0.0",
        808,
        debug=False,
    )
