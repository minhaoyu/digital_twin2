import time
from pprint import pprint

import utils,re,gvar
from flask import *


sql_editer = gvar.sql_editer

_2022_9_22_bp = Blueprint("_2022_9_22",__name__)


class Commands:
    get_data_by_time_periods_avg = """
    select avg({}) as `avg` from {}
    where `date_time` between :start and :end
    """

    get_data_by_time_periods_min_max = """
    select max({}) as `max`,min({}) as `min` from {}
    where `date_time` between :start and :end
    """

    get_data_by_ = """
    select date_format(date_time,"{}") `datetime`,
    {}(`{}`) as `value`
    from {}
    where `date_time` between :start and :end
    group by `datetime`
    ;"""

    get_data_by_quarter = """
    select date_format(date_time,"%Y") as `year`,
    FLOOR((date_format(date_time, "%m")+2)/3) as `quarter`,
    {}(`{}`) as `value`
    from {}
    where `date_time` between :start and :end
    group by `year`,`quarter`
    ;"""

@_2022_9_22_bp.route("/mean",methods=["POST"])
def mean():
    start = request.json.get("start")
    end = request.json.get("end")
    folder = request.json.get("folder")
    fields = request.json.get("fields")
    pprint(request.json)
    # test parameter
    if not (start and end and folder and fields):
        return "missing parameter！"

    # test data formate
    # time
    time_format = re.compile(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}")

    try:
        start = time_format.match(start).group()
    except:
        return "start wrong formate！"

    try:
        end = time_format.match(end).group()
    except:
        return "end wrong formate！"

    if not isinstance(fields,list):
        return "fields wrong formate！"

    result = {
        "mean":{},
    }

    for field in fields:
        data = sql_editer.fetch_one(Commands.get_data_by_time_periods_avg.format(field,folder),{
            "start":start,
            "end":end,
        })

        if folder == "battery":
            result["mean"][field] = data["avg"]
        else:
            result["mean"][field] = float("%.2f" % (data["avg"] / 1000))

    return jsonify(utils.to_jsonable(result))

@_2022_9_22_bp.route("/min_max",methods=["POST"])
def min_max():
    start = request.json.get("start")
    end = request.json.get("end")
    folder = request.json.get("folder")
    fields = request.json.get("fields")

    # test parameter
    if not (start and end and folder and fields):
        return "missing parameter！"

    # test data formate
    # time
    time_format = re.compile(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}")

    try:
        start = time_format.match(start).group()
    except:
        return "start wrong formate！"

    try:
        end = time_format.match(end).group()
    except:
        return "end wrong formate！"

    if not isinstance(fields,list):
        return "fields wrong formate！"

    result = {
        "max":{},
        "min":{},
    }

    for field in fields:
        data = sql_editer.fetch_one(Commands.get_data_by_time_periods_min_max.format(field,field,folder),{
            "start":start,
            "end":end,
        })

        if folder == "battery":
            result["max"][field] = data["max"]
            result["min"][field] = data["min"]
        else:
            result["max"][field] = float("%.2f" % (data["max"] / 1000))
            result["min"][field] = float("%.2f" % (data["min"] / 1000))

    return jsonify(utils.to_jsonable(result))

@_2022_9_22_bp.route("/table",methods=["POST"])
def table():
    start = request.json.get("start")
    end = request.json.get("end")
    folder = request.json.get("folder")
    method = request.json.get("method")
    fields = request.json.get("fields")
    interval = request.json.get("interval")
    print(request.json)

    # test parameter
    if not (start and end and folder and method and fields and interval):
        return "missing parameter！"

    # test data formate
    # time
    time_format = re.compile(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}")

    try:
        start = time_format.match(start).group()
    except:
        return "start wrong formate！"

    try:
        end = time_format.match(end).group()
    except:
        return "end wrong formate！"

    if not isinstance(fields,list):
        return "fields wrong formate！"

    table_request_methods = ["avg", "min", "max"]
    if method.casefold() not in table_request_methods:
        return "method doesn't exist！"

    frequency = ["minute","hour","day","week", "month","quarter"]
    if interval.casefold() not in frequency:
        return "interval doesn't exist！"

    result = {

    }

    # filter the data
    # parameter： time，method，string，table
    if interval == "minute":
        for field in fields:
            result[field] = sql_editer.fetch_all(Commands.get_data_by_.format("%Y-%m-%d %H:%i",method,field,folder), {
                "start": start,
                "end": end,
            })

    elif interval == "hour":
        print(fields)
        for field in fields:
            result[field] = sql_editer.fetch_all(Commands.get_data_by_.format("%Y-%m-%d %H",method,field,folder), {
                "start": start,
                "end": end,
            })

        print(result)
    elif interval == "day":
        for field in fields:
            result[field] = sql_editer.fetch_all(Commands.get_data_by_.format("%Y-%m-%d",method,field,folder), {
                "start": start,
                "end": end,
            })

            print(result[field])

    elif interval == "week":
        for field in fields:
            result[field] = sql_editer.fetch_all(Commands.get_data_by_.format("%x %v week",method,field,folder), {
                "start": start,
                "end": end,
            })

    elif interval == "month":
        for field in fields:
            result[field] = sql_editer.fetch_all(Commands.get_data_by_.format("%Y-%m",method,field,folder), {
                "start": start,
                "end": end,
            })

    elif interval == "quarter":
        for field in fields:
            result[field] = sql_editer.fetch_all(Commands.get_data_by_quarter.format(method,field,folder), {
                "start": start,
                "end": end,
            })

            result[field] = [{
                "value":each.value,
                "datetime":f"{each.year} {int(each.quarter)} quarter"
            } for each in result[field]]


    # transfer to diagram format
    result1 = {"date_time":[]}
    # get single string total value
    for i in range(len(result[fields[0]])):
        # add the time to the result
        result1["date_time"].append(result[fields[0]][i]["datetime"])
        # store data to the result
        for field in fields:
            if result1.get(field):
                if folder == "battery":
                    result1[field].append(result[field][i]["value"])
                else:
                    result1[field].append(result[field][i]["value"]/1000)
            else:
                if folder == "battery":
                    result1[field] = [result[field][i]["value"]]
                else:
                    result1[field] = [result[field][i]["value"]/1000]

    return jsonify(utils.to_jsonable(result1))

