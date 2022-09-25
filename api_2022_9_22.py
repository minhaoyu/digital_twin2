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
    # 检测四个参数是否都有数据
    if not (start and end and folder and fields):
        return "缺少参数！"

    # 检测数据格式
    # 时间
    time_format = re.compile(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}")

    try:
        start = time_format.match(start).group()
    except:
        return "start 格式错误！"

    try:
        end = time_format.match(end).group()
    except:
        return "end 格式错误！"

    if not isinstance(fields,list):
        return "fields 格式错误！"

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

    # 检测四个参数是否都有数据
    if not (start and end and folder and fields):
        return "缺少参数！"

    # 检测数据格式
    # 时间
    time_format = re.compile(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}")

    try:
        start = time_format.match(start).group()
    except:
        return "start 格式错误！"

    try:
        end = time_format.match(end).group()
    except:
        return "end 格式错误！"

    if not isinstance(fields,list):
        return "fields 格式错误！"

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

    # 检测四个参数是否都有数据
    if not (start and end and folder and method and fields and interval):
        return "缺少参数！"

    # 检测数据格式
    # 时间
    time_format = re.compile(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}")

    try:
        start = time_format.match(start).group()
    except:
        return "start 格式错误！"

    try:
        end = time_format.match(end).group()
    except:
        return "end 格式错误！"

    if not isinstance(fields,list):
        return "fields 格式错误！"

    table_request_methods = ["avg", "min", "max"]
    if method.casefold() not in table_request_methods:
        return "method 不存在！"

    frequency = ["minute","hour","day","week", "month","quarter"]
    if interval.casefold() not in frequency:
        return "interval 不存在！"

    result = {

    }

    # 根据 interval 参数过滤数据
    # 参数： 时间格式化表达式，方法，字段，库
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


    # 转换成图表格式
    result1 = {"date_time":[]}
    # 获取单个字段总数据条数，因为筛选方法与分组方法相同所以每一个字段的条数和数据位置都是一样的
    for i in range(len(result[fields[0]])):
        # 将其中一个字段当前条数记录的时间加入到结果中
        result1["date_time"].append(result[fields[0]][i]["datetime"])
        # 将每个字段的数据逐个添加到结果中
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

