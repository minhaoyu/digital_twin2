from utils import *
import Commands
import gvar
import serv_tools as serv_tools

sql_editer = gvar.sql_editer


def modify_max_min(bldng):
    if gvar.modify_max_min_flag[bldng]:
        for key in gvar.max_min["max"][bldng]:
            try:
                if float(gvar.max_min["max"][bldng][key]["value"]) < float(gvar.temp_compare_mean_values[bldng][key]):
                    gvar.max_min["max"][bldng][key] = {
                        "date": gvar.temp_compare_mean_values[bldng]["date"],
                        "time": gvar.temp_compare_mean_values[bldng]["time"],
                        "value": gvar.temp_compare_mean_values[bldng][key],
                    }
                elif float(gvar.max_min["min"][bldng][key]["value"]) > float(gvar.temp_compare_mean_values[bldng][key]):
                    gvar.max_min["min"][bldng][key] = {
                        "date": gvar.temp_compare_mean_values[bldng]["date"],
                        "time": gvar.temp_compare_mean_values[bldng]["time"],
                        "value": gvar.temp_compare_mean_values[bldng][key],
                    }
            except:
                print_exc()

def _calc_max_min(bldng):
    gvar.max_min["max"][bldng] = {}
    gvar.max_min["min"][bldng] = {}

    for i,each in enumerate(gvar.max_for_bldng):
        data = sql_editer.fetch_one(Commands.get_datas_order.format(bldng,*each))
        compare = data
        compare1 = gvar.temp_compare_mean_values[bldng]

        if float(compare1[each[0]]) > float(compare[each[0]]):
            gvar.max_min["max"][bldng][each[0]] = {
                    "date": compare1["date"],
                    "time": compare1["time"],
                    "value": compare1[each[0]],
                }
        else:
            gvar.max_min["max"][bldng][each[0]] = {
                    "date": compare["date"],
                    "time": compare["time"],
                    "value": compare[each[0]],
                }

    for i,each in enumerate(gvar.min_for_bldng):
        data = sql_editer.fetch_one(Commands.get_datas_order.format(bldng,*each))
        compare = data
        compare1 = gvar.temp_compare_mean_values[bldng]

        if float(compare1[each[0]]) < float(compare[each[0]]):
            gvar.max_min["min"][bldng][each[0]] = {
                    "date": compare1["date"],
                    "time": compare1["time"],
                    "value": compare1[each[0]],
                }
        else:
            gvar.max_min["min"][bldng][each[0]] = {
                    "date": compare["date"],
                    "time": compare["time"],
                    "value": compare[each[0]],
                }
    gvar.modify_max_min_flag[bldng] = True
    print(f"{bldng} data loaded！")

def calc_max_min(bldng):
    thread = Thread(target=_calc_max_min, args=(bldng,))
    thread.start()
    gvar.threads.append(thread)

def realtime_read_data(name):
    first = True
    for i,value in enumerate(gvar.datas[name],1):
        # get the time of this data
        _date, _time = value[0], value[1]
        _date = _date.replace("-", "/")  # 日期格式化
        if gvar.first < 2 and first:
            gvar.modify_max_min_flag[name] = False
            _date_time = to_time("1970/1/2 " + _time, "%Y/%m/%d %H:%M:%S")  # format the date
            cdate_time = to_time("1970/1/2 " + strftime("%H:%M:%S", localtime()), "%Y/%m/%d %H:%M:%S")

            if _date_time < cdate_time:  # if the time less than the current time, pass
                continue
            elif _date_time > cdate_time:  # if the time greater than the current time, break
                while _date_time > cdate_time:
                    sleep(1)
                    cdate_time = to_time("1970/1/2 " + strftime("%H:%M:%S", localtime()), "%Y/%m/%d %H:%M:%S")

                    # transfer timestamp
                    _begin_time = mktime(cdate_time)
                    _end_time = mktime(_date_time)

                    print(f"\rneed to wait {_end_time - _begin_time} seconds to load the data！", end="")

            serv_tools.calculate_mean_value_bldng(_date)
            gvar.realtime_result["datas"][name] = serv_tools.process_data(value)
            calc_max_min(name)
            gvar.date[name] = _date

            gvar.first += 1
            first = False
        # detect date change
        elif _date != gvar.date[name]:
            gvar.date[name] = _date
            serv_tools.calculate_mean_value_bldng(_date)

        gvar.realtime_result["datas"][name] = serv_tools.process_data(value)

        kwargs = {
            "date": gvar.realtime_result["datas"][name][0],
            "time": gvar.realtime_result["datas"][name][1],
            "pv_w": gvar.realtime_result["datas"][name][2] if gvar.realtime_result["datas"][name][2] else 0,
            "pv_wh": gvar.realtime_result["datas"][name][3] if gvar.realtime_result["datas"][name][3] else 0,
            "date_time": f'{gvar.realtime_result["datas"][name][0]} {gvar.realtime_result["datas"][name][1]}'
        }

        gvar.temp_compare_mean_values[name] = kwargs
        sql = Commands.insert_other_data.format(name)
        sql_editer.execute(sql, kwargs)

        print(gvar.realtime_result["datas"][name])

        modify_max_min(name)

        if i == len(gvar.datas[name]):
            while i == len(gvar.datas[name]):
                sleep(1)
        else:
            sleep(60)
