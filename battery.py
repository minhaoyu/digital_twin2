from utils import *
import Commands
import gvar
import serv_tools as serv_tools

sql_editer = gvar.sql_editer


def modify_max_min():
    if gvar.modify_max_min_flag["battery"]:
        for key in gvar.max_min["max"]["battery"]:
            try:
                if float(gvar.max_min["max"]["battery"][key]["value"]) < float(
                        gvar.temp_compare_mean_values["battery"][key]):
                    gvar.max_min["max"]["battery"][key] = {
                        "datetime": gvar.temp_compare_mean_values["battery"]["measurement_time"],
                        "value": gvar.temp_compare_mean_values["battery"][key],
                    }
                elif float(gvar.max_min["min"]["battery"][key]["value"]) > float(
                        gvar.temp_compare_mean_values["battery"][key]):
                    gvar.max_min["min"]["battery"][key] = {
                        "datetime": gvar.temp_compare_mean_values["battery"]["measurement_time"],
                        "value": gvar.temp_compare_mean_values["battery"][key],
                    }
            except:
                print_exc()


def _calc_max_min():
    for i, each in enumerate(gvar.max_for_battery):
        data = sql_editer.fetch_one(Commands.get_datas_order.format(*each))
        compare = data
        compare1 = gvar.temp_compare_mean_values["battery"]
        try:
            if float(compare1[each[1]]) > float(compare[each[1]]):
                gvar.max_min["max"]["battery"][each[1]] = {
                    "datetime": compare1["measurement_time"],
                    "value": compare1[each[1]]
                }
            else:
                gvar.max_min["max"]["battery"][each[1]] = {
                    "datetime": compare["measurement_time"],
                    "value": compare[each[1]]
                }
        except Exception as e:
            print_exc()

    for i, each in enumerate(gvar.min_for_battery):
        data = sql_editer.fetch_one(Commands.get_datas_order.format(*each))
        compare = data
        compare1 = gvar.temp_compare_mean_values["battery"]
        try:
            if float(compare1[each[1]]) < float(compare[each[1]]):
                gvar.max_min["min"]["battery"][each[1]] = {
                    "datetime": compare1[each[1]]["measurement_time"],
                    "value": compare1[each[1]],
                }
            else:
                gvar.max_min["min"]["battery"][each[1]] = {
                    "datetime": compare["measurement_time"],
                    "value": compare[each[1]],
                }
        except Exception as e:
            print_exc()

    gvar.modify_max_min_flag["battery"] = True
    print("battery data loaded！")


def calc_max_min():
    thread = Thread(target=_calc_max_min)
    thread.start()
    gvar.threads.append(thread)


def realtime_read_data():
    first = True
    for i, value in enumerate(gvar.datas['battery'], 1):
        _date, _time = value[0].split("+")[0].split("T")
        _date = _date.replace("-", "/")  # format date
        if gvar.first < 2 and first:
            gvar.modify_max_min_flag["battery"] = False

            _date_time = to_time("1970/1/2 " + _time, "%Y/%m/%d %H:%M:%S") # format the date
            cdate_time = to_time("1970/1/2 " + strftime("%H:%M:%S", localtime()), "%Y/%m/%d %H:%M:%S")

            if _date_time < cdate_time:  # if the time less than the current time, pass
                continue
            elif _date_time > cdate_time:  # if the time greater than the current time, break
                while _date_time > cdate_time:
                    sleep(0.8)
                    cdate_time = to_time("1970/1/2 " + strftime("%H:%M:%S", localtime()), "%Y/%m/%d %H:%M:%S")
                    # print("cdate_time", cdate_time)
                    # print("_date_time", _date_time)

                    # transfer timestamp
                    _begin_time = mktime(cdate_time)
                    _end_time = mktime(_date_time)

                    print(f"\rneed to wait {_end_time - _begin_time} seconds to load the data！", end="")

            serv_tools.calculate_mean_value_battery(_date)
            gvar.realtime_result["datas"]["battery"] = serv_tools.process_data(value)
            calc_max_min()
            gvar.date["battery"] = _date

            gvar.first += 1
            first = False
        # detect date change
        elif _date != gvar.date["battery"]:
            gvar.date["battery"] = _date
            serv_tools.calculate_mean_value_battery(_date)

        gvar.realtime_result["datas"]["battery"] = serv_tools.process_data(value)

        kwargs = {
            "measurement_time": gvar.realtime_result["datas"]["battery"][0],
            "FCAS_Event": gvar.realtime_result["datas"]["battery"][1] if gvar.realtime_result["datas"]["battery"][
                1] else 0,
            "full_charge_energy": gvar.realtime_result["datas"]["battery"][2] if
            gvar.realtime_result["datas"]["battery"][2] else 0,
            "nominal_energy": gvar.realtime_result["datas"]["battery"][3] if gvar.realtime_result["datas"]["battery"][
                3] else 0,
            "expected_energy": gvar.realtime_result["datas"]["battery"][4] if gvar.realtime_result["datas"]["battery"][
                4] else 0,
            "charge_p_max": gvar.realtime_result["datas"]["battery"][5] if gvar.realtime_result["datas"]["battery"][
                5] else 0,
            "discharge_p_max": gvar.realtime_result["datas"]["battery"][6] if gvar.realtime_result["datas"]["battery"][
                6] else 0,
            "available_blocks": gvar.realtime_result["datas"]["battery"][7] if gvar.realtime_result["datas"]["battery"][
                7] else 0,
            "_3_phase_voltage": gvar.realtime_result["datas"]["battery"][8] if gvar.realtime_result["datas"]["battery"][
                8] else 0,
            "_3_phase_current": gvar.realtime_result["datas"]["battery"][9] if gvar.realtime_result["datas"]["battery"][
                9] else 0,
            "_3_phase_power": gvar.realtime_result["datas"]["battery"][10] if gvar.realtime_result["datas"]["battery"][
                10] else 0,
            "_3_phase_reactive_power": gvar.realtime_result["datas"]["battery"][11] if
            gvar.realtime_result["datas"]["battery"][
                11] else 0,
            "_3_phase_apparent_power": gvar.realtime_result["datas"]["battery"][12] if
            gvar.realtime_result["datas"]["battery"][
                12] else 0,
            "power_factor": gvar.realtime_result["datas"]["battery"][13] if gvar.realtime_result["datas"]["battery"][
                13] else 0,
            "frequency": gvar.realtime_result["datas"]["battery"][14] if gvar.realtime_result["datas"]["battery"][
                14] else 0,
            "real_energy_imported": gvar.realtime_result["datas"]["battery"][15] if
            gvar.realtime_result["datas"]["battery"][
                15] else 0,
            "real_energy_exported": gvar.realtime_result["datas"]["battery"][16] if
            gvar.realtime_result["datas"]["battery"][
                16] else 0,
            "reactive_energy_imported": gvar.realtime_result["datas"]["battery"][17] if
            gvar.realtime_result["datas"]["battery"][
                17] else 0,
            "reactive_energy_exported": gvar.realtime_result["datas"]["battery"][18] if
            gvar.realtime_result["datas"]["battery"][
                18] else 0,
            "apparent_energy": gvar.realtime_result["datas"]["battery"][19] if gvar.realtime_result["datas"]["battery"][
                19] else 0,
            "energy_price": gvar.realtime_result["datas"]["battery"][20] if gvar.realtime_result["datas"]["battery"][
                20] else 0,
            "raise_6_sec_price": gvar.realtime_result["datas"]["battery"][21] if
            gvar.realtime_result["datas"]["battery"][
                21] else 0,
            "raise_60_sec_price": gvar.realtime_result["datas"]["battery"][22] if
            gvar.realtime_result["datas"]["battery"][
                22] else 0,
            "raise_5_min_price": gvar.realtime_result["datas"]["battery"][23] if
            gvar.realtime_result["datas"]["battery"][
                23] else 0,
            "date_time": f"{_date} {_time}",
        }
        gvar.temp_compare_mean_values["battery"] = kwargs
        sql_editer.execute(Commands.insert_battery_data, kwargs)
        modify_max_min()
        if i == len(gvar.datas['battery']):
            while i == len(gvar.datas['battery']):
                sleep(1)
        else:
            sleep(60)