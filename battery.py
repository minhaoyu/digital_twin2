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

        temp_values = value
        kwargs = {
            "measurement_time":temp_values[0],
            "FCAS_Event": temp_values[1] if temp_values[
                1] else 0,
            "full_charge_energy": temp_values[2] if
            temp_values[2] else 0,
            "nominal_energy": temp_values[3] if temp_values[
                3] else 0,
            "expected_energy": temp_values[4] if temp_values[
                4] else 0,
            "charge_p_max": temp_values[5] if temp_values[
                5] else 0,
            "discharge_p_max": temp_values[6] if temp_values[
                6] else 0,
            "available_blocks": temp_values[7] if temp_values[
                7] else 0,
            "_3_phase_voltage": temp_values[8] if temp_values[
                8] else 0,
            "_3_phase_current": temp_values[9] if temp_values[
                9] else 0,
            "_3_phase_power": temp_values[10] if temp_values[
                10] else 0,
            "_3_phase_reactive_power": temp_values[11] if
            temp_values[
                11] else 0,
            "_3_phase_apparent_power": temp_values[12] if
            temp_values[
                12] else 0,
            "power_factor": temp_values[13] if temp_values[
                13] else 0,
            "frequency": temp_values[14] if temp_values[
                14] else 0,
            "real_energy_imported": temp_values[15] if
            temp_values[
                15] else 0,
            "real_energy_exported": temp_values[16] if
            temp_values[
                16] else 0,
            "reactive_energy_imported": temp_values[17] if
            temp_values[
                17] else 0,
            "reactive_energy_exported": temp_values[18] if
            temp_values[
                18] else 0,
            "apparent_energy": temp_values[19] if temp_values[
                19] else 0,
            "energy_price": temp_values[20] if temp_values[
                20] else 0,
            "raise_6_sec_price": temp_values[21] if
            temp_values[
                21] else 0,
            "raise_60_sec_price": temp_values[22] if
            temp_values[
                22] else 0,
            "raise_5_min_price": temp_values[23] if
            temp_values[
                23] else 0,
            "date_time": f"{_date} {_time}",
        }
        sql_editer.execute(Commands.insert_battery_data, kwargs)


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
            # gvar.realtime_result["datas"]["battery"] = serv_tools.process_data(value)
            calc_max_min()
            gvar.date["battery"] = _date

            gvar.first += 1
            first = False
        # detect date change
        elif _date != gvar.date["battery"]:
            gvar.date["battery"] = _date
            serv_tools.calculate_mean_value_battery(_date)

        gvar.realtime_result["datas"]["battery"] = serv_tools.process_data(value)

        gvar.temp_compare_mean_values["battery"] = kwargs

        modify_max_min()
        if i == len(gvar.datas['battery']):
            while i == len(gvar.datas['battery']):
                sleep(1)
        else:
            sleep(60)