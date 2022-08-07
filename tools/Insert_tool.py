import os.path
import csv

from sql_edit import *

sql_editer = SQL_Edit()

class Commands:
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


root = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),"data")
print(root)

folders = os.listdir(root)

def import_battery(path):
    print(path)
    root = path
    tables = os.listdir(root)
    for each in tables:
        path = os.path.join(root,each)
        print(path)

        with open(path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            next(reader)
            for row in reader:
                _date,_time = row[0].split("+")[0].split("T")
                date_time = "{} {}".format(_date,_time)
                kwargs = {
                    "measurement_time":row[0],
                    "FCAS_Event":row[1] if row[1] else 0,
                    "full_charge_energy":row[2] if row[2] else 0,
                    "nominal_energy":row[3] if row[3] else 0,
                    "expected_energy":row[4] if row[4] else 0,
                    "charge_p_max":row[5] if row[5] else 0,
                    "discharge_p_max":row[6] if row[6] else 0,
                    "available_blocks":row[7] if row[7] else 0,
                    "_3_phase_voltage":row[8] if row[8] else 0,
                    "_3_phase_current":row[9] if row[9] else 0,
                    "_3_phase_power":row[10] if row[10] else 0,
                    "_3_phase_reactive_power":row[11] if row[11] else 0,
                    "_3_phase_apparent_power":row[12] if row[12] else 0,
                    "power_factor_frequency":row[13] if row[13] else 0,
                    "real_energy_imported":row[14] if row[14] else 0,
                    "real_energy_exported":row[15] if row[15] else 0,
                    "reactive_energy_imported":row[16] if row[16] else 0,
                    "reactive_energy_exported":row[17] if row[17] else 0,
                    "apparent_energy":row[18] if row[18] else 0,
                    "energy_price":row[19] if row[19] else 0,
                    "raise_6_sec_price":row[20] if row[20] else 0,
                    "raise_60_sec_price":row[21] if row[21] else 0,
                    "raise_5_min_price":row[22] if row[22] else 0,
                    "date_time": date_time,
                }
                sql = Commands.insert_battery_data
                sql_editer.execute(sql,kwargs)


def import_other(path,name):
    print(path,name)

    sql_editer.execute(Commands.create_table.format(name))

    root = path
    tables = os.listdir(root)
    for each in tables:
        path = os.path.join(root,each)
        print(path)

        with open(path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            next(reader)
            for row in reader:
                kwargs = {
                    "date":row[0],
                    "time":row[1],
                    "pv_w":row[2] if row[2] else 0,
                    "pv_wh":row[3] if row[3] else 0,
                    "date_time": f"{row[0]} {row[1]}"
                }
                sql = Commands.insert_other_data.format(name)
                sql_editer.execute(sql,kwargs)

for each in folders:
    path = os.path.join(root, each)
    if each == "battery":
        pass
    else:
        import_other(path,each)
