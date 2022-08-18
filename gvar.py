sql_editer = None

root = None

bldngs = None

date = {
    "battery":"",
}

realtime_read_data = None

tree = set()

datas = {}

first = 0

# get the result of real time data
realtime_result = {
    "datas": {
        "battery": None,
    },
    "mean_value": {}
}

# mean value
mean_value = {}

threads = []

modify_max_min_flag = {

}


temp_compare_mean_values = {}

max_for_battery = [
    ("battery","full_charge_energy","desc"),
    ("battery","nominal_energy","desc"),
    ("battery","expected_energy","desc"),
    ("battery","charge_p_max","desc"),
    ("battery","discharge_p_max","desc"),
    ("battery","available_blocks","desc"),
    ("battery","_3_phase_voltage","desc"),
    ("battery","_3_phase_current","desc"),
    ("battery","_3_phase_power","desc"),
    ("battery","_3_phase_reactive_power","desc"),
    ("battery","_3_phase_apparent_power","desc"),
    ("battery","power_factor","desc"),
    ("battery","frequency","desc"),
    ("battery","real_energy_imported","desc"),
    ("battery","real_energy_exported","desc"),
    ("battery","reactive_energy_imported","desc"),
    ("battery","reactive_energy_exported","desc"),
    ("battery","apparent_energy","desc"),
    ("battery","energy_price","desc"),
    ("battery","raise_6_sec_price","desc"),
    ("battery","raise_60_sec_price","desc"),
    ("battery","raise_5_min_price","desc"),
]

min_for_battery = [
    ("battery","full_charge_energy","asc"),
    ("battery","nominal_energy","asc"),
    ("battery","expected_energy","asc"),
    ("battery","charge_p_max","asc"),
    ("battery","discharge_p_max","asc"),
    ("battery","available_blocks","asc"),
    ("battery","_3_phase_voltage","asc"),
    ("battery","_3_phase_current","asc"),
    ("battery","_3_phase_power","asc"),
    ("battery","_3_phase_reactive_power","asc"),
    ("battery","_3_phase_apparent_power","asc"),
    ("battery","power_factor","asc"),
    ("battery","frequency","asc"),
    ("battery","real_energy_imported","asc"),
    ("battery","real_energy_exported","asc"),
    ("battery","reactive_energy_imported","asc"),
    ("battery","reactive_energy_exported","asc"),
    ("battery","apparent_energy","asc"),
    ("battery","energy_price","asc"),
    ("battery","raise_6_sec_price","asc"),
    ("battery","raise_60_sec_price","asc"),
    ("battery","raise_5_min_price","asc"),
]

max_for_bldng = [
    ("pv_wh","desc"),
    ("pv_w","desc"),
]

min_for_bldng = [
    ("pv_wh","asc"),
    ("pv_w","asc"),
]

max_min = {
    "max":{
        "battery":{},
    },
    "min":{
        "battery":{},
    },
}