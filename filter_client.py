from traceback import print_exc

import requests
from time import sleep
from easygui import choicebox
import numpy as np

chooses = [
    "single time",
    "time period",
    "single date",
    "date period",
    "single date with single time",
    "period of date with single time",
    "date time period",
]

while True:
    try:
        jdata = {
            "method": choicebox("Please select the method", "select", chooses),
            "folder": input("Enter the name of folder:"),
        }

        if jdata["method"] == "single time":
            jdata["time"] = input("please enter the time[HH:MM:SS]:")

        elif jdata["method"] == "time period":
            jdata["time1"] = input("please enter the start time[HH:MM:SS]:")
            jdata["time2"] = input("please enter the end time[HH:MM:SS]:")

        elif jdata["method"] == "single date":
            jdata["date"] = input("please enter the date[YY-mm-dd]:")

        elif jdata["method"] == "date period":
            jdata["date1"] = input("please enter the start date[YY-mm-dd]:")
            jdata["date2"] = input("please enter the end date[YY-mm-dd]:")

        elif jdata["method"] == "single date with single time":
            jdata["datetime"] = input("please enter the date and time[YY-mm-dd HH:MM:SS]:")

        elif jdata["method"] == "single date with period of time":
            jdata["date"] = input("please enter the date[YY-mm-dd]:")
            jdata["time1"] = input("please enter the start time[HH:MM:SS]:")
            jdata["time2"] = input("please enter the end time[HH:MM:SS]:")

        elif jdata["method"] == "period of date with single time":
            jdata["date1"] = input("please enter the start date[YY-mm-dd]:")
            jdata["date2"] = input("please enter the end date[YY-mm-dd]:")
            jdata["time"] = input("please enter the time[HH:MM:SS]:")

        elif jdata["method"] == "date time period":
            jdata["datetime1"] = input("please enter the start date and time[YY-mm-dd HH:MM:SS]:")
            jdata["datetime2"] = input("please enter the end date and time[YY-mm-dd HH:MM:SS]:")

        result = requests.get("http://127.0.0.1:808/filter", json=jdata).json()
        if result:
            with open('result.txt', 'w+') as f:
                for each in result:
                    f.write(str(each) + '\n')
                    print(each)
            f.close()
        else:
            print("no available data！")
    except:
        print_exc()
