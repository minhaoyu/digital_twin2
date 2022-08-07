import requests
from time import sleep
from pprint import pprint
from traceback import print_exc

import requests

jdata = {
    "method": "single time",
    "folder": "Bldng_049",
    "time": "02:14:00",
    #"time1": "02:14:00",
    #"time2": "02:16:00",
    #"date": "2021/03/10",
    #"date1": "2021/03/10",
    #"date2": "2021/03/11",
}

up = []

while True:
    try:

        # result = requests.get("http://127.0.0.1:808/filter", json=jdata).json()
        # pprint(result)
        datas = requests.get("http://localhost:808").json()
        if datas == up:
            continue

        print("data has been updated！")
        for each in datas:
            print(each)
        up = datas
    except:
        pass
    finally:
        sleep(1)