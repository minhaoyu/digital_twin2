from utils import *
import Commands
import gvar

sql_editer = gvar.sql_editer

# create tables
def create_datatables():
    new_bldngs = [
        each.casefold() for each in os.listdir(gvar.root)
    ]

    new_bldngs.remove("battery")

    if new_bldngs != gvar.bldngs:
        gvar.bldngs = new_bldngs
        for each in new_bldngs:
            try:
                sql_editer.execute(Commands.create_table.format(each))
            except:
                pass

def add_new_thread(name):
    gvar.realtime_result["datas"][name] = {}
    gvar.threads.append(Thread(target=gvar.realtime_read_data, args=(name,)))
    gvar.threads[-1].start()

def add_new_data(paths):
    start = []

    for path in paths:
        # print(path)
        # get folder name
        project_name = os.path.split(os.path.split(path)[0])[1].casefold()

        # if new folder was added, then add the new folder to realtime_datas
        if project_name not in gvar.datas.keys():
            gvar.datas[project_name] = []
            start.append(project_name)

        with open(path, "r") as file:
            data = csv.reader(file)
            next(data)
            gvar.datas[project_name].extend(list(data))

    for each in start:
        add_new_thread(each)

def _calculate_mean_value_battery(date):
    print("calculate_mean_value_battery")
    result = []
    battery_path = os.path.join(gvar.root, "battery")

    for file in os.listdir(battery_path):
        path = os.path.join(battery_path,file)
        # print(path)
        with open(path, "r") as file:
            csv_reader = csv.reader(file)
            headers = next(csv_reader)
            # print(headers)
            for each in csv_reader:
                _date, _time = each[0].split("+")[0].split("T")
                _date = _date.replace("-", "/")
                if _date == date:
                    result.append(each)

    gvar.realtime_result["mean_value"]["battery"] = {}
    for i in range(2, len(result[0])):
        gvar.realtime_result["mean_value"]["battery"][headers[i]] = 0
        for each in result:
            try:
                gvar.realtime_result["mean_value"]["battery"][headers[i]] += float(each[i])
            except:
                pass
        else:
            gvar.realtime_result["mean_value"]["battery"][headers[i]] /= len(result)

    # for key,value in gvar.realtime_result["mean_value"]["battery"].items():
    #     print(key,": ",value)

def _calculate_mean_value_bldng(date):
    print("calculate_mean_value_bldng")
    for bldng in gvar.bldngs:
        result = []
        bldng_path = os.path.join(gvar.root, bldng)

        for file in os.listdir(bldng_path):
            path = os.path.join(bldng_path, file)
            # print(path)
            with open(path, "r") as file:
                csv_reader = csv.reader(file)
                headers = next(csv_reader)
                # print(headers)
                for each in csv_reader:
                    _date = each[0].replace("-", "/")
                    if _date == date:
                        result.append(each)

        gvar.realtime_result["mean_value"][bldng] = {}
        for i in range(2,len(result[0])):
            gvar.realtime_result["mean_value"][bldng][headers[i]] = 0
            for each in result:
                try:
                    gvar.realtime_result["mean_value"][bldng][headers[i]] += float(each[i])
                except:
                    pass
            else:
                gvar.realtime_result["mean_value"][bldng][headers[i]] /= len(result)


        # for key, value in gvar.realtime_result["mean_value"][bldng].items():
        #     print(key, ": ", value)

def calculate_mean_value_battery(date):
    thread = Thread(target=_calculate_mean_value_battery, args=(date,))
    thread.start()
    gvar.threads.append(thread)

def calculate_mean_value_bldng(date):
    thread = Thread(target=_calculate_mean_value_bldng, args=(date,))
    thread.start()
    gvar.threads.append(thread)

def _check_dir_change():
    while True:
        # create new dir tree
        tree = create_tree(gvar.root)

        if tree != gvar.tree:
            print("There are chages happened in the folder，loading new data")
            # create table and return the newest table
            create_datatables()

            # get the changing part for the dir tree
            difference = sub_tree(tree, gvar.tree)

            gvar.tree = tree

            add_new_data(difference)

        sleep(0.5)

def check_dir_change():
    thread = Thread(target=_check_dir_change)
    thread.start()
    gvar.threads.append(thread)

def process_data(data):
    return data

if __name__ == '__main__':
    print(os.getcwd())
    gvar.root = "../data"
    create_datatables()
    check_dir_change()

    for each in gvar.threads:
        each.join()

