import os, sys, csv
from pprint import pprint
from traceback import print_exc
from time import strptime,sleep,strftime,localtime,mktime
from copy import deepcopy
from threading import Thread
import datetime

def check_middle(num, begin, end):
    try:
        if begin <= num <= end:
            return True
    except:
        pass

def to_time(timestr, _format):
    """%Y/%m/%d %H:%M:%S"""
    try:
        return strptime(timestr, _format)
    except:
        pass

def to_jsonable(data):
    if type(data) == list:
        return [to_jsonable(each) for each in data]
    elif type(data) == dict:
        return {to_jsonable(k):to_jsonable(v) for k,v in data.items()}
    else:
        if type(data) == datetime.timedelta or type(data) == datetime.date or type(data) == datetime.datetime:
            return data.__str__()
        else:
            return data

def create_tree(root):
    result = []
    for root,folders,files in os.walk(root):
        for file in files:
            result.append(os.path.join(root,file))
    return result

def sub_tree(tree1,tree2):
    for each in tree2:
        try:
            tree1.index(each)
            tree1.remove(each)
        except:
            pass

    return tree1


def printList(data):
    for each in data:
        print(each)