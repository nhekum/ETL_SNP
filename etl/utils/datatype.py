import re
import json
import pytz
from types import SimpleNamespace
from dateutil.parser import parse 

def remove_symbol(my_str, pattern, with_replace=''):
    try:
        pattern = r"[%s]"%(pattern)
        if with_replace:
            my_str = re.sub(pattern, with_replace, my_str)
        else:
            my_str = re.sub(pattern, '', my_str)
    except:
        raise Exception("Can't remove string")
    return my_str

def str2date(my_str):
    try: 
        date_time = parse(my_str, fuzzy=False)
    except ValueError:
        raise Exception("Fail convert str to date")
    return date_time

def replace_symbol(my_str, pattern):
    for symbols in pattern:
        my_str.replace(symbols[0], symbols[1])
    return my_str

def date2str(dtime, format="%m/%d/%Y %H:%M:%S"):
    date_str = dtime.strftime(format)
    return date_str

def date2int(dtime, format_str="%Y%m%d", number_digit="7"):
    assert number_digit in ["7", "8"] , "number_digit shouble be 7 or 8 number digits"
    if number_digit=="8":
        date_time = dtime.strftime(format_str)
        date_str = re.sub(r"[\-\/\_]", "", date_time)
        date_int = int(date_str)
    else:
        day_of_year = dtime.timetuple().tm_yday 
        year = dtime.strftime("%Y")
        date_str = year+str(day_of_year)
        date_int = int(date_str)
    return date_int
    
def datetime2timestamp(dtime, time_zone='Asia/Ho_Chi_Minh', milis=True):
    if time_zone:
        timezone = pytz.timezone(time_zone)
        dtzone = timezone.localize(dtime)
        dtimestamp = dtzone.timestamp()
    else:
        dtimestamp = dtime.timestamp()

    if milis:
        time_stamp = int(round(dtimestamp * 1000))
    else:
        time_stamp = int(round(dtimestamp))
    return time_stamp

def int2date(number):
    try:
        my_str = str(number)
        dtime = str2date(my_str)
    except:
        raise Exception("Wrong input format")
    return dtime

def json2object(json_config):
    if isinstance(json_config, str):
        with open(json_config) as json_file:
            data = str(str(json.load(json_file))).replace('\'', '\"')
    else:
        data = str(str(json_config)).replace('\'', '\"')
    json_object = json.loads(data, object_hook=lambda d: SimpleNamespace(**d))
    return json_object

def json2dict(json_config:str):
    with open(json_config) as f:
        conf_dict = json.load(f)
    return conf_dict

def string2query(url:str, all_col:list, schema:str, table:str, where:str=""):
    if 'postgres' in url:
        all_col = remove_symbol(str(all_col), "\[\]", with_replace='').replace('\'','\"')
    else:
        all_col = remove_symbol(str(all_col), "\[\]", with_replace='').replace('\'','')
    query = """ SELECT {} FROM "{}"."{}" """.format(all_col, schema, table) + where
    return query

