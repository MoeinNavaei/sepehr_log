
import pandas as pd
from datetime import datetime
from datetime import timedelta
import datetime
import requests
import json
from urllib.parse import urlencode
from urllib.request import Request, urlopen
import ast
import time

start = time.time()
xx = 0
date_name = '08/03/2021'
 ######################## READ OF EVENT AND SESSION FILES ########################
read_file_login = pd.read_csv (r'C:\Users\PC\Desktop\sepehr_log\kafka\test (1).csv',encoding='utf-8')
#session_data.to_excel('read_file_login.xlsx', index=False)
 ######################## MERGE OF EVENT AND LOGIN ########################
session_data = read_file_login.copy()
del session_data['@version']
del session_data['sys_id']
del session_data['@timestamp']
del session_data['service_id']
del session_data['content_name']
#del session_data['channel_name']
del session_data['content_id']
del session_data['content_type_id']

session_data['date'] = session_data['time_stamp'].str[:10]
session_data['time'] = session_data['time_stamp'].str[11:]
del session_data['time_stamp']
######################## CHOOSE OF DAY AND HOUR ########################
#event_login = event_login.query("date == @date_name")
#event_login = event_login.query("time >= @date_name_start")
#event_login = event_login.query("time <= @date_name_end")
#login_day_hour = login_day.query("time >= @date_name_start")
#login_day_hour = login_day_hour.query("time <= @date_name_end")
######################## EPG OF KOUHZADI ########################
url = 'https://epgservices.irib.ir:84/Service_EPG.svc/GetEpgNetwork'
epg1=pd.DataFrame(columns=['ID_Day_Item', 'Name_Item', 'Time_Play', 'EP', 'DTDay', 'Length',
       'Dec_Full', 'Dec_Summary', 'ID_Kind', 'channel'])
chan_table=pd.read_excel(r'C:\Users\PC\Desktop\EpgApi\chan_table.xlsx', index_col=False)
chan_table['code']=chan_table['code'].astype(str)

for i in range(31,150):
    print(i)
    ax="\"SID_Network\":{}".format(i)
    az=",\"DTStart\":\"@date_name\",\"DTEnd\":\"@date_name\""  # DETERMINATION OF DATE 04/25/2021
    print("az: ", az)
    ay2='{'+str(ax)+str(az)+'}'
    ay="{\"SID_Network\":31}"
    myobj = {"JsonData":ay2,"Key":"EPG99f06e12YHNbgtrfvCDEolmnbvc"}
#    print(ay2)
    # s = requests.session()
    headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
    x = requests.post(url, json = myobj)
    a=x.content
    a=a.decode('utf-8')
#    print(a)
    # ast.literal_eval(a)
    json_acceptable_string = a.replace("'", "\"")
    d = json.loads(json_acceptable_string)
    b=d.get('JsonData')
    x = ast.literal_eval(b)
    # print(type(x[1]))
    epg = pd.DataFrame(x)
    epg['channel2'] = i
    for w in range(0,len(chan_table)):
        epg_code=chan_table.loc[w,'epg_code']
        code = chan_table.loc[w,'code']
        if i==epg_code:
            epg['channel']=str(code)
#            print(code)
            break
    epg1=epg1.append(epg)

epg1 = pd.read_excel (r'C:\Users\PC\Desktop\sepehr_log\kafka\epg_08_07_2021 (1).xlsx') # read EPG
date2 = pd.to_datetime(epg1.Time_Play, errors='coerce')
epg1 = epg1.assign(s_date=date2.dt.date, s_time=date2.dt.time)
date3 = pd.to_datetime(epg1.EP, errors='coerce')
epg1=epg1.assign(e_date=date3.dt.date, e_time=date3.dt.time)
epg2=epg1[(epg1['s_date'] == datetime.date(2021,6,20))]

epg1 = epg1.rename(columns={"s_date":"s_date_EPG"})
epg1 = epg1.rename(columns={"e_date":"e_date_EPG"})
epg1 = epg1.rename(columns={"s_time":"s_time_EPG"})
epg1 = epg1.rename(columns={"e_time":"e_time_EPG"})
######################## CONVERT CODE TO CHANNEL IN EPG OF KOUHZADI ########################
start = time.time()            # deactive
epg1.insert(16, 'channel_name', '')
epg1 = epg1.reset_index()
del epg1['index']

session_day_hour = pd.to_datetime(session_data.time, errors='coerce')
session_data = session_data.assign(time=session_day_hour.dt.time)
session_day_date = pd.to_datetime(session_data.date, errors='coerce')
session_data = session_data.assign(date=session_day_date.dt.date)

for i in range(0, len(epg1)):
#    print(i)
    if epg1.loc[i, 'channel2'] == 31:
        epg1.loc[i, 'channel_name'] = 'یک'
    elif epg1.loc[i, 'channel2'] == 32:
        epg1.loc[i, 'channel_name'] = 'دو'
    elif epg1.loc[i, 'channel2'] == 33:
        epg1.loc[i, 'channel_name'] = 'سه'
    elif epg1.loc[i, 'channel2'] == 34:
        epg1.loc[i, 'channel_name'] = 'چهار'
    elif epg1.loc[i, 'channel2'] == 35:
        epg1.loc[i, 'channel_name'] = 'پنج'
    elif epg1.loc[i, 'channel2'] == 36:
        epg1.loc[i, 'channel_name'] = 'خبر'
    elif epg1.loc[i, 'channel2'] == 37:
        epg1.loc[i, 'channel_name'] = 'آموزش'
    elif epg1.loc[i, 'channel2'] == 38:
        epg1.loc[i, 'channel_name'] = 'قرآن'
    elif epg1.loc[i, 'channel2'] == 39:
        epg1.loc[i, 'channel_name'] = 'مستند'
    elif epg1.loc[i, 'channel2'] == 42:
        epg1.loc[i, 'channel_name'] = 'نمایش'
    elif epg1.loc[i, 'channel2'] == 43:
        epg1.loc[i, 'channel_name'] = 'افق'
    elif epg1.loc[i, 'channel2'] == 47:
        epg1.loc[i, 'channel_name'] = 'ورزش'
    elif epg1.loc[i, 'channel2'] == 44:
        epg1.loc[i, 'channel_name'] = 'آی فیلم'
    elif epg1.loc[i, 'channel2'] == 48:
        epg1.loc[i, 'channel_name'] = 'سلامت'
    elif epg1.loc[i, 'channel2'] == 49:
        epg1.loc[i, 'channel_name'] = 'نسیم'
    elif epg1.loc[i, 'channel2'] == 69:
        epg1.loc[i, 'channel_name'] = 'تماشا'
    elif epg1.loc[i, 'channel2'] == 40:
        epg1.loc[i, 'channel_name'] = 'شما'
    elif epg1.loc[i, 'channel2'] == 41:
        epg1.loc[i, 'channel_name'] = 'ایرانکالا'
    elif epg1.loc[i, 'channel2'] == 51:
        epg1.loc[i, 'channel_name'] = 'رادیو ایران'
    elif epg1.loc[i, 'channel2'] == 52:
        epg1.loc[i, 'channel_name'] = 'رادیو قرآن'
    elif epg1.loc[i, 'channel2'] == 53:
        epg1.loc[i, 'channel_name'] = 'رادیو فرهنگ'
    elif epg1.loc[i, 'channel2'] == 54:
        epg1.loc[i, 'channel_name'] = 'رادیو سلامت'
    elif epg1.loc[i, 'channel2'] == 55:
        epg1.loc[i, 'channel_name'] = 'رادیو ورزش'
    elif epg1.loc[i, 'channel2'] == 56:
        epg1.loc[i, 'channel_name'] = 'رادیو تلاوت'
    elif epg1.loc[i, 'channel2'] == 57:
        epg1.loc[i, 'channel_name'] = 'رادیو معارف'
    elif epg1.loc[i, 'channel2'] == 60:
        epg1.loc[i, 'channel_name'] = 'آفتاب'
    elif epg1.loc[i, 'channel2'] == 61:
        epg1.loc[i, 'channel_name'] = 'البرز'
    elif epg1.loc[i, 'channel2'] == 70:
        epg1.loc[i, 'channel_name'] = 'اصفهان'
    elif epg1.loc[i, 'channel2'] == 71:
        epg1.loc[i, 'channel_name'] = 'دنا'
    elif epg1.loc[i, 'channel2'] == 72:
        epg1.loc[i, 'channel_name'] = 'سهند'
    elif epg1.loc[i, 'channel2'] == 74:
        epg1.loc[i, 'channel_name'] = 'فارس'
#    elif epg1.loc[i, 'channel2'] == 218:
#        epg1.loc[i, 'channel_name'] = 'فارس'
#    elif epg1.loc[i, 'channel2'] == 219:
#        epg1.loc[i, 'channel_name'] = 'فارس'
#    elif epg1.loc[i, 'channel2'] == 220:
#        epg1.loc[i, 'channel_name'] = 'فارس'
#    elif epg1.loc[i, 'channel2'] == 221:
#        epg1.loc[i, 'channel_name'] = 'فارس'
#    elif epg1.loc[i, 'channel2'] == 222:
#        epg1.loc[i, 'channel_name'] = 'فارس'
#    elif epg1.loc[i, 'channel2'] == 223:
#        epg1.loc[i, 'channel_name'] = 'فارس'
#    elif epg1.loc[i, 'channel2'] == 224:
#        epg1.loc[i, 'channel_name'] = 'فارس'
#    elif epg1.loc[i, 'channel2'] == 225:
#        epg1.loc[i, 'channel_name'] = 'فارس'
#    elif epg1.loc[i, 'channel2'] == 226:
#        epg1.loc[i, 'channel_name'] = 'فارس'
#    elif epg1.loc[i, 'channel2'] == 234:
#        epg1.loc[i, 'channel_name'] = 'فارس'
#    elif epg1.loc[i, 'channel2'] == 235:
#        epg1.loc[i, 'channel_name'] = 'رادیو اردبیل'
    elif epg1.loc[i, 'channel2'] == 237:
        epg1.loc[i, 'channel_name'] = 'رادیو قم'
    elif epg1.loc[i, 'channel2'] == 248:
        epg1.loc[i, 'channel_name'] = 'سپهر'
    else: 
        epg1.loc[i, 'channel_name'] = 'NO'
 ######################## DETERMINATION OF TITLE FROM EPG KOUHZADI TO EVENT_LOGIN ########################
session_data = session_data.reset_index()
del session_data['index']
del session_data['time_code']
del epg1['DTDay']
del epg1['Dec_Full']
del epg1['Dec_Summary']
del epg1['ID_Day_Item']
del epg1['ID_Kind']
del epg1['ID_Program']
del epg1['Time_Play']
del epg1['EP']
#del epg1['Length']
del epg1['channel2']
del epg1['ID_SUB_Item']
del epg1['s_date_EPG']
del epg1['e_dete']
del epg1['e_date_EPG']
del epg1['channel']

epg1.sort_values('s_time_EPG', axis = 0, ascending = True, inplace = True, na_position ='last')
channel_name = epg1.copy()
channel_name.drop_duplicates(subset =['channel_name'], keep = 'last', inplace = True)
channel_name = channel_name[~channel_name.channel_name.str.contains("NO")]
channel_name = channel_name.reset_index()
del channel_name['index']
epg1 = epg1.reset_index()
del epg1['index']
total_channel = pd.DataFrame()
for ii in range(0, len(channel_name)):
    print(ii)
    name_list = channel_name.loc[ii, 'channel_name']
    epg1_channel = epg1.query("channel_name == @name_list")
    epg1_first = epg1_channel.copy()
    epg1_second = epg1_channel.copy()
    epg1_third = epg1_channel.copy()
    epg1_first = epg1_channel.shift(-1)
    epg1_second = epg1_channel.shift(-2)
    epg1_third = epg1_channel.shift(-3)
    epg1_channel['first_program_EPG'] = epg1_first['Name_Item']
    epg1_channel['first_s_time_EPG'] = epg1_first['s_time_EPG']
    epg1_channel['first_e_time_EPG'] = epg1_first['e_time_EPG']
    epg1_channel['first_length'] = epg1_first['Length']
    epg1_channel['second_program_EPG'] = epg1_second['Name_Item']
    epg1_channel['second_s_time_EPG'] = epg1_second['s_time_EPG']
    epg1_channel['second_e_time_EPG'] = epg1_second['e_time_EPG']
    epg1_channel['second_length'] = epg1_second['Length']
    epg1_channel['third_program_EPG'] = epg1_third['Name_Item']
    epg1_channel['third_s_time_EPG'] = epg1_third['s_time_EPG']
    epg1_channel['third_e_time_EPG'] = epg1_third['e_time_EPG']
    epg1_channel['third_length'] = epg1_third['Length']
    total_channel = total_channel.append([epg1_channel])
    
total_channel = total_channel.reset_index()
del total_channel['index']

session_data = pd.merge(session_data, total_channel, on = ['channel_name'])
session_data = session_data[session_data['time'] >= session_data['s_time_EPG']]
session_data = session_data[session_data['time'] <= session_data['e_time_EPG']]
session_data = session_data.rename(columns={"Name_Item":"title"})
del session_data['date']
########################################################################
######################## VISIT ########################
session_data.insert(20, 'counter', 1)
session_data['session_id'] = session_data['session_id'].astype(str)
session_data['session_id'].replace('', 'NO', inplace=True)
session_data['session_id'] = session_data['session_id'].str.replace('nan', 'NO')
session_data_visit = session_data.copy()
session_data_visit = session_data_visit[~session_data_visit.session_id.str.contains('NO')]
session_data_visit.drop_duplicates(subset =['channel_name', 'session_id', 'title'], keep = 'first', inplace = True)
session_data_visit = session_data_visit.groupby(['title', 'channel_name']).sum().reset_index()
session_data_visit['title'].replace('', 'NO', inplace=True)
session_data_visit = session_data_visit[~session_data_visit.title.str.contains('NO')]
session_data_visit=session_data_visit.rename(columns={"counter":"visit"})
######################## DURATION ########################
start = time.time()  
#session_duration = session_data.copy()
session_data['session_id'] = session_data['session_id'].astype(str)
session_data['session_id'].replace('', 'NO', inplace=True)
session_data['session_id'] = session_data['session_id'].str.replace('nan', 'NO')
session_data = session_data[~session_data.session_id.str.contains('NO')]
session_data = session_data.reset_index()
del session_data['index']

session_list_unique = session_data['session_id']
session_list_unique = pd.DataFrame(session_list_unique)
session_list_unique.drop_duplicates(subset =['session_id'], keep = 'last', inplace = True)
session_list_unique = session_list_unique.reset_index()
del session_list_unique['index']





def calculate_session_df(session_df, length, counter):
    hour_time = session_df.loc[length, 'time'].hour
    minute_time = session_df.loc[length, 'time'].minute
    second_time = session_df.loc[length, 'time'].second
    duration_length_time = hour_time*3600 + minute_time*60 + second_time
    
    hour_e_time_EPG = session_df.loc[length, 'e_time_EPG'].hour
    minute_e_time_EPG = session_df.loc[length, 'e_time_EPG'].minute
    second_e_time_EPG = session_df.loc[length, 'e_time_EPG'].second
    duration_length_e_time_EPG = hour_e_time_EPG*3600 + minute_e_time_EPG*60 + second_e_time_EPG
    
    hour_time_first_length = session_df.loc[length, 'first_length'].hour
    minute_time_first_length = session_df.loc[length, 'first_length'].minute
    second_time_first_length = session_df.loc[length, 'first_length'].second
    duration_length_first_length = hour_time_first_length*3600 + minute_time_first_length*60 + second_time_first_length
    
    hour_time_second_length = session_df.loc[length, 'second_length'].hour
    minute_time_second_length = session_df.loc[length, 'second_length'].minute
    second_time_second_length = session_df.loc[length, 'second_length'].second
    duration_length_second_length = hour_time_second_length*3600 + minute_time_second_length*60 + second_time_second_length
    
    hour_time_third_length = session_df.loc[length, 'third_length'].hour
    minute_time_third_length = session_df.loc[length, 'third_length'].minute
    second_time_third_length = session_df.loc[length, 'third_length'].second
    duration_length_third_length = hour_time_third_length*3600 + minute_time_third_length*60 + second_time_third_length
    
    hour_time_counter = session_df.loc[length-counter, 'time'].hour
    minute_time_counter = session_df.loc[length-counter, 'time'].minute
    second_time_counter = session_df.loc[length-counter, 'time'].second
    duration_length_time_counter = hour_time_counter*3600 + minute_time_counter*60 + second_time_counter
    
    return duration_length_e_time_EPG, duration_length_time, duration_length_first_length, \
            duration_length_second_length, duration_length_third_length, duration_length_time_counter

def calculate_session_df_action_2(session_df_action_2, length, counter):
    hour_time = session_df_action_2.loc[length, 'time'].hour
    minute_time = session_df_action_2.loc[length, 'time'].minute
    second_time = session_df_action_2.loc[length, 'time'].second
    duration_length_time = hour_time*3600 + minute_time*60 + second_time
    
    hour_e_time_EPG = session_df_action_2.loc[length, 'e_time_EPG'].hour
    minute_e_time_EPG = session_df_action_2.loc[length, 'e_time_EPG'].minute
    second_e_time_EPG = session_df_action_2.loc[length, 'e_time_EPG'].second
    duration_length_e_time_EPG = hour_e_time_EPG*3600 + minute_e_time_EPG*60 + second_e_time_EPG
    
    hour_time_first_length = session_df_action_2.loc[length, 'first_length'].hour
    minute_time_first_length = session_df_action_2.loc[length, 'first_length'].minute
    second_time_first_length = session_df_action_2.loc[length, 'first_length'].second
    duration_length_first_length = hour_time_first_length*3600 + minute_time_first_length*60 + second_time_first_length
    
    hour_time_second_length = session_df_action_2.loc[length, 'second_length'].hour
    minute_time_second_length = session_df_action_2.loc[length, 'second_length'].minute
    second_time_second_length = session_df_action_2.loc[length, 'second_length'].second
    duration_length_second_length = hour_time_second_length*3600 + minute_time_second_length*60 + second_time_second_length
    
    hour_time_third_length = session_df_action_2.loc[length, 'third_length'].hour
    minute_time_third_length = session_df_action_2.loc[length, 'third_length'].minute
    second_time_third_length = session_df_action_2.loc[length, 'third_length'].second
    duration_length_third_length = hour_time_third_length*3600 + minute_time_third_length*60 + second_time_third_length
    
    hour_time_counter = session_df_action_2.loc[length-counter, 'time'].hour
    minute_time_counter = session_df_action_2.loc[length-counter, 'time'].minute
    second_time_counter = session_df_action_2.loc[length-counter, 'time'].second
    duration_length_time_counter = hour_time_counter*3600 + minute_time_counter*60 + second_time_counter
    
    return duration_length_e_time_EPG, duration_length_time, duration_length_first_length, \
            duration_length_second_length, duration_length_third_length, duration_length_time_counter





                ######## function of calculate of duration ########
def calculate_one(session_df, length, counter):
    counter = 0
    [duration_length_e_time_EPG - duration_length_time, duration_length_first_length, duration_length_second_length, duration_length_third_length] = calculate_session_df(session_df, length, counter)
    duration_dif = duration_length_e_time_EPG - duration_length_time
    if duration_dif >= 1800:
        duration = 1800
    elif duration_dif <= 1800:
        if duration_dif + duration_length_first_length > 1800:
            duration = duration_dif
            duration_first = 1800 - duration_dif
        elif duration_dif + duration_length_first_length + duration_length_second_length > 1800:
            duration = duration_dif
            duration_first = duration_length_first_length
            duration_second = 1800 - duration_dif - duration_length_first_length
        elif duration_dif + duration_length_first_length + duration_length_second_length + duration_length_third_length > 1800:
            duration = duration_dif
            duration_first = duration_length_first_length
            duration_second = 1800 - duration_length_second_length
            duration_third = 1800 - duration_dif - duration_length_first_length - duration_length_second_length 
#    print("duration1: ", duration)
    return duration, duration_first, duration_second, duration_third

def calculate_two(session_df, length):
    counter = 0
    [duration_length_e_time_EPG - duration_length_time, duration_length_first_length, duration_length_second_length, duration_length_third_length] = calculate_session_df(session_df, length, counter)
    duration_length_e_time_EPG_0 = duration_length_e_time_EPG
    duration_length_time_0 = duration_length_time
    duration_length_first_length_0 = duration_length_first_length
    duration_length_second_length_0 = duration_length_second_length
    duration_length_third_length_0 = duration_length_third_length
    length = length - 1
    [duration_length_e_time_EPG - duration_length_time, duration_length_first_length, duration_length_second_length, duration_length_third_length] = calculate_session_df(session_df, length, counter)
    duration_dif = duration_length_e_time_EPG - duration_length_time_0
    if duration_dif >= 1800:
        duration = 1800
    elif duration_dif < 1800:
        if duration_dif + duration_length_first_length > 1800:
            duration = duration_dif
            duration_first = 1800 - duration_dif
        elif duration_dif + duration_length_first_length + duration_length_second_length > 1800:
            duration = duration_dif
            duration_first = duration_length_first_length
            duration_second = 1800 - duration_dif - duration_length_first_length
        elif duration_dif + duration_length_first_length + duration_length_second_length + duration_length_third_length > 1800:
            duration = duration_dif
            duration_first = duration_length_first_length
            duration_second = 1800 - duration_length_second_length
            duration_third = 1800 - duration_dif - duration_length_first_length - duration_length_second_length
    return duration, duration_first, duration_second, duration_third

    
#    hour_time = session_df.loc[length, 'time'].hour
#    minute_time = session_df.loc[length, 'time'].minute
#    second_time = session_df.loc[length, 'time'].second
#    hour_e_time_EPG = session_df.loc[length-1, 'e_time_EPG'].hour
#    minute_e_time_EPG = session_df.loc[length-1, 'e_time_EPG'].minute
#    second_e_time_EPG = session_df.loc[length-1, 'e_time_EPG'].second
#    duration = (hour_e_time_EPG - hour_time) * 3600 + (minute_e_time_EPG - minute_time) * 60 + (second_e_time_EPG - second_time)
#    if duration > 1800:
#        duration = 1800
#    print("duration2: ", duration)
#    return duration

def calculate_three(session_df_action_2, length):
    hour_time = session_df_action_2.loc[length, 'time'].hour
    minute_time = session_df_action_2.loc[length, 'time'].minute
    second_time = session_df_action_2.loc[length, 'time'].second
    hour_e_time_EPG = session_df_action_2.loc[length, 'e_time_EPG'].hour
    minute_e_time_EPG = session_df_action_2.loc[length, 'e_time_EPG'].minute
    second_e_time_EPG = session_df_action_2.loc[length, 'e_time_EPG'].second
    duration = (hour_e_time_EPG - hour_time) * 3600 + (minute_e_time_EPG - minute_time) * 60 + (second_e_time_EPG - second_time)
    if duration > 1800:
        duration = 1800
    print("duration3: ", duration)
    return duration

def calculate_four(session_df_action_2, length, counter):
    hour_time = session_df_action_2.loc[length-counter, 'time'].hour
    minute_time = session_df_action_2.loc[length-counter, 'time'].minute
    second_time = session_df_action_2.loc[length-counter, 'time'].second
    hour_e_time_EPG = session_df_action_2.loc[length, 'e_time_EPG'].hour
    minute_e_time_EPG = session_df_action_2.loc[length, 'e_time_EPG'].minute
    second_e_time_EPG = session_df_action_2.loc[length, 'e_time_EPG'].second
    duration = (hour_e_time_EPG - hour_time) * 3600 + (minute_e_time_EPG - minute_time) * 60 + (second_e_time_EPG - second_time)
    if duration > 1800:
        duration = 1800
    print("duration4: ", duration)
    return duration

def calculate_five(session_df, length):
    hour_time = session_df.loc[0, 'time'].hour
    minute_time = session_df.loc[0, 'time'].minute
    second_time = session_df.loc[0, 'time'].second
    hour_e_time_EPG = session_df.loc[length, 'time'].hour
    minute_e_time_EPG = session_df.loc[length, 'time'].minute
    second_e_time_EPG = session_df.loc[length, 'time'].second
    duration = (hour_e_time_EPG - hour_time) * 3600 + (minute_e_time_EPG - minute_time) * 60 + (second_e_time_EPG - second_time)
    if duration > 1800:
        duration = 1800
    print("duration5: ", duration)
    return duration

def calculate_six(session_df_action_2, length, counter):
    hour_time = session_df_action_2.loc[length-counter, 'time'].hour
    minute_time = session_df_action_2.loc[length-counter, 'time'].minute
    second_time = session_df_action_2.loc[length-counter, 'time'].second
    hour_e_time_EPG = session_df_action_2.loc[length, 'time'].hour
    minute_e_time_EPG = session_df_action_2.loc[length, 'time'].minute
    second_e_time_EPG = session_df_action_2.loc[length, 'time'].second
    duration = (hour_e_time_EPG - hour_time) * 3600 + (minute_e_time_EPG - minute_time) * 60 + (second_e_time_EPG - second_time)
    if duration > 1800:
        duration = 1800
    print("duration6: ", duration)
    return duration

def calculate_seven(session_df, length, counter):
    [duration_length_e_time_EPG - duration_length_time, duration_length_first_length, duration_length_second_length, duration_length_third_length] = calculate_session_df(session_df, length, counter)
    duration_dif = duration_length_e_time_EPG - duration_length_time_counter
    if duration_dif >= 1800:
        duration = 1800
    elif duration_dif <= 1800:
        if duration_dif + duration_length_first_length > 1800:
            duration = duration_dif
            duration_first = 1800 - duration_dif
        elif duration_dif + duration_length_first_length + duration_length_second_length > 1800:
            duration = duration_dif
            duration_first = duration_length_first_length
            duration_second = 1800 - duration_dif - duration_length_first_length
        elif duration_dif + duration_length_first_length + duration_length_second_length + duration_length_third_length > 1800:
            duration = duration_dif
            duration_first = duration_length_first_length
            duration_second = 1800 - duration_length_second_length
            duration_third = 1800 - duration_dif - duration_length_first_length - duration_length_second_length 
#    print("duration1: ", duration)
    return duration, duration_first, duration_second, duration_third
    
    
    
    
                ######## function of dataframe create for any session ########

def session1_func():
   session1 = pd.DataFrame()
   session1.insert(0, 'title', '')
   session1.insert(1, 'channel', '')
   session1.insert(2, 'duration_min', '')
   session1.insert(3, 'session_id', '')
   return session1

def session1_fill_func(session1, session_df_action_2, duration, xx):
    session1.loc[xx, 'title'] = session_df_action_2.loc[xx, 'title']
    session1.loc[xx, 'channel'] = session_df_action_2.loc[xx, 'channel_name']
    session1.loc[xx, 'duration_min'] = duration
    session1.loc[xx, 'session_id'] = session_df_action_2.loc[xx, 'session_id']
    return session1

def session1_df_fill_func(session1, session_df, duration, j):
    session1.loc[j, 'title'] = session_df.loc[j, 'title']
    session1.loc[j, 'channel'] = session_df.loc[j, 'channel_name']
    session1.loc[j, 'duration_min'] = duration
    session1.loc[j, 'session_id'] = session_df.loc[j, 'session_id']
    return session1

                ######## session has one record ########

def sessionPer_duration_one_length(session_df, i):
    session = pd.DataFrame()
    session.insert(0, 'title', '')
    session.insert(1, 'channel', '')
    session.insert(2, 'duration_min', '')
    session.insert(3, 'session_id', '')
    if session_df.loc[0, 'action_id'] == '2':
        session.loc[0, 'title'] = session_df.loc[0, 'title']
        session.loc[0, 'channel'] = session_df.loc[0, 'channel_name']
        session.loc[0, 'duration_min'] = 0
        session.loc[0, 'session_id'] = session_df.loc[0, 'session_id']
    if session_df.loc[0, 'action_id'] != '2':
        session.loc[0, 'title'] = session_df.loc[0, 'title']
        session.loc[0, 'channel'] = session_df.loc[0, 'channel_name']
        length = 0
        duration = calculate_one(session_df, length)
        session.loc[0, 'duration_min'] = duration
        session.loc[0, 'session_id'] = session_df.loc[0, 'session_id']
    return session

                ######## session has two records ########

def sessionPer_duration_two_length(session_df, i):
    session = pd.DataFrame()
    session.insert(0, 'title', '')
    session.insert(1, 'channel', '')
    session.insert(2, 'duration_min', '')
    session.insert(3, 'session_id', '')
    if session_df.loc[0, 'action_id'] == '2' and session_df.loc[1, 'action_id'] == '2':
        session.loc[0, 'title'] = session_df.loc[0, 'title']
        session.loc[0, 'channel'] = session_df.loc[0, 'channel_name']
        session.loc[0, 'duration_min'] = 0
        session.loc[0, 'session_id'] = session_df.loc[0, 'session_id']
    if session_df.loc[0, 'action_id'] == '2' and session_df.loc[1, 'action_id'] != '2':
        length = 1
        duration = calculate_one(session_df, length)
        session.loc[0, 'title'] = session_df.loc[1, 'title']
        session.loc[0, 'channel'] = session_df.loc[1, 'channel_name']
        session.loc[0, 'session_id'] = session_df.loc[1, 'session_id']
        session.loc[0, 'duration_min'] = duration
    if session_df.loc[0, 'action_id'] != '2' and session_df.loc[1, 'action_id'] == '2':
        if session_df.loc[0, 'channel_name'] == session_df.loc[1, 'channel_name'] and \
           session_df.loc[0, 'title'] == session_df.loc[1, 'title']:
            length = 1
            duration = calculate_five(session_df, length)
            session.loc[0, 'title'] = session_df.loc[1, 'title']
            session.loc[0, 'channel'] = session_df.loc[1, 'channel_name']
            session.loc[0, 'session_id'] = session_df.loc[1, 'session_id']
            session.loc[0, 'duration_min'] = duration
        else:
            length = 0
            duration = calculate_one(session_df, length)
            session.loc[0, 'title'] = session_df.loc[0, 'title']
            session.loc[0, 'channel'] = session_df.loc[0, 'channel_name']
            session.loc[0, 'session_id'] = session_df.loc[0, 'session_id']
            session.loc[0, 'duration_min'] = duration
    if session_df.loc[0, 'action_id'] != '2' and session_df.loc[1, 'action_id'] != '2':
        if session_df.loc[0, 'channel_name'] == session_df.loc[1, 'channel_name'] and \
           session_df.loc[0, 'title'] == session_df.loc[1, 'title']:
            length = 1
            duration = calculate_two(session_df, length)
            session.loc[0, 'title'] = session_df.loc[1, 'title']
            session.loc[0, 'channel'] = session_df.loc[1, 'channel_name']
            session.loc[0, 'session_id'] = session_df.loc[1, 'session_id']
            session.loc[0, 'duration_min'] = duration
        else:
            length = 0
            duration = calculate_one(session_df, length)
            session.loc[0, 'title'] = session_df.loc[0, 'title']
            session.loc[0, 'channel'] = session_df.loc[0, 'channel_name']
            session.loc[0, 'session_id'] = session_df.loc[0, 'session_id']
            session.loc[0, 'duration_min'] = duration
            length = 1
            duration = calculate_one(session_df, length)
            session.loc[0, 'title'] = session_df.loc[1, 'title']
            session.loc[0, 'channel'] = session_df.loc[1, 'channel_name']
            session.loc[0, 'session_id'] = session_df.loc[1, 'session_id']
            session.loc[0, 'duration_min'] = duration
    return session

                ######## session has more than two records ########
           
def sessionPer_duration_more_length(session_df, i):
    session = pd.DataFrame()
    session.insert(0, 'title', '')
    session.insert(1, 'channel', '')
    session.insert(2, 'duration_min', '')
    session.insert(3, 'session_id', '')
    action_id_2 = session_df [session_df.action_id.str.contains('2')]
    action_id_2.to_excel('action_id_2.xlsx')
    action_id_2 = pd.read_excel (r'C:\Users\PC\Desktop\sepehr_log\action_id_2.xlsx')
    action_id_2 = action_id_2.rename(columns={"Unnamed: 0":"number_index"})
    ######## session has not action_id of 2 ########
    if len(action_id_2) == 0:
        if len(session_df) == 1:
            session1 = session1_func()
            length = 0
#            session_df_action_2 = session_df
            duration = calculate_one(session_df, length)
            session1.loc[0, 'title'] = session_df.loc[0, 'title']
            session1.loc[0, 'channel'] = session_df.loc[0, 'channel_name']
            session1.loc[0, 'duration_min'] = duration
            session1.loc[0, 'session_id'] = session_df.loc[0, 'session_id']
            session = session1.append(session)
            del session1    
        else:
            counter = 0
            for j in range(0, len(session_df) - 1):
                session1 = session1_func()
                if session_df.loc[j, 'channel_name'] != session_df.loc[j+1, 'channel_name'] or \
                   session_df.loc[j, 'title'] != session_df.loc[j+1, 'title']:
                       if counter == 0:
                           length = j
                           session_df_action_2 = session_df
                           duration = calculate_three(session_df_action_2, length)
                           print("number 13")
                           xx = j
                           session1 = session1_fill_func(session1, session_df_action_2, duration, xx)
                           session = session1.append(session)
                           del session1
                       elif counter != 0 and j == len(session_df) - 2:
                          session1 = session1_func()
                          length = j + 1
                          duration = calculate_one(session_df, length)
                          print("number 16")
                          session1.loc[j, 'title'] = session_df.loc[j+1, 'title']
                          session1.loc[j, 'channel'] = session_df.loc[j+1, 'channel_name']
                          session1.loc[j, 'duration_min'] = duration
                          session1.loc[j, 'session_id'] = session_df.loc[j+1, 'session_id']
                          session = session1.append(session)
                          del session1
                          counter = 0
                          
                          
#                          session1 = session1_func()
#                          length = j + 1
#                          duration = calculate_one(session_df, length)
#                          print("number 16")
#                          session1.loc[j, 'title'] = session_df.loc[j+1, 'title']
#                          session1.loc[j, 'channel'] = session_df.loc[j+1, 'channel_name']
#                          session1.loc[j, 'duration_min'] = duration
#                          session1.loc[j, 'session_id'] = session_df.loc[j+1, 'session_id']
#                          session = session1.append(session)
#                          del session1
#                          counter = 0
                          
                       elif counter != 0:
                            length = j
                            session_df_action_2 = session_df
                            duration = calculate_four(session_df_action_2, length, counter)
                            print("number 14")
                            session1 = session1_df_fill_func(session1, session_df, duration, j)
                            session = session1.append(session)
                            del session1
                            counter = 0
                elif session_df.loc[j, 'channel_name'] == session_df.loc[j+1, 'channel_name'] or \
                   session_df.loc[j, 'title'] == session_df.loc[j+1, 'title']:
                       counter = counter + 1
                       if j == len(session_df) - 2:
                           length = j + 1
#                           session_df_action_2 = session_df
                           duration = calculate_seven(session_df, length, counter)
                           print("number 17")
                           session1 = session1_df_fill_func(session1, session_df, duration, j)
                           session = session1.append(session)
                           del session1
    ######## session has action_id of 2 ########
    else:
        for k in range(0, len(action_id_2)+1):
            if k == 0:        #### STEP 1 ####
                end = action_id_2.loc[0, 'number_index']
                session_df_action_2 = session_df.iloc[0:end+1, :]
                session_df_action_2 = session_df_action_2.reset_index()
                del session_df_action_2['index']
                session_df_action_2.to_excel('session_df_action_2.xlsx') # del
                counter = 0
                for j in range(0, len(session_df_action_2) - 1):
                    session1 = session1_func()
                    if session_df_action_2.loc[j, 'channel_name'] != session_df_action_2.loc[j+1, 'channel_name'] or \
                       session_df_action_2.loc[j, 'title'] != session_df_action_2.loc[j+1, 'title']:
                           if counter == 0:
                               length = j
                               duration = calculate_three(session_df_action_2, length)
#                               print("number 1")
                               xx = j
                               session1_fill_func(session1, session_df_action_2, duration, xx)
                               session = session1.append(session)
                               del session1
                           elif counter != 0 and session_df_action_2.loc[j, 'action_id'] != '2':
                                length = j
                                duration = calculate_four(session_df_action_2, length, counter)
#                                print("number 2")
                                xx = j
                                session1_fill_func(session1, session_df_action_2, duration, xx)
                                session = session1.append(session)
                                del session1
                                counter = 0
                           elif counter != 0 and session_df_action_2.loc[j, 'action_id'] == '2':
                                length = j
                                duration = calculate_six(session_df_action_2, length, counter)
                                print("number 3")
                                xx = j
                                session1_fill_func(session1, session_df_action_2, duration, xx)
                                session = session1.append(session)
                                del session1
                                counter = 0
                    elif session_df_action_2.loc[j, 'channel_name'] == session_df_action_2.loc[j+1, 'channel_name'] or \
                       session_df_action_2.loc[j, 'title'] == session_df_action_2.loc[j+1, 'title']:
                           counter = counter + 1
                           if j == len(session_df_action_2) - 2:
                               length = j + 1
                               duration = calculate_six(session_df_action_2, length, counter)
                               print("number 4")
                               xx = j
                               session1_fill_func(session1, session_df_action_2, duration, xx)
                               session = session1.append(session)
                               del session1
                               counter = 0
                if len(session_df_action_2) == 1:
                    session1 = session1_func()
                    length = 0
                    duration = calculate_three(session_df_action_2, length)
                    xx = 0
                    session1_fill_func(session1, session_df_action_2, duration, xx)
                    session = session1.append(session)
                    del session1               
            elif k != 0 and k != len(action_id_2):    #### STEP 2 ####
                start = action_id_2.loc[k-1, 'number_index'] + 1
                end = action_id_2.loc[k, 'number_index']
                session_df_action_2 = session_df.iloc[start:end+1, :]
                session_df_action_2 = session_df_action_2.reset_index()
                del session_df_action_2['index']
                session_df_action_2.to_excel('session_df_action_2.xlsx')
                counter = 0
                for j in range(0, len(session_df_action_2) - 1):
                    session1 = session1_func()
                    if session_df_action_2.loc[j, 'channel_name'] != session_df_action_2.loc[j+1, 'channel_name'] or \
                       session_df_action_2.loc[j, 'title'] != session_df_action_2.loc[j+1, 'title']:
                           if counter == 0:
                               length = j
                               duration = calculate_three(session_df_action_2, length)
#                               print("number 5")
                               xx = j
                               session1_fill_func(session1, session_df_action_2, duration, xx)
                               session = session1.append(session)
                               del session1
                           elif counter != 0 and session_df_action_2.loc[j, 'action_id'] != '2':
                                length = j
                                duration = calculate_four(session_df_action_2, length, counter)
#                                print("number 6")
                                xx = j
                                session1_fill_func(session1, session_df_action_2, duration, xx)
                                session = session1.append(session)
                                del session1
                                counter = 0
                           elif counter != 0 and session_df_action_2.loc[j, 'action_id'] == '2':
                                length = j
                                duration = calculate_six(session_df_action_2, length, counter)
                                print("number 7")
                                xx = j
                                session1_fill_func(session1, session_df_action_2, duration, xx)
                                session = session1.append(session)
                                del session1
                                counter = 0
                    elif session_df_action_2.loc[j, 'channel_name'] == session_df_action_2.loc[j+1, 'channel_name'] or \
                       session_df_action_2.loc[j, 'title'] == session_df_action_2.loc[j+1, 'title']:
                           counter = counter + 1
                           if j == len(session_df_action_2) - 2:
                               length = j + 1
                               duration = calculate_six(session_df_action_2, length, counter)
                               print("number 8")
                               xx = j
                               session1_fill_func(session1, session_df_action_2, duration, xx)
                               session = session1.append(session)
                               del session1
                               counter = 0
                if len(session_df_action_2) == 1:
                    session1 = session1_func()
                    length = 0
                    duration = calculate_three(session_df_action_2, length)
                    xx = 0
                    session1_fill_func(session1, session_df_action_2, duration, xx)
                    session = session1.append(session)
                    del session1
                if len(session_df_action_2) > 1:
                    if (session_df_action_2.loc[len(session_df_action_2)-1, 'action_id']) != '2' and \
                           (session_df_action_2.loc[len(session_df_action_2)-1, 'channel_name'] != session_df_action_2.loc[len(session_df_action_2)-2, 'channel_name'] or \
                           session_df_action_2.loc[len(session_df_action_2)-1, 'title'] != session_df_action_2.loc[len(session_df_action_2)-2, 'title']):
                               session1 = session1_func()
                               length = len(session_df_action_2)-1
                               duration = calculate_three(session_df_action_2, length)
#                               print("number 19")
                               session1.loc[0, 'title'] = session_df_action_2.loc[length, 'title']
                               session1.loc[0, 'channel'] = session_df_action_2.loc[length, 'channel_name']
                               session1.loc[0, 'duration_min'] = duration
                               session1.loc[0, 'session_id'] = session_df_action_2.loc[length, 'session_id']
                               session = session1.append(session)
                               del session1
            elif k != 0 and k == len(action_id_2) and \
                 session_df.loc[len(session_df)-1, 'action_id'] != '2':  #### STEP 3 ####
                start = action_id_2.loc[k-1, 'number_index'] + 1
                end = len(session_df)
                session_df_action_2 = session_df.iloc[start:end, :]
                session_df_action_2 = session_df_action_2.reset_index()
                del session_df_action_2['index']
                counter = 0
                for j in range(0, len(session_df_action_2) - 1):
                    session1 = session1_func()
                    if session_df_action_2.loc[j, 'channel_name'] != session_df_action_2.loc[j+1, 'channel_name'] or \
                       session_df_action_2.loc[j, 'title'] != session_df_action_2.loc[j+1, 'title']:
                           if counter == 0 and j != len(session_df_action_2) - 2:
                               length = j
                               duration = calculate_three(session_df_action_2, length)
#                               print("number 9")
                               xx = j
                               session1_fill_func(session1, session_df_action_2, duration, xx)
                               session = session1.append(session)
                               del session1
                           elif counter == 0 and j == len(session_df_action_2) - 2:
                               length = j
                               duration = calculate_three(session_df_action_2, length)
#                               print("number 9")
                               xx = j
                               session1_fill_func(session1, session_df_action_2, duration, xx)
                               session = session1.append(session)
                               del session1
                               length = j + 1
                               duration = calculate_three(session_df_action_2, length)
#                               print("number 9")
                               xx = j + 1
                               session1_fill_func(session1, session_df_action_2, duration, xx)
                               session = session1.append(session)
                               del session1
                           elif counter != 0 and j == len(session_df_action_2) - 2:
                                length = j
                                duration = calculate_four(session_df_action_2, length, counter)
#                                print("number 10")
                                xx = j
                                session1_fill_func(session1, session_df_action_2, duration, xx)
                                session = session1.append(session)
                                del session1
                                counter = 0
                                length = j + 1
                                duration = calculate_one(session_df_action_2, length, counter)
#                                print("number 10")
                                xx = j
                                session1_fill_func(session1, session_df_action_2, duration, xx)
                                session = session1.append(session)
                                del session1
                                counter = 0
                           elif counter != 0:
                                length = j
                                duration = calculate_four(session_df_action_2, length, counter)
#                                print("number 10")
                                xx = j
                                session1_fill_func(session1, session_df_action_2, duration, xx)
                                session = session1.append(session)
                                del session1
                                counter = 0
#                           elif counter != 0 and session_df_action_2.loc[j, 'action_id'] == '2':
#                                length = j
#                                duration = calculate_six(session_df_action_2, length, counter)
#                                print("number 11")
#                                xx = j
#                                session1_fill_func(session1, session_df_action_2, duration, xx)
#                                session = session1.append(session)
#                                del session1
#                                counter = 0
                    elif session_df_action_2.loc[j, 'channel_name'] == session_df_action_2.loc[j+1, 'channel_name'] or \
                       session_df_action_2.loc[j, 'title'] == session_df_action_2.loc[j+1, 'title']:
                           counter = counter + 1
                           if j == len(session_df_action_2) - 2:
                               length = j + 1
                               duration = calculate_six(session_df_action_2, length, counter)
                               print("number 12")
                               xx = j
                               session1_fill_func(session1, session_df_action_2, duration, xx)
                               session = session1.append(session)
                               del session1
                               counter = 0
                if len(session_df_action_2) == 1:
                    session1 = session1_func()
                    length = 0
                    duration = calculate_three(session_df_action_2, length)
                    xx = 0
                    session1_fill_func(session1, session_df_action_2, duration, xx)
                    session = session1.append(session)
                    del session1
                if len(session_df_action_2) > 1:
                    if (session_df_action_2.loc[len(session_df_action_2)-1, 'action_id']) != '2' and \
                           (session_df_action_2.loc[len(session_df_action_2)-1, 'channel_name'] != session_df_action_2.loc[len(session_df_action_2)-2, 'channel_name'] or \
                           session_df_action_2.loc[len(session_df_action_2)-1, 'title'] != session_df_action_2.loc[len(session_df_action_2)-2, 'title']):
                               session1 = session1_func()
                               length = len(session_df_action_2)-1
                               duration = calculate_three(session_df_action_2, length)
#                               print("number 18")
                               session1.loc[0, 'title'] = session_df_action_2.loc[length, 'title']
                               session1.loc[0, 'channel'] = session_df_action_2.loc[length, 'channel_name']
                               session1.loc[0, 'duration_min'] = duration
                               session1.loc[0, 'session_id'] = session_df_action_2.loc[length, 'session_id']
                               session = session1.append(session)
                               del session1
            elif k != 0 and k == len(action_id_2) and \
                 session_df.loc[len(session_df)-1, 'action_id'] == '2':  #### STEP 4 ####
                break
    return session
                          
 ################################## CONCLUSION #################################    
session_total = pd.DataFrame()
session_total.insert(0, 'title', '')
session_total.insert(1, 'channel', '')
session_total.insert(2, 'duration_min', '')
session_total.insert(3, 'session_id', '')
lenght_of_data = len(session_list_unique)   
for i in range(0, 1):   # len(session_list_unique)
    print(lenght_of_data,": ", i)
    session_id_from_list = session_list_unique.loc[i, 'session_id']
    session_df = session_data.query("session_id == @session_id_from_list")
    session_df['action_id'] = session_df['action_id'].astype(str)
    session_df = session_df.reset_index()
    del session_df['index']
    try:
        if session_df.loc[0, 'action_id'] == '2' and session_df.loc[1, 'action_id'] == '2' and session_df.loc[2, 'action_id'] == '2':
            session_df = session_df.drop([0,1,2])
        elif session_df.loc[0, 'action_id'] == '2' and session_df.loc[1, 'action_id'] == '2':
            session_df = session_df.drop([0,1])
        elif session_df.loc[0, 'action_id'] == '2':
            session_df = session_df.drop([0])
    except: pass
    session_df = session_df.reset_index()
    del session_df['index']
    session_df.sort_values('time', axis = 0, ascending = True, inplace = True, na_position ='last')
    if len(session_df) == 1:
        session = sessionPer_duration_one_length(session_df, i)
        session_total = session_total.append(session)
    if len(session_df) == 2:
        session = sessionPer_duration_two_length(session_df, i)
        session_total = session_total.append(session)
    if len(session_df) > 2:
        session = sessionPer_duration_more_length(session_df, i)
        session_total = session_total.append(session)

session_total = session_total.reset_index()
del session_total['index']

del session_total['session_id']
session_duration = session_total.groupby(['title', 'channel']).sum().reset_index()
session_duration['duration_min'] = round(session_duration['duration_min']/60, 2)
session_duration_visit = pd.merge(session_duration, session_data_visit, on=['', ''])
print("--- %s min (finish of the program) ---" % round((time.time() - start)/60, 2))

























