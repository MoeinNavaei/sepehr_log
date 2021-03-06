
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
#date_name_start = '12:00:00'
#date_name_end = '17:00:00'
 ######################## READ OF EVENT AND SESSION FILES ########################
read_file_event = pd.read_csv (r'C:\Users\PC\Desktop\sepehr_log\kafka\epkaf_e_08_07_2021.csv',encoding='utf-8')
read_file_login = pd.read_csv (r'C:\Users\PC\Desktop\sepehr_log\kafka\test (1).csv',encoding='utf-8')
 ######################## MERGE OF EVENT AND LOGIN ########################
del read_file_event['time_stamp']
del read_file_event['@version']
del read_file_event['sys_id']
del read_file_event['@timestamp']
del read_file_event['user_agent']
del read_file_event['session_end']
del read_file_event['content_id']
del read_file_event['referer']
del read_file_event['xReferer']
del read_file_event['ip']

event_login = pd.merge(read_file_event, read_file_login, on = ['session_id'])
del event_login['@version']
del event_login['sys_id']
del event_login['@timestamp']
del event_login['service_id']
del event_login['content_name']
#del event_login['channel_name']
del event_login['content_id']
del event_login['content_type_id']

event_login['date'] = event_login['time_stamp'].str[:10]
event_login['time'] = event_login['time_stamp'].str[11:]
del event_login['time_stamp']
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

event_day_hour = pd.to_datetime(event_login.time, errors='coerce')
event_login = event_login.assign(time=event_day_hour.dt.time)
event_day_date = pd.to_datetime(event_login.date, errors='coerce')
event_login = event_login.assign(date=event_day_date.dt.date)

for i in range(0, len(epg1)):
#    print(i)
    if epg1.loc[i, 'channel2'] == 31:
        epg1.loc[i, 'channel_name'] = '????'
    elif epg1.loc[i, 'channel2'] == 32:
        epg1.loc[i, 'channel_name'] = '????'
    elif epg1.loc[i, 'channel2'] == 33:
        epg1.loc[i, 'channel_name'] = '????'
    elif epg1.loc[i, 'channel2'] == 34:
        epg1.loc[i, 'channel_name'] = '????????'
    elif epg1.loc[i, 'channel2'] == 35:
        epg1.loc[i, 'channel_name'] = '??????'
    elif epg1.loc[i, 'channel2'] == 36:
        epg1.loc[i, 'channel_name'] = '??????'
    elif epg1.loc[i, 'channel2'] == 37:
        epg1.loc[i, 'channel_name'] = '??????????'
    elif epg1.loc[i, 'channel2'] == 38:
        epg1.loc[i, 'channel_name'] = '????????'
    elif epg1.loc[i, 'channel2'] == 39:
        epg1.loc[i, 'channel_name'] = '??????????'
    elif epg1.loc[i, 'channel2'] == 42:
        epg1.loc[i, 'channel_name'] = '??????????'
    elif epg1.loc[i, 'channel2'] == 43:
        epg1.loc[i, 'channel_name'] = '??????'
    elif epg1.loc[i, 'channel2'] == 47:
        epg1.loc[i, 'channel_name'] = '????????'
    elif epg1.loc[i, 'channel2'] == 44:
        epg1.loc[i, 'channel_name'] = '???? ????????'
    elif epg1.loc[i, 'channel2'] == 48:
        epg1.loc[i, 'channel_name'] = '??????????'
    elif epg1.loc[i, 'channel2'] == 49:
        epg1.loc[i, 'channel_name'] = '????????'
    elif epg1.loc[i, 'channel2'] == 69:
        epg1.loc[i, 'channel_name'] = '??????????'
    elif epg1.loc[i, 'channel2'] == 40:
        epg1.loc[i, 'channel_name'] = '??????'
    elif epg1.loc[i, 'channel2'] == 41:
        epg1.loc[i, 'channel_name'] = '??????????????????'
    elif epg1.loc[i, 'channel2'] == 51:
        epg1.loc[i, 'channel_name'] = '?????????? ??????????'
    elif epg1.loc[i, 'channel2'] == 52:
        epg1.loc[i, 'channel_name'] = '?????????? ????????'
    elif epg1.loc[i, 'channel2'] == 53:
        epg1.loc[i, 'channel_name'] = '?????????? ??????????'
    elif epg1.loc[i, 'channel2'] == 54:
        epg1.loc[i, 'channel_name'] = '?????????? ??????????'
    elif epg1.loc[i, 'channel2'] == 55:
        epg1.loc[i, 'channel_name'] = '?????????? ????????'
    elif epg1.loc[i, 'channel2'] == 56:
        epg1.loc[i, 'channel_name'] = '?????????? ??????????'
    elif epg1.loc[i, 'channel2'] == 57:
        epg1.loc[i, 'channel_name'] = '?????????? ??????????'
    elif epg1.loc[i, 'channel2'] == 60:
        epg1.loc[i, 'channel_name'] = '??????????'
    elif epg1.loc[i, 'channel2'] == 61:
        epg1.loc[i, 'channel_name'] = '??????????'
    elif epg1.loc[i, 'channel2'] == 70:
        epg1.loc[i, 'channel_name'] = '????????????'
    elif epg1.loc[i, 'channel2'] == 71:
        epg1.loc[i, 'channel_name'] = '??????'
    elif epg1.loc[i, 'channel2'] == 72:
        epg1.loc[i, 'channel_name'] = '????????'
    elif epg1.loc[i, 'channel2'] == 74:
        epg1.loc[i, 'channel_name'] = '????????'
#    elif epg1.loc[i, 'channel2'] == 218:
#        epg1.loc[i, 'channel_name'] = '????????'
#    elif epg1.loc[i, 'channel2'] == 219:
#        epg1.loc[i, 'channel_name'] = '????????'
#    elif epg1.loc[i, 'channel2'] == 220:
#        epg1.loc[i, 'channel_name'] = '????????'
#    elif epg1.loc[i, 'channel2'] == 221:
#        epg1.loc[i, 'channel_name'] = '????????'
#    elif epg1.loc[i, 'channel2'] == 222:
#        epg1.loc[i, 'channel_name'] = '????????'
#    elif epg1.loc[i, 'channel2'] == 223:
#        epg1.loc[i, 'channel_name'] = '????????'
#    elif epg1.loc[i, 'channel2'] == 224:
#        epg1.loc[i, 'channel_name'] = '????????'
#    elif epg1.loc[i, 'channel2'] == 225:
#        epg1.loc[i, 'channel_name'] = '????????'
#    elif epg1.loc[i, 'channel2'] == 226:
#        epg1.loc[i, 'channel_name'] = '????????'
#    elif epg1.loc[i, 'channel2'] == 234:
#        epg1.loc[i, 'channel_name'] = '????????'
#    elif epg1.loc[i, 'channel2'] == 235:
#        epg1.loc[i, 'channel_name'] = '?????????? ????????????'
    elif epg1.loc[i, 'channel2'] == 237:
        epg1.loc[i, 'channel_name'] = '?????????? ????'
    elif epg1.loc[i, 'channel2'] == 248:
        epg1.loc[i, 'channel_name'] = '????????'
    else: 
        epg1.loc[i, 'channel_name'] = 'NO'
 ######################## DETERMINATION OF TITLE FROM EPG KOUHZADI TO EVENT_LOGIN ########################
event_login = event_login.reset_index()
del event_login['index']
del epg1['DTDay']
del epg1['Dec_Full']
del epg1['Dec_Summary']
del epg1['ID_Day_Item']
del epg1['ID_Kind']
del epg1['ID_Program']
del epg1['channel']

event_login = pd.merge(event_login, epg1, on = ['channel_name'])
event_login = event_login[event_login['time'] >= event_login['s_time_EPG']]
event_login = event_login[event_login['time'] <= event_login['e_time_EPG']]
event_login = event_login.rename(columns={"Name_Item":"title"})
######################################################################## 
del event_login['time_code']
del event_login['date']
del event_login['Time_Play']
del event_login['channel2']
del event_login['s_date_EPG']
del event_login['EP']
########################################################################
######################## VISIT ########################
event_login.insert(9, 'counter', 1)
event_login['session_id'].replace('', 'NO', inplace=True)
event_login_visit = event_login.copy()
event_login_visit = event_login_visit[~event_login_visit.session_id.str.contains('NO')]
event_login_visit.drop_duplicates(subset =['channel_name', 'session_id', 'title'], keep = 'first', inplace = True)
event_login_visit = event_login_visit.groupby(['title', 'channel_name']).sum().reset_index()
event_login_visit['title'].replace('', 'NO', inplace=True)
event_login_visit = event_login_visit[~event_login_visit.title.str.contains('NO')]
event_login_visit=event_login_visit.rename(columns={"counter":"visit"})
del event_login_visit['action_id']
######################## ACTIVE_USER ########################
event_login['user_id'].replace('', 'NO', inplace=True)
event_login_user = event_login.copy()
event_login_user['user_id'] = event_login_user['user_id'].astype(str)
event_login_user = event_login_user[~event_login_user.user_id.str.contains('NO')] 
event_login_user.drop_duplicates(subset =['channel_name', 'user_id', 'title'], keep = 'first', inplace = True)
event_login_user=event_login_user.groupby(['title', 'channel_name']).sum().reset_index()
event_login_user['title'].replace('', 'NO', inplace=True)
event_login_user = event_login_user[~event_login_visit.title.str.contains('NO')]
event_login_user=event_login_user.rename(columns={"counter":"active_user"})
del event_login_user['action_id']
######################## TRANSMISSION OF DATA TO NEW DATAFRAME ########################
program_statistics = pd.DataFrame()
program_statistics['title'] = event_login_user['title']
program_statistics['channel'] = event_login_user['channel_name']

event_login_visit = event_login_visit.reset_index()
del event_login_visit['index']
event_login_user = event_login_user.reset_index()
del event_login_user['index']
program_statistics = program_statistics.reset_index()
del program_statistics['index']

event_login_visit = event_login_visit.rename(columns={"channel_name":"channel"})
event_login_user = event_login_user.rename(columns={"channel_name":"channel"})

program_statistics = pd.merge(program_statistics, event_login_visit, on = ['title', 'channel'])
program_statistics = pd.merge(program_statistics, event_login_user, on = ['title', 'channel'])

event_login['action_id'] = event_login['action_id'].astype(int)
######################## DURATION ########################
start = time.time()  
event_login_duration = event_login.copy()
event_login_duration = event_login_duration[~event_login_duration.session_id.str.contains('NO')]
event_login_duration = event_login_duration.reset_index()
del event_login_duration['index']

session_list_unique = event_login_duration['session_id']
session_list_unique = pd.DataFrame(session_list_unique)
session_list_unique.drop_duplicates(subset =['session_id'], keep = 'last', inplace = True)
session_list_unique = session_list_unique.reset_index()
del session_list_unique['index']
                ######## function of calculate of duration ########
def calculate_one(session_df, length):
    hour_time = session_df.loc[length, 'time'].hour
    minute_time = session_df.loc[length, 'time'].minute
    second_time = session_df.loc[length, 'time'].second
    hour_e_time_EPG = session_df.loc[length, 'e_time_EPG'].hour
    minute_e_time_EPG = session_df.loc[length, 'e_time_EPG'].minute
    second_e_time_EPG = session_df.loc[length, 'e_time_EPG'].second
    duration = (hour_e_time_EPG - hour_time) * 3600 + (minute_e_time_EPG - minute_time) * 60 + (second_e_time_EPG - second_time)
    if duration > 1800:
        duration = 1800
#    print("duration1: ", duration)
    return duration

def calculate_two(session_df, length):
    hour_time = session_df.loc[length, 'time'].hour
    minute_time = session_df.loc[length, 'time'].minute
    second_time = session_df.loc[length, 'time'].second
    hour_e_time_EPG = session_df.loc[length-1, 'e_time_EPG'].hour
    minute_e_time_EPG = session_df.loc[length-1, 'e_time_EPG'].minute
    second_e_time_EPG = session_df.loc[length-1, 'e_time_EPG'].second
    duration = (hour_e_time_EPG - hour_time) * 3600 + (minute_e_time_EPG - minute_time) * 60 + (second_e_time_EPG - second_time)
    if duration > 1800:
        duration = 1800
#    print("duration2: ", duration)
    return duration

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
#    print("duration3: ", duration)
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
#    print("duration4: ", duration)
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
#    print("duration5: ", duration)
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
#    print("duration6: ", duration)
    return duration

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
            session_df_action_2 = session_df
            duration = calculate_three(session_df_action_2, length)
            session1.loc[0, 'title'] = session_df.loc[0, 'title']
            session1.loc[0, 'channel'] = session_df.loc[0, 'channel_name']
            session1.loc[0, 'duration_min'] = duration
            session1.loc[0, 'session_id'] = session_df.loc[0, 'session_id']
            session = session1.append(session)
            del session1    
        counter = 0
        for j in range(0, len(session_df) - 1):
            session1 = session1_func()
            if session_df.loc[j, 'channel_name'] != session_df.loc[j+1, 'channel_name'] or \
               session_df.loc[j, 'title'] != session_df.loc[j+1, 'title']:
                   if counter == 0:
                       length = j
                       session_df_action_2 = session_df
                       duration = calculate_three(session_df_action_2, length)
#                       print("number 13")
                       xx = j
                       session1 = session1_fill_func(session1, session_df_action_2, duration, xx)
                       session = session1.append(session)
                       del session1
                   elif counter != 0 and session_df.loc[j, 'action_id'] != '2':
                        length = j
                        session_df_action_2 = session_df
                        duration = calculate_four(session_df_action_2, length, counter)
#                        print("number 14")
                        session1 = session1_df_fill_func(session1, session_df, duration, j)
                        session = session1.append(session)
                        del session1
                        counter = 0
                   elif counter != 0 and session_df.loc[j, 'action_id'] == '2':
                        length = j
                        session_df_action_2 = session_df
                        duration = calculate_six(session_df_action_2, length, counter)
#                        print("number 15")
                        session1 = session1_df_fill_func(session1, session_df, duration, j)
                        session = session1.append(session)
                        del session1
                        counter = 0
                   if counter != 0 and session_df.loc[j, 'action_id'] != '2' and j == len(session_df) - 2:
                      session1 = session1_func()
                      length = j + 1
                      duration = calculate_one(session_df, length)
#                      print("number 16")
                      session1.loc[j, 'title'] = session_df.loc[j+1, 'title']
                      session1.loc[j, 'channel'] = session_df.loc[j+1, 'channel_name']
                      session1.loc[j, 'duration_min'] = duration
                      session1.loc[j, 'session_id'] = session_df.loc[j+1, 'session_id']
                      session = session1.append(session)
                      del session1
                      counter = 0
            elif session_df.loc[j, 'channel_name'] == session_df.loc[j+1, 'channel_name'] or \
               session_df.loc[j, 'title'] == session_df.loc[j+1, 'title']:
                   counter = counter + 1
                   if j == len(session_df) - 2:
                       length = j + 1
                       session_df_action_2 = session_df
                       duration = calculate_four(session_df_action_2, length, counter)
#                       print("number 17")
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
#                                print("number 3")
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
#                               print("number 4")
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
#                                print("number 7")
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
#                               print("number 8")
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
                           if counter == 0:
                               length = j
                               duration = calculate_three(session_df_action_2, length)
#                               print("number 9")
                               xx = j
                               session1_fill_func(session1, session_df_action_2, duration, xx)
                               session = session1.append(session)
                               del session1
                           elif counter != 0 and session_df_action_2.loc[j, 'action_id'] != '2':
                                length = j
                                duration = calculate_four(session_df_action_2, length, counter)
#                                print("number 10")
                                xx = j
                                session1_fill_func(session1, session_df_action_2, duration, xx)
                                session = session1.append(session)
                                del session1
                                counter = 0
                           elif counter != 0 and session_df_action_2.loc[j, 'action_id'] == '2':
                                length = j
                                duration = calculate_six(session_df_action_2, length, counter)
#                                print("number 11")
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
#                               print("number 12")
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
for i in range(0, len(session_list_unique)):   # len(session_list_unique)
    print(lenght_of_data,": ", i)
    session_id_from_list = session_list_unique.loc[i, 'session_id']
    session_df = event_login_duration.query("session_id == @session_id_from_list")
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

program_statistics1 = pd.merge(program_statistics, session_duration, on =['title', 'channel'])

print("--- %s min (finish of the program) ---" % round((time.time() - start)/60, 2))



