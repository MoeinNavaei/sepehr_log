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
date_name = '2021-05-23'
date_name_start = '12:00:00'
date_name_end = '17:00:00'
 ######################## READ OF EVENT AND SESSION FILES ########################
read_file_event = pd.read_csv (r'C:\Users\PC\Desktop\sepehr_log\event',encoding='utf-8')
read_file_login = pd.read_csv (r'C:\Users\PC\Desktop\sepehr_log\login',encoding='utf-8', error_bad_lines=False)
 ######################## EVENT ########################
event1=pd.DataFrame(read_file_event)
event1.columns=['content_type_id','time_stamp',
       'channel_name', 'action_id', 'content_name',
       '@timestamp', 'time_code',
       'service_id', 'content_id', 'session_id', '@version']

event=pd.DataFrame()

event['content_type_id'] = event1['content_type_id'].str[19:]
event['date'] = event1['time_stamp'].str[12:22]
event['time'] = event1['time_stamp'].str[23:35]

event['channel_name'] = event1['channel_name'].str[13:]
event['action_id'] = event1['action_id'].str[10:]
event['content_name'] = event1['content_name'].str[13:]
event['@timestamp'] = event1['@timestamp'].str[11:]
event['time_code'] = event1['time_code'].str[10:]
event['service_id'] = event1['service_id'].str[11:]
event['content_id'] = event1['content_id'].str[11:]
event['session_id'] = event1['session_id'].str[11:]
event['@version'] = event1['@version'].str[9:]

cols_to_check_event = ['content_type_id','date', 'time',
       'channel_name', 'action_id', 'content_name',
       '@timestamp', 'time_code',
       'service_id', 'content_id', 'session_id', '@version']

event[cols_to_check_event] = event[cols_to_check_event].replace({'"':''}, regex=True)
event['@version'] = event['@version'].replace({'}':''}, regex=True)
#event.to_csv('event.csv')
print("--- %s min (event) ---" % round((time.time() - start)/60, 2))
 ######################## LOGIN ########################
start = time.time()
login1=pd.DataFrame(read_file_login)
login1.columns=['referer','time_stamp',
       'session_end', '@timestamp', 'user_id',
       'xReferer', 'content_id', 'sys_id', 
       'session_id', '@versitimestampon', 'user_agent', 
       'like Gecko) Chrome/90.0.4430.212 Safari/537.36"', 'ip']
login=pd.DataFrame()

login['referer'] = login1['referer'].str[12:]
login['date'] = login1['time_stamp'].str[12:22]
login['time'] = login1['time_stamp'].str[23:35]

login['session_end'] = login1['session_end'].str[12:]
login['@timestamp'] = login1['@timestamp'].str[12:]
login['user_id'] = login1['user_id'].str[9:]
login['xReferer'] = login1['xReferer'].str[10:]
login['content_id'] = login1['content_id']   # .str[12:]
login['sys_id'] = login1['sys_id'].str[8:]
login['session_id'] = login1['session_id'].str[12:]
login['@versitimestampon'] = login1['@versitimestampon'].str[10:]
login['user_agent'] = login1['user_agent'].str[11:]
login['like Gecko) Chrome/90.0.4430.212 Safari/537.36"'] = login1['like Gecko) Chrome/90.0.4430.212 Safari/537.36"'].str[12:]
login['ip'] = login1['ip'].str[4:]

cols_to_check_login = ['referer','date', 'time',
       'session_end', '@timestamp', 'user_id',
       'xReferer', 'content_id', 'sys_id', 
       'session_id', '@versitimestampon', 'user_agent', 
       'like Gecko) Chrome/90.0.4430.212 Safari/537.36"', 'ip']

login[cols_to_check_login] = login[cols_to_check_login].replace({'"':''}, regex=True)
login['@versitimestampon'] = login['@versitimestampon'].replace({'"':''}, regex=True)
login['ip'] = login['ip'].replace({'"}':''}, regex=True)

#login.to_csv('login.csv', index=False)
print("--- %s min (login) ---" % round((time.time() - start)/60, 2))
 ######################## CHOOSE OF DAY AND HOUR ########################
start = time.time()
event_day = event.query("date == @date_name")
login_day = login.query("date == @date_name")
event_day_hour = event_day.query("time >= @date_name_start")
event_day_hour = event_day_hour.query("time <= @date_name_end")
login_day_hour = login_day.query("time >= @date_name_start")
login_day_hour = login_day_hour.query("time <= @date_name_end")
print("--- %s min (CHOOSE OF DAY AND HOUR) ---" % round((time.time() - start)/60, 2))
 ######################## EPG OF KOUHZADI ########################
start = time.time()
url = 'https://epgservices.irib.ir:84/Service_EPG.svc/GetEpgNetwork'
epg1=pd.DataFrame(columns=['ID_Day_Item', 'Name_Item', 'Time_Play', 'EP', 'DTDay', 'Length',
       'Dec_Full', 'Dec_Summary', 'ID_Kind', 'channel'])
chan_table=pd.read_excel(r'C:\Users\PC\Desktop\EpgApi\chan_table.xlsx', index_col=False)
chan_table['code']=chan_table['code'].astype(str)

for i in range(31,150):
    ax="\"SID_Network\":{}".format(i)
    az=",\"DTStart\":\"05/23/2021\",\"DTEnd\":\"05/23/2021\""  # DETERMINATION OF DATE
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

date2 = pd.to_datetime(epg1.Time_Play, errors='coerce')
epg1 = epg1.assign(s_date=date2.dt.date, s_time=date2.dt.time)
date3 = pd.to_datetime(epg1.EP, errors='coerce')
epg1=epg1.assign(e_dete=date3.dt.date, e_time=date3.dt.time)
epg2=epg1[(epg1['s_date'] == datetime.date(2021,6,20))]
epg1 = epg1.rename(columns={"s_date":"s_date_EPG"})
epg1 = epg1.rename(columns={"e_date":"e_date_EPG"})
epg1 = epg1.rename(columns={"s_time":"s_time_EPG"})
epg1 = epg1.rename(columns={"e_time":"e_time_EPG"})
print("--- %s min (EPG OF KOUHZADI) ---" % round((time.time() - start)/60, 2))
######################## CONVERT CODE TO CHANNEL IN EPG OF KOUHZADI ########################
start = time.time()
epg1.insert(16, 'channel_name', '')
epg1 = epg1.reset_index()
del epg1['index']

event_day_hour1 = pd.to_datetime(event_day_hour.time, errors='coerce')
event_day_hour = event_day_hour.assign(s_time=event_day_hour1.dt.time)
event_day_hour2 = pd.to_datetime(event_day_hour.date, errors='coerce')
event_day_hour = event_day_hour.assign(s_date=event_day_hour2.dt.date)

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
print("--- %s min (CONVERT CODE TO CHANNEL IN EPG OF KOUHZADI) ---" % round((time.time() - start)/60, 2))
 ######################## DETERMINATION OF TITLE FROM EPG KOUHZADI TO EVENT ########################
start = time.time()
event_day_hour = event_day_hour.reset_index()
del event_day_hour['index']
event_day_hour = pd.merge(event_day_hour, epg1, on = ['channel_name'])
event_day_hour = event_day_hour[event_day_hour['s_time'] >= event_day_hour['s_time_EPG']]
event_day_hour = event_day_hour[event_day_hour['s_time'] <= event_day_hour['e_time_EPG']]
event_day_hour=event_day_hour.rename(columns={"Name_Item":"title"})
print("--- %s min (DETERMINATION OF TITLE FROM EPG KOUHZADI TO EVENT) ---" % round((time.time() - start)/60, 2))
 ######################## TRANSMITION OF USER_ID FROM LOGIN TO EVENT ########################
start = time.time()
login_day_hour = login_day_hour.reset_index()
del login_day_hour['index']
event_day_hour_new = pd.merge(event_day_hour, login_day_hour, on=['session_id'])
del event_day_hour_new['date_y']
del event_day_hour_new['time_y']
del event_day_hour_new['@timestamp_y']
del event_day_hour_new['content_id_y']
del event_day_hour_new['referer']
del event_day_hour_new['session_end']
del event_day_hour_new['xReferer']
del event_day_hour_new['sys_id']
del event_day_hour_new['@versitimestampon']
del event_day_hour_new['user_agent']
del event_day_hour_new['like Gecko) Chrome/90.0.4430.212 Safari/537.36"']
del event_day_hour_new['time_code']
del event_day_hour_new['content_id_x']
del event_day_hour_new['@version']
del event_day_hour_new['ip']

event_day_hour_new=event_day_hour_new.rename(columns={"@timestamp_x":"@timestamp"})
event_day_hour_new=event_day_hour_new.rename(columns={"date_x":"date"})
event_day_hour_new=event_day_hour_new.rename(columns={"time_x":"time"})
del event_day_hour
event_day_hour = event_day_hour_new
print("--- %s min (TRANSMITION OF USER_ID FROM LOGIN TO EVENT) ---" % round((time.time() - start)/60, 2))
 ######################## VISIT ########################
start = time.time()
event_day_hour.insert(28, 'counter', 1)
event_day_hour['session_id'].replace('', 'NO', inplace=True)
event_day_hour_visit = event_day_hour.copy()
event_day_hour_visit = event_day_hour_visit[~event_day_hour_visit.session_id.str.contains('NO')]
event_day_hour_visit.drop_duplicates(subset =['channel_name', 'session_id', 'title'], keep = 'first', inplace = True)
event_day_hour_visit = event_day_hour_visit.groupby(['title', 'channel_name']).sum().reset_index()
event_day_hour_visit['title'].replace('', 'NO', inplace=True)
event_day_hour_visit = event_day_hour_visit[~event_day_hour_visit.title.str.contains('NO')]
event_day_hour_visit=event_day_hour_visit.rename(columns={"counter":"visit"})
del event_day_hour_visit['ID_Program']
del event_day_hour_visit['channel2']
print("--- %s min (VISIT) ---" % round((time.time() - start)/60, 2))
######################## ACTIVE_USER ########################
start = time.time()
#event_day_hour = event_day_hour.fillna(0)
event_day_hour['user_id'].replace('', 'NO', inplace=True)
event_day_hour_user = event_day_hour.copy()
event_day_hour_user = event_day_hour_user[~event_day_hour_user.user_id.str.contains('NO')] 
event_day_hour_user.drop_duplicates(subset =['channel_name', 'user_id', 'title'], keep = 'first', inplace = True)
event_day_hour_user=event_day_hour_user.groupby(['title', 'channel_name']).sum().reset_index()
event_day_hour_user['title'].replace('', 'NO', inplace=True)
event_day_hour_user = event_day_hour_user[~event_day_hour_visit.title.str.contains('NO')]
event_day_hour_user=event_day_hour_user.rename(columns={"counter":"active_user"})
del event_day_hour_user['ID_Program']
del event_day_hour_user['channel2']
print("--- %s min (ACTIVE_USER) ---" % round((time.time() - start)/60, 2))
######################## NEW DATAFRAME ########################
start = time.time()
program_statistics = pd.DataFrame()
######################## TRANSMISSION OF DATA TO NEW DATAFRAME ########################
program_statistics['title'] = event_day_hour_user['title']
program_statistics['channel'] = event_day_hour_user['channel_name']

event_day_hour_visit = event_day_hour_visit.reset_index()
del event_day_hour_visit['index']
event_day_hour_user = event_day_hour_user.reset_index()
del event_day_hour_user['index']
program_statistics = program_statistics.reset_index()
del program_statistics['index']

event_day_hour_visit=event_day_hour_visit.rename(columns={"channel_name":"channel"})
event_day_hour_user=event_day_hour_user.rename(columns={"channel_name":"channel"})

program_statistics = pd.merge(program_statistics, event_day_hour_visit, on = ['title', 'channel'])
program_statistics = pd.merge(program_statistics, event_day_hour_user, on = ['title', 'channel'])

event_day_hour['action_id']=event_day_hour['action_id'].astype(int)
print("--- %s min (NEW DATAFRAME) ---" % round((time.time() - start)/60, 2))
######################## DURATION ########################
start = time.time()
event_day_hour_duration = event_day_hour.copy()
event_day_hour_duration.sort_values('s_time_EPG', axis = 0, ascending = True, inplace = True, na_position ='last')
event_day_hour_duration_title_pivot = event_day_hour_duration.copy()
event_day_hour_duration_title_pivot = event_day_hour_duration_title_pivot.groupby(['title']).sum().reset_index()
event_day_hour_duration = event_day_hour_duration[~event_day_hour_duration.session_id.str.contains('NO')]
event_day_hour_duration = event_day_hour_duration.reset_index()
del event_day_hour_duration['index']

def calculate_without_2(event_day_hour_duration_title_session, counter_action_2, j_counter):
    time_one_action_without_2 = 0
    event_day_hour_duration_title_session_details = event_day_hour_duration_title_session.iloc[counter_action_2:j_counter+1 , 0:4]
    event_day_hour_duration_title_session_details.sort_values('s_time', axis = 0, ascending = True, inplace = True, na_position ='last')
    event_day_hour_duration_title_session_details = event_day_hour_duration_title_session_details.reset_index()
    del event_day_hour_duration_title_session_details['index']
    length_details = len(event_day_hour_duration_title_session_details)
    hour_s_time = event_day_hour_duration_title_session_details.loc[0, 's_time'].hour
    minute_s_time = event_day_hour_duration_title_session_details.loc[0, 's_time'].minute
    second_s_time = event_day_hour_duration_title_session_details.loc[0, 's_time'].second
    hour_e_time_EPG = event_day_hour_duration_title_session_details.loc[length_details-1, 'e_time_EPG'].hour
    minute_e_time_EPG = event_day_hour_duration_title_session_details.loc[length_details-1, 'e_time_EPG'].minute
    second_e_time_EPG = event_day_hour_duration_title_session_details.loc[length_details-1, 'e_time_EPG'].second
    time_one_action_without_2 = (hour_e_time_EPG - hour_s_time) * 3600 + (minute_e_time_EPG - minute_s_time) * 60 + (second_e_time_EPG - second_s_time)
    if time_one_action_without_2 > 1800:
        time_one_action_without_2 = 1800
    return time_one_action_without_2    
event_day.to_excel('event_day.xlsx', index=False)
def calculate_with_2(event_day_hour_duration_title_session, counter_action_2, j_counte):
    time_one_action_with_2 = 0
    event_day_hour_duration_title_session_details = event_day_hour_duration_title_session.iloc[counter_action_2:j_counter+1 , 0:4]
    event_day_hour_duration_title_session_details.sort_values('s_time', axis = 0, ascending = True, inplace = True, na_position ='last')
    event_day_hour_duration_title_session_details = event_day_hour_duration_title_session_details.reset_index()
    del event_day_hour_duration_title_session_details['index']
    length_details = len(event_day_hour_duration_title_session_details)
    hour_s_time = event_day_hour_duration_title_session_details.loc[0, 's_time'].hour
    minute_s_time = event_day_hour_duration_title_session_details.loc[0, 's_time'].minute
    second_s_time = event_day_hour_duration_title_session_details.loc[0, 's_time'].second
    hour_s_time_EPG_last = event_day_hour_duration_title_session_details.loc[length_details-1, 's_time'].hour
    minute_s_time_EPG_last = event_day_hour_duration_title_session_details.loc[length_details-1, 's_time'].minute
    second_s_time_EPG_last = event_day_hour_duration_title_session_details.loc[length_details-1, 's_time'].second
    time_one_action_with_2 = (hour_s_time_EPG_last - hour_s_time) * 3600 + (minute_s_time_EPG_last - minute_s_time) * 60 + (second_s_time_EPG_last - second_s_time)
    if time_one_action_with_2 > 1800:
        time_one_action_with_2 = 1800
    return time_one_action_with_2  

for i_program_statistics in range(0, len(event_day_hour_duration_title_pivot)):  # len(event_day_hour_duration_title_pivot)
    time_title = 0
######## start choose a title ########        
    title_name = program_statistics.loc[i_program_statistics, 'title']
    event_day_hour_duration_title = pd.DataFrame()
    event_day_hour_duration_title.insert(0, 'action_id', '')
    event_day_hour_duration_title.insert(1, 'session_id', '')
    event_day_hour_duration_title.insert(2, 's_time', '')
    event_day_hour_duration_title.insert(3, 's_time_EPG', '')
    event_day_hour_duration_title.insert(4, 'e_time_EPG', '')
    event_day_hour_duration_title.insert(5, 'channel', '')
    k_title_counter = 0
    for i_duration in range(0, len(event_day_hour_duration)):
        if event_day_hour_duration.loc[i_duration, 'title'] == title_name:
            event_day_hour_duration_title.loc[k_title_counter, 'action_id'] = event_day_hour_duration.loc[i_duration, 'action_id']
            event_day_hour_duration_title.loc[k_title_counter, 'session_id'] = event_day_hour_duration.loc[i_duration, 'session_id']
            event_day_hour_duration_title.loc[k_title_counter, 's_time'] = event_day_hour_duration.loc[i_duration, 's_time']
            event_day_hour_duration_title.loc[k_title_counter, 's_time_EPG'] = event_day_hour_duration.loc[i_duration, 's_time_EPG']
            event_day_hour_duration_title.loc[k_title_counter, 'e_time_EPG'] = event_day_hour_duration.loc[i_duration, 'e_time_EPG']
            event_day_hour_duration_title.loc[k_title_counter, 'channel'] = event_day_hour_duration.loc[i_duration, 'channel_name']
            k_title_counter = k_title_counter + 1
            ######## finish choose a title ########
    ######## start choose a session ########
    channel_name = program_statistics.loc[i_program_statistics ,'channel']
    event_day_hour_duration_title = event_day_hour_duration_title.query("channel == @channel_name")
    event_day_hour_duration_title = event_day_hour_duration_title.reset_index()
    del event_day_hour_duration_title['index']
    event_day_hour_duration_title_session_pivot = event_day_hour_duration_title.copy()
    event_day_hour_duration_title_session_pivot = event_day_hour_duration_title_session_pivot.groupby(['session_id']).sum().reset_index()
    time_one_session = 0
    for i_session_pivot in range(0, len(event_day_hour_duration_title_session_pivot)):
        event_day_hour_duration_title_session = pd.DataFrame()
        event_day_hour_duration_title_session.insert(0, 'action_id', '')
        event_day_hour_duration_title_session.insert(1, 's_time', '')
        event_day_hour_duration_title_session.insert(2, 's_time_EPG', '')
        event_day_hour_duration_title_session.insert(3, 'e_time_EPG', '')
        k_session_counter = -1
        session_name = event_day_hour_duration_title_session_pivot.loc[i_session_pivot, 'session_id']
        for i_session in range(0, len(event_day_hour_duration_title)):
            if event_day_hour_duration_title.loc[i_session, 'session_id'] == session_name:
                k_session_counter = k_session_counter + 1
                event_day_hour_duration_title_session.loc[k_session_counter, 'action_id'] = event_day_hour_duration_title.loc[i_session, 'action_id']
                event_day_hour_duration_title_session.loc[k_session_counter, 's_time'] = event_day_hour_duration_title.loc[i_session, 's_time']
                event_day_hour_duration_title_session.loc[k_session_counter, 's_time_EPG'] = event_day_hour_duration_title.loc[i_session, 's_time_EPG']
                event_day_hour_duration_title_session.loc[k_session_counter, 'e_time_EPG'] = event_day_hour_duration_title.loc[i_session, 'e_time_EPG']
                ######## finish choose a session ########
        ####### start choose a deataframe from 2 action_id ########
        event_day_hour_duration_title_session.sort_values('s_time', axis = 0, ascending = True, inplace = True, na_position ='last')
        counter_action_2 = 0
        event_day_hour_duration_title_session = event_day_hour_duration_title_session.reset_index()
        del event_day_hour_duration_title_session['index']
        time_total_action = 0
        for counter in range(0, len(event_day_hour_duration_title_session) + 1):
            time_one_action = 0
            time_one_action_without_2 = 0
            time_one_action_with_2 = 0
            if len(event_day_hour_duration_title_session) == 1 and \
                event_day_hour_duration_title_session.loc[0, 'action_id'] == 2:
                    time_title = 0
                    break
            elif len(event_day_hour_duration_title_session) == 1 and \
                 event_day_hour_duration_title_session.loc[0, 'action_id'] != 2:
                     j_counter = 0
                     time_one_action_without_2 = calculate_without_2(event_day_hour_duration_title_session, counter_action_2, j_counter)
                     time_one_action = time_one_action + time_one_action_with_2 + time_one_action_without_2
                     time_total_action = time_total_action + time_one_action
                     break
            elif counter_action_2 + 1 == len(event_day_hour_duration_title_session):
                j_counter = counter_action_2
                time_one_action_without_2 = calculate_without_2(event_day_hour_duration_title_session, counter_action_2, j_counter)
                time_one_action = time_one_action + time_one_action_with_2 + time_one_action_without_2
                time_total_action = time_total_action + time_one_action
                break
            else:
                if event_day_hour_duration_title_session.loc[counter_action_2, 'action_id'] != 2 and \
                   counter != len(event_day_hour_duration_title_session):
                    for j_counter in range(counter_action_2 + 1, len(event_day_hour_duration_title_session)):
                        if event_day_hour_duration_title_session.loc[j_counter, 'action_id'] == 2:
                            time_one_action_with_2 = calculate_with_2(event_day_hour_duration_title_session, counter_action_2, j_counter)
                            time_one_action = time_one_action + time_one_action_with_2 + time_one_action_without_2
                            time_total_action = time_total_action + time_one_action
                            counter_action_2 = j_counter + 1
                            break
                elif event_day_hour_duration_title_session.loc[counter_action_2, 'action_id'] != 2 and \
                    counter == len(event_day_hour_duration_title_session):
                    time_one_action_without_2 = calculate_without_2(event_day_hour_duration_title_session, counter_action_2, j_counter)
                    time_one_action = time_one_action + time_one_action_with_2 + time_one_action_without_2
                    time_total_action = time_total_action + time_one_action
                    break
                elif event_day_hour_duration_title_session.loc[counter_action_2, 'action_id'] == 2:
                    counter_action_2 = counter_action_2 + 1
            if counter_action_2 >= len(event_day_hour_duration_title_session):
                break
            ######## finish choose a deataframe from 2 action_id ########                
        time_one_session = time_one_session + time_total_action
        del event_day_hour_duration_title_session
    time_title = time_title + time_one_session
    time_title = round(time_title/60, 2)
    program_statistics.loc[i_program_statistics, 'duration (min)'] = time_title

program_statistics.to_excel('program_statistics.xlsx')
print("--- %s min (duration) ---" % round((time.time() - start)/60, 2))












