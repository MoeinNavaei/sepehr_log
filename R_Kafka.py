
from kafka import KafkaConsumer
from json import loads
from kafka import TopicPartition
import json
import pandas as pd

consumer = KafkaConsumer(

     bootstrap_servers=['10.32.141.21:9092'],
     auto_offset_reset='earliest',
)



topic_partition = TopicPartition('logstash-kafka', 0)
consumer.assign([topic_partition])
# consumer.seek(topic_partition, 8)
# # format: topic, partition
# consumer.assign([topic_partition])
consumer.seek(topic_partition, 0)

#***Get Data***

i = 0
ce = 1
e1 = 0
e3 = 0
u=1
list=[]
# print(i)
for msg in consumer:
    i = i + 1
    print(i)

    a = msg.value  # Give value of msg
    b = a.decode(encoding='UTF-8')  # Convert byte to str
    b = json.loads(b)

    print(b)
    print(type(b))
    list.append(b)
    print(list)

    if i ==10:
        break

df=pd.DataFrame(list)
print(df)
df.to_excel('E:\sourcecode\epg_nginx01/epkaf.xlsx')