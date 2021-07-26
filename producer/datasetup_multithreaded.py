#!/usr/bin/python
import zipfile
import re
import os
import pandas as pd
from kafka import KafkaProducer
from json import dumps
import threading
import shutil

pattern = re.compile('(.*)On_Time_On_Time_Performance_(.*)_(.*).csv')
column_names = [
    'Year', 'Month', 'DayofMonth', 'DayOfWeek', 'FlightDate', 'UniqueCarrier',
    'FlightNum', 'Origin', 'Dest', 'CRSDepTime', 'DepDelay', 'ArrDelay', 'Cancelled'
]
basepath = 'mnt/aviation/airline_ontime'

extracted_dir = 'extracted'
filtered_dir = 'filtered'

shutil.rmtree(os.path.join(extracted_dir), True)
shutil.rmtree(os.path.join(filtered_dir), True)

producer = KafkaProducer(bootstrap_servers='127.0.0.1:9092',
                         value_serializer=lambda K:dumps(K).encode('utf-8'),
                         compression_type='gzip',
                         acks=0,
                         batch_size=65536)

class myThread (threading.Thread):
    def __init__(self, threadID, name, counter):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.counter = counter
    def run(self):
        print("Starting " + self.name)
        mybasepath = os.path.join(basepath,self.name)
        myextracteddir = extracted_dir + "/" + self.name
        for subdir, dirs, files in os.walk(mybasepath):
            for file in files:
                try:
                    filepath = os.path.join(subdir, file)
                    print(self.name + ": Processing: " + filepath)
                    with zipfile.ZipFile(filepath, 'r') as zip_ref:
                        zip_ref.extractall(myextracteddir)
                        original_csv = []
                        for f in os.listdir(myextracteddir):
                            if f.endswith('csv'):
                                original_csv.append(f)
                        for csv in original_csv:
                            print(self.name + ": Process extracted csv: " + os.path.join(myextracteddir, csv))
                            match = re.search(pattern, csv)
                            if match:
                                df = pd.read_csv(os.path.join(myextracteddir, csv),
                                             usecols=column_names)
                                for index in df.index:
                                    producer.send('t1', df.loc[index].to_json())
                            os.remove(os.path.join(myextracteddir, csv))
                except Exception as ex:
                    print(self.name + ": Error processing file: " + file)
                    print(ex)
        print("Exiting " + self.name)
        self.name.exit()

# Create new threads
thread1 = myThread(1988, "1988", 1)
# thread2 = myThread(1989, "1989", 2)
# thread3 = myThread(1990, "1990", 3)
# thread4 = myThread(1991, "1991", 4)
#thread5 = myThread(1992, "1992", 5)
#thread6 = myThread(1993, "1993", 6)
#thread7 = myThread(1994, "1994", 7)
#thread8 = myThread(1995, "1995", 8)
# thread9 = myThread(1996, "1996", 9)
# thread10 = myThread(1997, "1997", 10)
# thread11 = myThread(1998, "1998", 11)
# thread12 = myThread(1999, "1999", 12)
# thread13 = myThread(2000, "2000", 13)
# thread14 = myThread(2001, "2001", 14)
# thread15 = myThread(2001, "2002", 15)
# thread16 = myThread(2003, "2003", 16)
# thread17 = myThread(2004, "2004", 17)
# thread18 = myThread(2005, "2005", 18)
# thread19 = myThread(2006, "2006", 19)
# thread20 = myThread(2007, "2007", 20)
# thread21 = myThread(2008, "2008", 21)
# Start new Threads
thread1.start()
# thread2.start()
# thread3.start()
# thread4.start()
# thread5.start()
# thread6.start()
# thread7.start()
# thread8.start()
# thread9.start()
# thread10.start()
# thread11.start()
# thread12.start()
# thread13.start()
# thread14.start()
# thread15.start()
# thread16.start()
# thread17.start()
# thread18.start()
# thread19.start()
# thread20.start()
# thread21.start()

print("Exiting Main Thread")