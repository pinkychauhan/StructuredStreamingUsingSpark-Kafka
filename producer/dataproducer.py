import zipfile
import re
import os
import pandas as pd
from kafka import KafkaProducer
from json import dumps
import csv as cs
import shutil

extracted_dir = 'extracted'
shutil.rmtree(os.path.join(extracted_dir), True)
shutil.rmtree(os.path.join('filtered'), True)
os.mkdir(extracted_dir)
os.mkdir('filtered')
pattern = re.compile('(.*)On_Time_On_Time_Performance_(.*)_(.*).csv')
column_names = [
    'Year', 'Month', 'DayofMonth', 'DayOfWeek', 'FlightDate', 'UniqueCarrier',
    'FlightNum', 'Origin', 'Dest', 'CRSDepTime', 'DepDelay', 'ArrDelay', 'Cancelled'
]
cwd = os.path.split(os.getcwd())[1]
print("Processing year: " + cwd)
basepath = '/home/ec2-user/mnt/aviation/airline_ontime/' + cwd
producer = KafkaProducer(bootstrap_servers='xxxxxxx:9092,xxxxxx:9092',
                         value_serializer=lambda K:dumps(K).encode('utf-8'),compression_type='lz4',acks=0,batch_size=100000)

for subdir, dirs, files in os.walk(basepath):
    for file in files:
        try:
            filepath = os.path.join(subdir, file)
            extractedsubdir = os.path.join(subdir, extracted_dir)
            print("Processing: " + filepath)
            with zipfile.ZipFile(filepath, 'r') as zip_ref:
                zip_ref.extractall(extracted_dir)
                original_csv = []
                for f in os.listdir(extracted_dir):
                    if f.endswith('csv'):
                        original_csv.append(f)
                for csv in original_csv:
                    print("Process extracted csv: " + os.path.join(extracted_dir, csv))
                    match = re.search(pattern, csv)
                    if match:
                        year = match.group(2)
                        month = match.group(3)
                        csv_df = pd.read_csv(os.path.join(extracted_dir, csv),
                                             usecols=column_names, encoding='ISO-8859-1')
                        converted_csv = os.path.join('filtered', year+"_"+month+".csv")
                        print("Writing csv: " + converted_csv)
                        csv_df.to_csv(converted_csv, index=False)
                        with open(converted_csv, 'r') as conv_file:
                            reader = cs.DictReader(conv_file, delimiter = ',')
                            for messages in reader:
                                producer.send('topic', messages)
                    os.remove(os.path.join(extracted_dir, csv))
        except Exception as ex:
            print("Error processing file: " + file)
            print(ex)
producer.flush()