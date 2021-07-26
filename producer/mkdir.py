import shutil, os

for i in range(1988, 2009) :
    os.mkdir(str(i))
    shutil.copy('dataproducer.py', str(i))
