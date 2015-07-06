__author__ = 'lucas'

import os
import pandas as pd
import numpy as np
from pymongo import MongoClient
client = MongoClient()
import time


def load_class_to_databse(path):
    files = [os.path.join(path, name) for name in os.listdir(path) if os.path.isfile(os.path.join(path, name))]
    for file in files:
        file_name, class_name, data = read_lightcurve_file(path)

def read_lightcurve_file(path):
    class_name = os.path.basename(os.path.dirname(path))
    file_name = os.path.basename(path)
    data_pandas = pd.read_csv('/home/lucas/Desktop/MACHO/Be_lc/lc_1.3567.1310.B.mjd',delim_whitespace=True,header=2)
    data = data_pandas.values
    return file_name, class_name, data



def main():
    root = '/home/lucas/Desktop/MACHO'
    subdirectories = [os.path.join(root, name) for name in os.listdir(root) if os.path.isdir(os.path.join(root, name))]
    print('reading from files')
    start = time.time()
    for directory in subdirectories:
        load_class_to_databse(directory)
    end = time.time()
    print('Elapsed time {0} \n'.format(end - start))
    subdirectories = [os.path.join(root, name) for name in os.listdir(root) if os.path.isdir(os.path.join(root, name))]
    print('reading from database')
    start = time.time()
    db = client.lightcurves
    collection = db.macho
    cursor = collection.find()
    while cursor.alive:
        document = cursor.next()
        mjd = document['mjd']
        magnitude = document['magnitude']
        error = document['error']
        curve = np.column_stack((mjd, magnitude, error))
    end = time.time()
    print('Elapsed time {0} \n'.format(end - start))


if __name__ == '__main__':
    main()
