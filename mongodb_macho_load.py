__author__ = 'lucas'

import os
import pandas as pd
from pymongo import MongoClient
client = MongoClient()


def load_class_to_databse(path):
    files = [os.path.join(path, name) for name in os.listdir(path) if os.path.isfile(os.path.join(path, name))]
    for file in files:
        file_name, class_name, data = read_lightcurve_file(path)
        write_lightcurve_to_db(file_name, class_name, data)

def read_lightcurve_file(path):
    class_name = os.path.basename(os.path.dirname(path))
    file_name = os.path.basename(path)
    data_pandas = pd.read_csv('/home/lucas/Desktop/MACHO/Be_lc/lc_1.3567.1310.B.mjd',delim_whitespace=True,header=2)
    data = data_pandas.values
    return file_name, class_name, data



def write_lightcurve_to_db(file_name, class_name, data):
     db = client.lightcurves
     collection = db.macho
     mjd = data[:,0].tolist()
     magnitude = data[:,1].tolist()
     error = data[:,2].tolist()
     data = {"file_name": file_name, "class_name": class_name, "mjd": mjd, "magnitude": magnitude, "error": error}
     lightcurve = collection.insert_one(data)


def main():
    root = '/home/lucas/Desktop/MACHO'
    subdirectories = [os.path.join(root, name) for name in os.listdir(root) if os.path.isdir(os.path.join(root, name))]
    for directory in subdirectories:
        load_class_to_databse(directory)






if __name__ == '__main__':
    main()
