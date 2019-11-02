"""
consists of class df_store which allows the user to 
easily store and load their panda dataframes. current storage formats include: json, csv, parquet, pickle, feather and HDF5
--df_store('test.json').load_df()
--df_store('test.json').store_df(df)
-- TODO incorporate: BSON
"""
from datetime import datetime
import pandas  as pd
from calendar import monthrange
import sys, os
import pandas as pd
import json
import pickle

try:
    from fanalysis.utils import Ipython
    import fanalysis.utils as u
except ImportError:
    from utils import Ipython
    import utils as u



    
def try_import(module_names):

    import importlib

    failed_imports = []

    def import_module_fn(mod_in):
        try:
            #import mod_in
            mod_in = importlib.import_module(mod_in)
        except ImportError:
            failed_imports.append(mod_in)

    if isinstance(module_names, list):
        for mod in module_names:
            import_module_fn(mod)
    else:
        import_module_fn(module_names)

    if failed_imports ==[]:
        pass
    else:
        print('Unable to import ' + ', '.join(failed_imports))


#try_import(['feather', 'pyarrow.parquet'])

#import feather
#import pyarrow.parquet

storagetypes = ["pickle", "json", "csv", "parquet", "feather", "h5"]
storagetypes_dict = {"pickle": "", "json": "json", "csv": "csv", "parquet": "parquet", "feather": "feather", "HDF5": "h5" }



if Ipython.run_from_ipython()==True:
    print('Ipython active')
    standardpath='fanalysis\data'
else:
    standardpath = 'data'

class df_store:
    """
    loads/stores specified files to and from pickle, json, csv or parquet formats 
    e.g.1 df_store('test.json').load_df()
    e.g.2 df_store('test.json').store_df(df)
    *args = path parts. if no path entered the default fanalysis\\data is used
    parquet format: "test.parquet.gzip" 
    to store and load in current directory simply: df_store('test.csv', 'fanalysis').load_df()
    """

    def __init__(self, filename, *args):
        if args:
            self.filename = os.path.join(*args, filename)
        else:
            self.filename = os.path.join(standardpath, filename)
        try:
            self.filetype = self.filename.split(".",-1)[1]
        except:
            self.filetype = "pickle"


    def load_df(self):     
        filename = self.filename
        filetype = self.filetype   

        if filetype in storagetypes:
            fn = "self." + filetype + "_load(filename)"
            print("Loading "+ filetype + ": " + filename + "...")

            assert self.check_file_exists(filename) ==True, "Filename does not exist. please try again"

            dataframe = eval(fn)

            if dataframe is not None:
                print("dataframe loaded successfully")

            #except:
            #    raise OSError("could not load "+filetype+". Check file exists - file may be too large.")
            return dataframe
        else:
            raise ValueError("Cannot load that file type. Please check file name and try again.")     

    
    def store_df(self, dataframe):
        """
        when replace_existing set to true any existing files with the same name will be automatically replaced
        """ 
        filename = self.filename
        filetype = self.filetype
        
        if filetype in storagetypes:
            fn = "self.dfto" + filetype + "(dataframe, filename)"
            print("Storing "+ filetype + ": " + filename + "...")

            assert self.check_file_exists(filename) == False, "Filename already exists. please try again"

            try:
                create_path(os.path.dirname(filename))
                exec(fn)
            except:
                exec(fn)
                    
            print("dataframe stored successfully")
            return filename
        
        else:
            raise ValueError("Cannot store that file type. Please check file name and try again.")
    

    def append_df(self, dataframe, replace_existing = True):
        """
        when replace_existing set to true any existing files with the same name will be automatically replaced
        """ 

        
        filename = self.filename
        filetype = self.filetype
        
        if filetype in storagetypes:

            assert self.check_file_exists(filename) ==True, "Filename does not exist. please try again"

            fn = "self." + filetype + "_dfappend(dataframe, filename)"
            print("Appending "+ filetype + ": " + filename + "...")

            
            try:
                exec(fn)
            except:
                exec(fn)
                    
            print("dataframe updated successfully")
            return filename
        
        else:
            raise ValueError("Cannot store that file type. Please check file name and try again.")
       
 
    def check_file_exists(self, filename): 
        return os.path.exists(filename)



    ###########################################################
    #LOAD FUNCTIONS
    ###########################################################

    def pickle_load(self, pfile):
        with open(pfile,'rb') as pObject:
            df = pickle.load(pObject)
        df = pd.DataFrame(df)
        return df

    def json_load(self, jfile):    
        df = pd.read_json(jfile, orient='records') #, convert_dates=['date'])
        return df

    def csv_load(self, csvfile):    
        df = pd.read_csv(csvfile) 
        return df
    
    def h5_load(self, hdffile):
        try:
            df = pd.read_hdf(hdffile, 'df')
        except:
            import h5py
            df = h5py.File(hdffile, 'r')
            print("file loaded as {}".format(type(df)))
        return df


    ###########################################################
    #STORAGE FUNCTIONS
    ###########################################################

    
    
    def dftopickle(self, dataframe, filename):
        with open(filename,'wb') as fObject:
            pickle.dump(dataframe,fObject, pickle.HIGHEST_PROTOCOL)   

    def dftojson(self, dataframe, filename):
        with open(filename, 'wb') as f:
            dataframe.to_json(filename, orient='records')

    def dftocsv(self, dataframe, filename):
        #sep='\t'
        dataframe.to_csv(filename, sep=',')
    
    def dftoh5(self, dataframe, filename):
        #e.g. data.h5
        dataframe.to_hdf(filename, key= 'df', mode='w', format='t',  data_columns=True)


    ###########################################################
    #APPEND FUNCTIONS
    ###########################################################

    
    
    def pickle_dfappend(self, dataframe, filename):
        with open(filename,'a+') as fObject:
            pickle.dump(dataframe,fObject, pickle.HIGHEST_PROTOCOL)   

    def json_dfappend(self, dataframe, filename):
        raise TypeError("Cannot append to json")

    def csv_dfappend(self, dataframe, filename):
        #sep='\t'
        with open(filename, 'a') as f:
            dataframe.to_csv(f, header=False, sep=',')
    
    def h5_dfappend(self, dataframe, filename):

        #store = pd.HDFStore(filename)
        #store.append('dataframe', dataframe, format='t',  data_columns=True)
        #store.close()
        #TODO update key
        dataframe.to_hdf(filename, key= 'df1', append = True, mode='w', format='t',  data_columns=True)





def mkdir_p(path):
    import errno
    try:
        path = path
        os.makedirs(path)
        return path
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            return path
        else:
            raise


def create_path(path):
    if not os.path.exists(path):
        a = True
        while a == True:
            user_input = input(path + ' does not exist. Create folder? (y/n): ')
            if user_input == 'y' or  user_input == 'y':
                if user_input == 'y':
                    path = mkdir_p(path)
                elif user_input == 'n':
                    pass
                a = False
    else:
        pass
    return path



def str_listconcat (input, str):
    l = len(input)
    list = []
    for i in range(l):
        list.append(str)
        list[i] = list[i] + input[i]
    input = list
    return input


def get_path():
    dir_path = os.path.dirname(os.path.realpath(__file__))
    print(dir_path)
    return dir_path





if __name__=='__main__':
    pass
