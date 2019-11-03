# -*- coding: utf-8 -*-
"""
Created on Sun Mar 31 16:23:50 2019

@author: Luke
"""

    

class Ipython():
    @staticmethod
    def run_from_ipython():
        try:
            __IPYTHON__
            return True
        except NameError:
            return False


class timing():
    import threading
    from datetime import datetime, timedelta

    local = threading.local()
    class ExecutionTimeout(Exception): pass

    def start(max_duration = timedelta(seconds=1)):
        local.start_time = datetime.now()
        local.max_duration = max_duration

    def check():
        if datetime.now() - local.start_time > local.max_duration:
            raise ExecutionTimeout()

    def do_work():
        start()
        while True:
            check()
            # do stuff here
        return 10


def df_islarge(df):
    if df.memory_usage().sum()>100*10^6: return True
    else: return False

    
    
def df_describe(df, col_details = True, columns = None):
    """
    returns dataframe statistics
    col_details : column analysis
    
    """
    try: #for pandas series compatability
        print('Number of rows: {:23} \nNumber of columns: {:20} \nDataframe size: {:20} mb'
              .format(len(df), len(df.columns), df.memory_usage().sum()/1000000))
    except:
         print('Number of rows: {:23} \nDataframe size: {:20} mb'
              .format(len(df), df.memory_usage().sum()/1000000))       

    if df_islarge(df):
        print('Large dataset warning')

    print('head: ')
    print(df.head())
    
    if col_details == True:
        if columns == None:
            print('columns: ', df.columns.values)
            print(df.describe().T)
            print(df.isnull().sum())
        
        else:
            for col in columns:
                print('Column: {:20} \nType: {:20} \nMemory usage: {:20}'
                      .format(col, str(df[col].dtype), df[col].memory_usage()/1000000))
                #print(df[col].describe())
                print('Number of nulls: ', df[col].isnull().sum())


def get_path():
    dir_path = os.path.dirname(os.path.realpath(__file__))
    print(dir_path)
    return dir_path


#######################################
#count_class_function_calls decorator


from functools import wraps



def callable(o):
    return hasattr(o, "__call__")



call_count = {}

def count_calls(fn):   

    print("decorating" )    

    def new_function(*args,**kwargs):

        print("starting timer" )      
        import datetime                 
        before = datetime.datetime.now()                     
        print(fn.__name__)
        if call_count.get(fn.__name__) is None:
            call_count[fn.__name__] = 1 
        else:
            call_count[fn.__name__] = call_count.get(fn.__name__) + 1
        print(call_count)

        x = fn(*args,**kwargs)

        after = datetime.datetime.now()                      
        print("Elapsed Time: {0}".format(after-before)   )   

        return x

    return new_function

def count_class_function_calls(cls):
    """
    counts and times each occasion a function is run in a class
    """

    class NewCls(object):

        def __init__(self,*args,**kwargs):
            self.oInstance = cls(*args,**kwargs)

        def __getattribute__(self,s):
            """
            called whenever any attribute of a NewCls object is accessed. 
            This function first tries to get the attribute off NewCls. If it fails then it tries to fetch the attribute from self.oInstance (an
            instance of the decorated class). If it manages to fetch the attribute from self.oInstance, and 
            the attribute is an instance method then `count_calls` is applied.
            """

            try:    
                x = super(NewCls,self).__getattribute__(s)
            except AttributeError:      
                pass
            else:
                return x

            x = self.oInstance.__getattribute__(s)

            if type(x) == type(self.__init__): # it is an instance method
                return count_calls(x)
            else:
                return x
            

    return NewCls




if __name__ == '__main__':

    @count_class_function_calls
    class test_class():
        def __init__(self, a):
            self.x = 5
            self.a = a

        #@count_calls
        def fn(self):
            print("ran fn")
            return 2

        def b(self): return self.a

    my_class = test_class(5)
    print(my_class.fn())
    print(my_class.a)
    print(my_class.b())

    print(test_class.__name__)


