from __future__ import print_function
import subprocess
import time


from concurrent.futures.thread import ThreadPoolExecutor
from subprocess import check_output
from dsl.fileop import PosixFileSystem, HadoopFileSystem, GalaxyFileSystem

class FileSystem():
    
    @staticmethod
    def FS_by_typename(typename):

        if typename == 'hdfs':
            url = 'hdfs://206.12.102.75:54310/'
            root = '/user'
            user = 'hadoop'
            return HadoopFileSystem(url, root, user)
        elif typename == 'posix':
            url = '/home/mishuk/biowl/storage'
            public = '/public'
            name = 'LocalFS'
            return PosixFileSystem(url, public, name)
        elif typename == 'gfs':
            url = 'http://sr-p2irc-big8.usask.ca:8080'
            password = '7483fa940d53add053903042c39f853a'
            return GalaxyFileSystem(url, password)
        pass
    
    @staticmethod
    def ds_by_prefix(path):
        if not path:
            return None
        path = str(path)
        datasources= '' #datasources = DataSource.query.all()
        for ds in datasources:
            if ds.prefix:
                if path.startswith(ds.prefix):
                    return ds
                
            if ds.url:
                if path.startswith(ds.url):
                    return ds
    
    @staticmethod
    def fs_by_prefix_or_default(path):
        ds = FileSystem.ds_by_prefix(path)
        return FileSystem.create_fs(ds) if ds else FileSystem.FS_by_typename("posix")
    
    
class exechelper():
    
    @staticmethod
    def func_exec_stdout(app, *args):
        cmd = app
        if args:
            cmd += ' ' + ' '.join(args)
        p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        return p.stdout, p.stderr
    
    @staticmethod
    def func_exec_run(app, *args):
        out, err = exechelper.func_exec_stdout(app, *args)
        return out.decode('utf-8'), err.decode('utf-8')



failure = False    
# logger for the current script
logger_name = 'Model_Logger'

# log file for the whole experiment
log_file = 'workflow.log'


class TaskManager():
    def __init__(self, max_count = 5):
        self.pool = ThreadPoolExecutor(max_count)
        self.futures = []
        
    def submit_func(self, func, *args):
        self.cleanup_pool()
        future = self.pool.submit(func, *args)
        self.futures.append(future)
    
    def submit(self, argv):
        self.cleanup_pool()
        future = self.pool.submit(check_output, ' '.join(argv), shell=True)
        self.futures.append(future)
        return future.result()
        
    def cleanup_pool(self):
        self.futures = list(filter(lambda f : f and not f.done(), self.futures))
    
    def wait(self):
        for future in self.futures:
            future.result() # blocks and forward exceptions
            
    def idle(self):
        '''
        True if no task is running
        '''
        for future in self.futures:
            if not future.done():
                return False
        return True

class FolderItem():
    def __init__(self, path):
        self.path = path
    
    def __str__(self):
        return self.path
    
    def __repr__(self):
        return self.__str__()
    
    @staticmethod
    def StrToFolderItem(item_s):
        return [FolderItem(f) for f in item_s] if isinstance(item_s, list) else FolderItem(item_s)
    
    # set operations  
    @staticmethod
    def union(left, right):
        if not isinstance(left, list):
            left = [left]
             
        if not isinstance(right, list):
            right = [right]
         
        left = [str(f) for f in left]
        right = [str(f) for f in right]
        left_s = set(left)
        right_s = set(right)
         
        left = left_s.union(right_s)
        return [FolderItem(f) for f in left]
    
    @staticmethod
    def substract(left, right):
        if not isinstance(left, list):
            left = [left]
             
        if not isinstance(right, list):
            right = [right]
         
        left = [str(f) for f in left]
        right = [str(f) for f in right]
        left_s = set(left)
        right_s = set(right)
         
        left = left_s - right_s
        return [FolderItem(f) for f in left]   
           
class Timer(object):
    def __init__(self, verbose=False):
        self.verbose = verbose

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.secs = self.end - self.start
        self.msecs = self.secs * 1000  # millisecs
        with open("log.txt", "a") as logfile:
            logfile.write("[Elapsed Time]: {0}s\n".format(self.secs))

    