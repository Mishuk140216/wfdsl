from __future__ import print_function

import subprocess
import uuid
import time
import logging
import getpass
import couchdb

from concurrent.futures.thread import ThreadPoolExecutor
from subprocess import check_output
from fileop import PosixFileSystem, HadoopFileSystem, GalaxyFileSystem

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

formatter_info = logging.Formatter('time: %(asctime)s, msecs: %(msecs)d, log: %(message)s' + '\n---------')    
formatter_debug = logging.Formatter('time: %(asctime)s, file: %(filename)s, func: %(funcName)s, level: %(levelname)s, line: %(lineno)d, module: %(module)s, msecs: %(msecs)d, output: %(message)s, logger: %(name)s, path: %(pathname)s, procID: %(process)d, proc: %(processName)s, threadID: %(thread)d, thread: %(threadName)s' + '\n*********')
formatter_error = logging.Formatter('time: %(asctime)s, file: %(filename)s, func: %(funcName)s, level: %(levelname)s, line: %(lineno)d, module: %(module)s, msecs: %(msecs)d, except: %(message)s, logger: %(name)s, path: %(pathname)s, procID: %(process)d, proc: %(processName)s, threadID: %(thread)d, thread: %(threadName)s' + '\nXXXXXXXXX')


class configuration():
    
    @staticmethod
    def logger_info(name, log_file, level=logging.INFO):
        handler = logging.FileHandler(log_file)
        handler.setFormatter(formatter_info)
    
        logger = logging.getLogger(name + '-INFO')
        logger.setLevel(level)
        logger.addHandler(handler)
    
        return logger
    
    @staticmethod
    def logger_debug(name, log_file, level=logging.DEBUG):
        handler = logging.FileHandler(log_file)
        handler.setFormatter(formatter_debug)
    
        logger = logging.getLogger(name + '-DEBUG')
        logger.setLevel(level)
        logger.addHandler(handler)
    
        return logger
    
    @staticmethod
    def logger_error(name, log_file, level=logging.ERROR):
        # type: (object, object, object) -> object
        handler = logging.FileHandler(log_file)
        handler.setFormatter(formatter_error)
    
        logger = logging.getLogger(name + '-ERROR')
        logger.setLevel(level)
        logger.addHandler(handler)
    
        return logger

failure = False    
# logger for the current script
logger_name = 'Model_Logger'

# log file for the whole experiment
log_file = 'workflow.log'
    
info = configuration.logger_info(logger_name, log_file)
deb  = configuration.logger_debug(logger_name, log_file)
err = configuration.logger_error(logger_name, log_file)

class Data:

    def __init__(self):
        self.id = None
        self.ref = None
        self.user = None
        
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

   
        
        
        
        
    
class Object(Data):

    def __init__(self, reference):
        # type: (Any) -> None

        self.id = uuid.uuid4()
        self.user = User()

        if failure is True:
            # preoondition management
            pass

        else:
            # if all preconditions passed
            try:
                self.ref = reference

                deb.debug('Object Data Created. (flowID, type, value, user) =  (' + str(self.id) + ', ' + str(
                    type(reference)) + ', ' + str(reference) + ', ' + str(self.user.id) + ')')
                info.info('object data created >>> (objID, obj) = (' + str(self.id) + ', ' + str(self.ref) + ')')

            except Exception as e:
                # if any further error occurs somehow
                err.error(str(e) + '. (flowID) = ' + '(' + str(self.id) + ')')

class File(Data):

    def __init__(self, f):
        # type: (file) -> None

        self.id = uuid.uuid4()
        self.user = User()

        if failure is True:
            # precondition management
            pass

        elif not isinstance(f):
            # if file not found
            err.error(str(IOError) + '. (flowID) = ' + '(' + str(self.id) + ')')

        else:
            # if all exceptions passed
            try:
                self.ref = f

                deb.debug('File Data Created. (flowID, type, value, user) =  (' + str(self.id) + ',' + str(
                    type(f)) + ', ' + str(f) + ', ' + str(self.user.id) + ')')
                info.info('file data created >>> (fileID, name) = (' + str(self.id) + ', ' + str(self.ref) + ')')

            except Exception as e:
                # if any further exception occurs
                err.error(str(e) + '. (flowID) = ' + '(' + str(self.id) + ')')

class Document(Data):

    def __init__(self, document):
        # type: (couchdb.Document) -> None

        self.id = uuid.uuid4()
        self.user = User()

        if failure is True:
            # precondition management
            pass
        elif not isinstance(document, couchdb.Document):
            err.error(str(IOError) + '. (flowID) = ' + '(' + str(self.id) + ')')

        else:
            # if all exceptions passed

            try:
                self.ref = document

                deb.debug('Document Data Created. (flowID, type, value, user) =  (' + str(self.id) + ',' + str(
                    type(document)) + ', ' + str(document) + ', ' + str(self.user.id) + ')')

                info.info(
                    'document data created >>> (docID, doc) = (' + str(self.id) + ', ' + str(self.ref) + ')')

            except Exception as e:
                # if any further exception occurs
                err.error(str(e) + '. (flowID) = ' + '(' + str(self.id) + ')')               
    
class Module:

    def logStart(self):
        deb.debug('Node Invokation Started. (moduleID) = ' + '(' + str(self.id) + ')')

        info.info('module start >>> (modID) = (' + str(self.__class__.__name__) + ')')

    def logEnd(self):
        deb.debug('Node Invokation Ended. (moduleID) = ' + '(' + str(self.id) + ')')

        info.info('module end >>> (modID) = (' + str(self.__class__.__name__) + ')')

    def body(self):
        '''
        :param interfaceParam:
        :return:
        '''

    def __init__(self, *args):

        self.id = uuid.uuid4()
        self.user = User()

        self.P = args

        try:

            log = ''
            for i in args:
                if isinstance(i, Object) or isinstance(i, File) or isinstance(i, Document):
                    log = log + '(' + str(i.id) + ', ' + str(i.__class__.__name__) + '), '
                else:
                    log = log +  str(i) + ', '

            deb.debug('Module Initiated. (inFlow, moduleID, user) = ' + '(' + log + ', ' + str(
                self.id) + ', ' + str(
                self.user.id) + ')')

            info.info('module initiated >>> (modID, input) = (' + str(self.id) + ', ' + log)

        except Exception as e:
            err.error(str(e))

    def run(self, when = True, false_return = None):

        if when is True:
            self.logStart()
            self.outgoing = self.body()

            log = ''
            for i in self.outgoing:
                log = log + str(i.id) + ', '

            info.info('module flows >>> (modID, outIDs) = (' + str(self.id) + ', ' + log + ')')
            self.logEnd()

            return self.outgoing

        else:
            return false_return

class User:

    def __init__(self):
        self.id = uuid.uuid4()
        self.name =getpass.getuser()

     
    