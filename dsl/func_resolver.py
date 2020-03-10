from importlib import import_module
import json
from os import path, getcwd
import os
import pathlib

from externalInfo import FileSystem, exechelper
from fileop import FilterManager, FolderItem

# import_module can't load the following modules in the NGINX server
# while running in 'immediate' mode. The early imports here are needed.
# This needs to be fixed, otherwise dynamic loading will not work.
# try:
#     import app.biowl.libraries.galaxy.adapter
# except:
#     pass
# 
# try:
#     import app.biowl.libraries.seqtk.adapter
#     import app.biowl.libraries.bowtie2.adapter
#     import app.biowl.libraries.bwa.adapter
#     import app.biowl.libraries.pysam.adapter
#     import app.biowl.libraries.fastqc.adapter    
#     import app.biowl.libraries.apachebeam.adapter
#     import app.biowl.libraries.flash.adapter
#     import app.biowl.libraries.hadoop.adapter
#     import app.biowl.libraries.pear.adapter
#     import app.biowl.libraries.seqtk.adapter
#     import app.biowl.libraries.usearch.adapter
#     import app.biowl.libraries.vsearch.adapter
# except:
#     pass

def load_module(modulename):
    '''
    Load a module dynamically from a string module name.
    It was first implemented with __import__, but later
    replaced by importlib.import_module.
    :param modulename:
    '''
    #if modulename not in sys.modules:
    #name = "package." + modulename
    #return __import__(modulename, fromlist=[''])
    return import_module(modulename)
      
class LibraryBase():
    def __init__(self):
        self.tasks = {}
        self.localdir = path.join(path.abspath(path.dirname(__file__)), 'storage')
    
    def add_task(self, name, expr):
        self.tasks[name] = expr
    
    def run_task(self, name, args, dotaskstmt):
        if name in self.tasks:
            return dotaskstmt(self.tasks[name][1:], args)

    def code_run_task(self, name, args, dotaskstmt):
        if name in self.tasks:
            return dotaskstmt(self.tasks[name], args), set()
           
#     def get_function(self, name, package = None):
#         services = Service.get_service_by_name_package(name, package)
#         return services.first().value
    
    @staticmethod
    def default(name):
        if name == None:
            name = 'Default Service'
        return name
    
    @staticmethod
    def check_function(name, package = None):
        return LibraryBase.default(name)
    
    @staticmethod
    def check_functions(v):
        for f in v:
            if LibraryBase.check_function(f.name, f.package):
                return True
        return False
            
        
    @staticmethod
    def split_args(arguments):
        args = []
        kwargs = {}
        for arg in arguments:
            if isinstance(arg, tuple):
                kwargs[arg[0]] = arg[1]
            else:
                args.append(arg)
        return args, kwargs
    
    @staticmethod
    def GetDataTypeFromFunc(returns, result = None):
        if not returns:
            return DataType.Custom
        
        datatype = DataType.Unknown
        try:
            returnsLower = returns.lower().split('|')
            if 'file' in returnsLower:
                fs = FileSystem.FS_by_typename("posix")
                if fs.isfile(result):
                    datatype = datatype | DataType.File
            elif 'folder' in returnsLower:
                fs = FileSystem.FS_by_typename("posix")
                if fs.isdir(result):
                    datatype = datatype | DataType.Folder
            elif 'file[]' in returnsLower:
                if type(result) == 'list':
                    datatype = datatype | DataType.FileList
            elif 'folder[]' in returnsLower:
                if type(result) == 'list':
                    datatype = datatype | DataType.FolderList
            else:
                datatype = DataType.Custom
        except:
            pass # don't propagate the exceptions
        return datatype




    @staticmethod
    def call_func(context, package, function, args):
        '''
        Call a function from a module.
        :param context: The context for output and error
        :param package: The name of the package. If it's empty, local function is called    
        :param function: Name of the function
        :param args: The arguments for the function
        '''
        task = None
        try:
            arguments, kwargs = LibraryBase.split_args(args)
            #func = Service.get_first_service_by_name_package(function, package)
            #func = {"package": "data", "name": "MyService", "internal": "", "returns": "file", "org": "", "group": "", "desc": "", "href": "", "example": "data = MyService(data)", "params": [], "module": "app.biowl.libraries.users.mishuk.mylib_102"}
            
            #task = TaskManager.create_task(context.runnable, func["name"])
            #context.task_id = task.id
            #task.start()
        
            if function.lower() == "print":
                result = context.write(*arguments)
            elif function.lower() == "range":
                result = range(*arguments)
            elif function.lower() == "read":
                if not arguments:
                    raise ValueError("Read must have one argument.")
                try:
                    print('Access right not checked')
                    #DataSourceAllocation.check_access_rights(context.user_id, arguments[0], AccessRights.Read)
                except:
                    pass
                fs = FileSystem.fs_by_prefix_or_default(arguments[0])
                result = fs.read(arguments[0])
            elif function.lower() == "write":
                if len(arguments) < 2:
                    raise ValueError("Write must have two arguments.")
                try:
                    print('Access right not checked')
                    #DataSourceAllocation.check_access_rights(context.user_id, arguments[0], AccessRights.Write)
                except:
                    pass
                fs = FileSystem.fs_by_prefix_or_default(arguments[0])
                result = fs.write(arguments[0], arguments[1])
                task.succeeded(DataType.File, str(result))
            elif function.lower() == "getfiles":
                try:
                    print('Access right not checked')
                    #DataSourceAllocation.check_access_rights(context.user_id, arguments[0], AccessRights.Read)
                except:
                    pass
                fs = FileSystem.fs_by_prefix_or_default(arguments[0])
                result = FolderItem.StrToFolderItem(FilterManager.listdirR(fs, arguments[0], arguments[1], True)) if len(arguments) == 2 else FolderItem.StrToFolderItem(fs.get_files(arguments[0]))
                task.succeeded(DataType.FileList, str(result))
            elif function.lower() == "getfolders":
                try:
                    print('Access right not checked')
                    #DataSourceAllocation.check_access_rights(context.user_id, arguments[0], AccessRights.Read)
                except:
                    pass
                fs = FileSystem.fs_by_prefix_or_default(arguments[0])
                result = fs.get_folders(arguments[0])
                task.succeeded(DataType.FolderList, str(result))
            elif function.lower() == "createfolder":
                try:
                    print('Access right not checked')
                    #DataSourceAllocation.check_access_rights(context.user_id, arguments[0], AccessRights.Write)
                except:
                    pass
                fs = FileSystem.fs_by_prefix_or_default(arguments[0])
                result = fs.makedirs(arguments[0])
                task.succeeded(DataType.FolderList, str(result))
            elif function.lower() == "remove":
                try:
                    print('Access right not checked')
                    #DataSourceAllocation.check_access_rights(context.user_id, arguments[0], AccessRights.Write)
                except:
                    pass
                fs = FileSystem.fs_by_prefix_or_default(arguments[0])
                result = fs.remove(arguments[0])
                task.succeeded(DataType.File, str(result))
            elif function.lower() == "makedirs":
                try:
                    print('Access right not checked')
                    #DataSourceAllocation.check_access_rights(context.user_id, arguments[0], AccessRights.Write)
                except:
                    pass
                fs = FileSystem.fs_by_prefix_or_default(arguments[0])
                result = fs.makedirs(arguments[0])
                task.succeeded(DataType.Folder, str(result))
            elif function.lower() == "getcwd":
                result = getcwd()
                task.succeeded(DataType.Text, str(result))
            elif function.lower() == "isfile":
                fs = FileSystem.fs_by_prefix_or_default(arguments[0])
                result = fs.isfile(arguments[0])
                task.succeeded(DataType.Text, str(result))
            elif function.lower() == "dirname":
                result = os.path.dirname(arguments[0])
                task.succeeded(DataType.Folder, str(result))
            elif function.lower() == "basename":
                result = os.path.basename(arguments[0])
            elif function.lower() == "getdatatype":
                try:
                    print('Access right not checked')
                    #DataSourceAllocation.check_access_rights(context.user_id, arguments[0], AccessRights.Read)
                except:
                    pass
                fs = FileSystem.fs_by_prefix_or_default(arguments[0])
                extension = pathlib.Path(arguments[0]).suffix
                result = extension[1:] if extension else extension
                task.succeeded(DataType.Text, str(result))
            elif function.lower() == "len":
                result = len(arguments[0])
                task.succeeded(DataType.Text, str(result))
            elif function.lower() == "exec":
                try:
                    print('Access right not checked')
                    #DataSourceAllocation.check_access_rights(context.user_id, arguments[0], AccessRights.Read)
                except:
                    pass
                result = exechelper.func_exec_run(arguments[0], *arguments[1:])
                task.succeeded(DataType.Text, str(result))
            elif function.lower() == "copyfile":
                try:
                    print('Access right not checked')
                    #DataSourceAllocation.check_access_rights(context.user_id, arguments[1], AccessRights.Write)
                except:
                    pass
                fs = FileSystem.fs_by_prefix_or_default(arguments[0])
                result = fs.copy(arguments[0], arguments[1])
                task.succeeded(DataType.File, str(result))
            elif function.lower() == "deletefile":
                try:
                    print('Access right not checked')
                    #DataSourceAllocation.check_access_rights(context.user_id, arguments[0], AccessRights.Write)
                except:
                    pass
                fs = FileSystem.fs_by_prefix_or_default(arguments[0])
                result = fs.remove(arguments[0])
                task.succeeded(DataType.File, str(result))
            else:
                module_obj = load_module(func["module"])
                function = getattr(module_obj, func["internal"])
                
                result = function(context, *arguments, **kwargs)
                
            #datatype = Library.GetDataTypeFromFunc(func[0].returns, result)
            
#                 DataProperty.add(data_alloc.id, { 'job_id': task.runnable_id}, DataType.Value)
#                 workflow_id = Runnable.query.get(task.runnable_id).workflow_id
#                 DataProperty.add(data_alloc.id, { 'workflow_id': workflow_id}, DataType.Value)
            return result
        except Exception as e:
            if task:
                task.failed(str(e))
            raise

    def code_func(self, context, package, function, arguments):
        '''
        Call a function from a module.
        :param context: The context for output and error
        :param package: The name of the package. If it's empty, local function is called    
        :param function: Name of the function
        :param arguments: The arguments for the function
        '''
        imports = set()
        args = ','.join(arguments)
        code = ''
        if not package or package == "None":
            if function.lower() == "print":
                code = "print({0})".format(args)
            elif function.lower() == "range":
                code = "range({0})".format(args)
            elif function.lower() == "read":
                imports.add("from fileop import IOHelper")
                code = "IOHelper.read({0})".format(args)
            elif function.lower() == "write":
                imports.add("from fileop import IOHelper")
                code = "IOHelper.write({0})".format(args)
            elif function.lower() == "getfiles":
                imports.add("from fileop import IOHelper")
                code = "IOHelper.getfiles({0})".format(args)
            elif function.lower() == "getfolders":
                imports.add("from fileop import IOHelper")
                code = "IOHelper.getfolders({0})".format(args)
            elif function.lower() == "remove":
                imports.add("from fileop import IOHelper")
                code = "IOHelper.remove({0})".format(args)
            elif function.lower() == "createfolder":
                imports.add("from fileop import IOHelper")
                code = "IOHelper.makedirs({0})".format(args)
            elif function.lower() == "getcwd":
                imports.add("import os")
                code = "os.getcwd()"
            elif function.lower() == "len":
                code = "len({0})".format(arguments[0])
            elif function.lower() == "exec":
                imports.add("import subprocess")
                code =  "func_exec_run({0}, {1})".format(arguments[0], arguments[1])

        if code:
            return code, imports
        
        imports.add("from importlib import import_module")
        func = self.get_function(function, package)
        code = "module_obj = load_module({0})\n".format(func[0].module)
        code += "function = getattr(module_obj, {0})\n".format(func[0].internal)
        if context.dci and context.dci[-1] and func.runmode == 'distibuted':
            args = [context.dci[-1]] + args
        code += "function({0})".format(args)
        return code, imports
            
    def __repr__(self):
        return "Library: " + repr(self.funcs)
    def __getitem__(self, key):
        return self.funcs[key]
    def __setitem__(self, key, val):
        self.funcs[key] = val
    def __delitem__(self, key):
        del self.funcs[key]
    def __contains__(self, key):
        return key in self.funcs
    def __iter__(self):
        return iter(self.funcs.keys())

    def __str__(self):                
        return "Library"

class DataType:
    Unknown = 0x00
    Folder = 0x01
    File = 0x02
    Image = 0x04
    Video = 0x08
    Binary = 0x10
    Text = 0x20
    CSV = 0x40
    SQL = 0x80
    Custom = 0x100
    Root = 0x200
    FileList = 0x400
    FolderList = 0x800,
    Value = 0x1000



