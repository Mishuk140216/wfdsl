from __future__ import print_function

import os
from os.path import join
import pathlib
import shutil
import sys
import tempfile
from urllib.parse import urlparse, urlunparse, urlsplit, urljoin
import uuid
import urllib
import re
from datetime import datetime

__author__ = "Mainul Hossain"
__date__ = "$Dec 10, 2016 2:23:14 PM$"

urllib.parse.uses_relative.append('hdfs')
urllib.parse.uses_netloc.append('hdfs')

try:
    import pyarrow
except:
    pass

try:
    from bioblend.galaxy import GalaxyInstance
except:
    pass

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
    
#     @staticmethod
#     def union(left, right):
#         v = []
#         if not isinstance(left, list):
#             left = [left]
#             
#         if not isinstance(right, list):
#             right = [right]
#         
#         rest = []
#         for l in left:
#             rest = [r for r in right if str(l) != str(r)]
#         left.extend(rest)
#         return left
    
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
      
#     @staticmethod
#     def substract(left, right):
#         if not isinstance(left, list):
#             left = [left]
#             
#         if not isinstance(right, list):
#             right = [right]
#             
#         for r in right:
#             left = [l for l in left if str(l) != str(r)]
#             
#         return left
    
    
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
    
class FilterManager:
    @staticmethod
    def Filter(fs, path, filename, filters):
        for fil in filters:
            if not fil['selected']:
                continue
            name = fil['name'].lower()
            if name == "name":
                if not FilterManager.CheckRegExValue(fil['value'], filename):
                    return None
            elif name == "created" or name == "modified" or name == "accessed" or name == "size":
                stat = fs.stat(fs.join(path, filename))
                if name == "size":
                    if not FilterManager.SizeInRange(stat.st_size, fil['value']):
                        return None
                else:
                    t = None
                    if name == "created":
                        t = stat.st_ctime
                    elif name == "modified":
                        t = stat.st_mtime
                    else:
                        t = stat.st_atime
                    if not FilterManager.DateInRange(t, fil['value']):
                        return None
            elif name == "key:value":
                pass
            
        return filename

    @staticmethod
    def CheckRegExValue(regexvalue, value):
        return re.search(regexvalue, value, flags=re.RegexFlag.IGNORECASE)
        
    @staticmethod
    def DateInRange(dateV, dateR):
        begin_end = FilterManager.SplitDateRange(dateR)
        if not begin_end:
            return True
        if dateV < datetime.timestamp(begin_end[0]):
            return False
        if len(begin_end) > 1 and  dateV > datetime.timestamp(begin_end[1]):
            return False
        return True
    
    @staticmethod
    def SizeInRange(sizeV, sizeR):
        sizeR = sizeR.replace(" KB", "")
        begin_end = sizeR.split(",")
        if not begin_end:
            return True
        if sizeV < int(begin_end[0]) * 1024:
            return False
        if len(begin_end) > 1 and  sizeV > int(begin_end[1]) * 1024:
            #print_len = len(begin_end)
            #print_endSize = int(begin_end[1]) * 1024
            return False
        return True
    
    @staticmethod
    def SplitDateRange(dateR):
        begin_end = dateR.split('-')
        return [datetime.strptime(begin_end[0].strip(), '%m/%d/%Y'), datetime.strptime(begin_end[1].strip(), '%m/%d/%Y')] 

    @staticmethod
    def Sort(fs, filenames, filters):
        for fil in filters:
            if fil['selected'] and fil['sort']:
                name = fil['name'].lower()
                if name == "name":
                    filenames = sorted(filenames, reverse = (int(fil['sort'])==2))
        return filenames
    
    @staticmethod
    def listdirR(fs, path, filters, flatfiles = False):
        
        ancestors = FilterManager.listdirRInternal(fs, path, filters, flatfiles)
        if not ancestors:
            return None
        
        if flatfiles:
            return ancestors
        
        ancestorPath = path
        while fs.dirname(ancestorPath) and fs.strip_root(ancestorPath) != os.sep:
            ancestorPath = fs.dirname(ancestorPath) 
            ancestor = fs.make_json_item(ancestorPath)
            if fs.strip_root(ancestorPath) == os.sep:
                ancestor['text'] = fs.prefix
            ancestor['children'].append(ancestors)
            ancestors = ancestor
                
        return ancestors
            
    @staticmethod
    def listdirRInternal(fs, path, filters, flatfiles = False):
        if fs.isfile(path):
            filtered_file = filter(path, filters)
            if not filtered_file:
                return None
            
            return fs.make_json_item(path) if not flatfiles else path

        fsitems = []
        for fd in fs.get_folders(path):
            fditem = FilterManager.listdirRInternal(fs, fs.join(path, fd), filters, flatfiles)
            if fditem:
                fsitems.append(fditem)
        
        filter_active = any(f['selected'] for f in filters)
        fitems = []
        for f in fs.get_files(path):
            filename = fs.basename(f)
            if not filter_active or FilterManager.Filter(fs, path, filename, filters):
                fitems.append(filename)
                
        if fitems:
            fitems = FilterManager.Sort(fs, fitems, filters)
            for fitem in fitems:
                if flatfiles:
                    fsitems.append(fs.join(path, fitem))
                else:
                    fsitems.append(fs.make_json_item(fs.join(path, fitem)))
        
        if fsitems and not flatfiles:
            data_json = fs.make_json_item(path)
            data_json['children'] = fsitems
            data_json['loaded'] = True
            fsitems = data_json
            
        return fsitems
        
class BaseFileSystem(object):
    def __init__(self, root, public, prefix):
        self.localdir = root
        self.prefix = prefix
        self.public = public
        self.url = '/'
        
    def is_parent_of(self, parent, child):
        parent = self.normalize_path(parent)
        child = self.normalize_path(child)
        return child.startswith(parent)

    def normalize_fullpath(self, path):
        return self.normalize_path(path)
    
    def mkdir(self, path):
        raise ValueError("Not implemented error.")
    
    def remove(self, path):
        raise ValueError("Not implemented error.")
    
    def rename(self, oldpath, newpath):
        raise ValueError("Not implemented error.")
    
    def get_files(self, path):
        raise ValueError("Not implemented error.")
    
    def get_folders(self, path):
        raise ValueError("Not implemented error.")
    
    def listdir(self, path):
        raise ValueError("Not implemented error.")
    
    def listdirR(self, path, filters):
        raise ValueError("Not implemented error.")
    
    def copyfile(self, src, dst):
        raise ValueError("Not implemented error.")
                
    def read(self, path):
        raise ValueError("Not implemented error.")
    
    def write(self, path, content):
        raise ValueError("Not implemented error.")
        
    def unique_filename(self, path, prefix, ext):
        raise ValueError("Not implemented error.")
    
    def make_unique_dir(self, path):
        raise ValueError("Not implemented error.")
            
    def exists(self, path):
        raise ValueError("Not implemented error.")
        
    def isdir(self, path):
        raise ValueError("Not implemented error.")
    
    def isfile(self, path):
        raise ValueError("Not implemented error.")
    
    def join(self, path1, path2):
        raise ValueError("Not implemented error.")
    
    def basename(self, path):
        raise ValueError("Not implemented error.")
    
    #check again
    def dirname(self, path):
        raise ValueError("Not implemented error.")

    def make_json(self, path):
        raise ValueError("Not implemented error.")
    
    def make_json_r(self, path):
        raise ValueError("Not implemented error.")

    def save_upload(self, file, path):
        raise ValueError("Not implemented error.")
    
    def download(self, path):
        raise ValueError("Not implemented error.")
    
    def stat(self, path):
        raise ValueError("Not implemented error.")

class PosixFileSystem(BaseFileSystem):
    
    def __init__(self, root, public = None, prefix = None):
        super().__init__(root, public, prefix)
    
    def typename(self):
        return "posix"
        
    def normalize_path(self, path):
        path = os.path.normpath(path)
        if self.prefix:
            path = self.strip_prefix(path)
        if not self.localdir or path.startswith(self.localdir) or (self.localdir.endswith(os.sep) and path == self.localdir[:-1]):
            return path
        while path and path[0] == os.sep:
            path = path[1:]    
        return os.path.join(self.localdir, path)
    
    def makedirs(self, path):
        path = self.normalize_path(path)
        if not os.path.exists(path):
            os.makedirs(path)
        return self.make_prefix(path)
    
    def unique_fs_name(self, path, prefix, ext):
        return IOHelper.unique_fs_name(self, path, prefix, ext)
                    
    def make_prefix(self, path):
        if not self.prefix:
            return path     
        if path.startswith(self.prefix):
            return path
        if path.startswith(self.localdir):
            path = path[len(self.localdir):]
        if not path.startswith(os.sep):
            path = os.sep + path
        return self.prefix + path
    
    def make_url(self, path):
        path = self.strip_prefix(path) 
        if self.localdir and path.startswith(self.localdir):
            path = path[len(self.localdir):]
        return path if path.startswith(os.sep) else os.sep + path
    
    def strip_prefix(self, path):
        return path[len(self.prefix):] if self.prefix and path.startswith(self.prefix) else path
        
    def strip_root(self, path):
        path = self.strip_prefix(path)
        if path.startswith(self.localdir):
            path = path[len(self.localdir):]
        return path if path.startswith(os.sep) else os.sep + path
            
    def mkdir(self, path):
        path = self.normalize_path(path)
        if not os.path.exists(path):
            os.mkdir(path) 
        return self.make_prefix(path)
    
    def remove(self, path):
        path = self.normalize_path(path)
        dirpath = os.path.dirname(path)
        if os.path.isdir(path):
            shutil.rmtree(path)
        elif os.path.isfile(path):
            os.remove(path)
        return self.make_prefix(dirpath)
               
    def rename(self, oldpath, newpath):
        oldpath = self.normalize_path(oldpath)
        newpath = self.join(self.dirname(oldpath), newpath)
        newpath = self.normalize_path(newpath)
        
        if oldpath == newpath:
            return oldpath
        
        os.rename(oldpath, newpath)
        return self.strip_root(newpath)
    
    def get_files(self, path):
        path = self.normalize_path(path)
        return [self.strip_root(os.path.join(path, f)) for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]
    
    def get_folders(self, path):
        path = self.normalize_path(path)
        return [f for f in os.listdir(path) if os.path.isdir(os.path.join(path, f))]
    
    def listdir(self, path):
        path = self.normalize_path(path)
        return os.listdir(path)
    
    def listdirR(self, path, filters):
        filters = filters if not filters or type(filters) is list else [filters]
        return FilterManager.listdirR(self, path, filters)
       
    def copyfile(self, src, dst):
        return shutil.copy2(self.normalize_path(src), self.normalize_path(dst))
    
    def stat(self, path):
        return os.stat(self.normalize_path(path))
    
    def read(self, path):
        path = self.normalize_path(path)
        with open(path, 'rb') as reader:
            return reader.read()
        
    def write(self, path, content):
        path = self.normalize_path(path)
        with open(path, 'wb') as writer:
            return writer.write(content)
        
    def unique_filename(self, path, prefix, ext):
        stem = pathlib.Path(prefix).stem
        if not ext:
            ext = pathlib.Path(prefix).suffix
        make_fn = lambda i: self.join(path, '{0}_{1}.{2}'.format(stem, i, ext) if ext else '{0}_{1}'.format(stem, i))

        for i in range(1, sys.maxsize):
            uni_fn = make_fn(i)
            if not self.exists(uni_fn):
                return uni_fn
    
    def make_unique_dir(self, path):
        unique_dir = self.join(path, str(uuid.uuid4()))
        unique_dir = self.normalize_path(unique_dir)
        os.makedirs(unique_dir)
        return unique_dir
            
    def exists(self, path):
        return os.path.exists(self.normalize_path(path))
        
    def isdir(self, path):
        return os.path.isdir(self.normalize_path(path))
    
    def isfile(self, path):
        return os.path.isfile(self.normalize_path(path))
    
    def join(self, path1, path2):
        path1 = self.normalize_path(path1)
        return self.make_url(os.path.join(path1, path2))
    
    def get_json_name(self, path):
        if (self.localdir and self.localdir == path) or path == os.sep:
            return self.prefix if self.prefix else self.localdir 
        
        return self.basename(path)
        
    def make_json_item(self, path):
        data_json =  { 'path': self.make_url(path), 'text': self.get_json_name(path) }
        if self.isdir(path):
            data_json['children'] = []
            data_json['type'] = 'folder'
        else:
            data_json['type'] = 'file'
        return data_json
        
    def make_json(self, path):
        data_json = self.make_json_item(path)
        
        if 'children' in data_json: # folder
            data_json['children'] = [self.make_json_item(self.join(path, fn)) for fn in self.listdir(path)]
            data_json['loaded'] = True
        return data_json
    
    def make_json_r(self, path):
        data_json = self.make_json_item(path)  
        if 'children' in data_json: # folder 
            data_json['children'] = [self.make_json_r(self.join(path, fn)) for fn in self.listdir(path)]
            data_json['loaded'] = True
        return data_json

    def save_upload(self, file, path):
        path = self.normalize_path(path)
        if self.isfile(path):
            path = os.path.dirname(path)
        elif not os.path.exists(path):
            os.makedirs(path)
        path = os.path.join(path, file.filename)
        file.save(path)
        return self.strip_root(path)
    
    def download(self, path):
        path = self.normalize_path(path)
        if os.path.isfile(path):
            return path
        else:
            return None
    
    def basename(self, path):
        path = self.normalize_path(path)
        return os.path.basename(path)
    
    #check again
    def dirname(self, path):
        path = self.strip_root(path)
        return os.path.dirname(path) if path.startswith('/') else path
    
                                   
class HadoopFileSystem():
    timeout = 20 # 100s timeout
    def __init__(self, url, root, user, prefix = None):
        u = urlsplit(url)
        if u.scheme != 'http' and u.scheme != 'https' and u.scheme != 'hdfs':
            raise ValueError("Invalid name node address")
        
        parsedurl = urlparse(url)
        self.client = pyarrow.hdfs.connect(host=parsedurl.hostname, port=parsedurl.port, user=user, driver='libhdfs')
        self.url = urlunparse((u.scheme, u.netloc, '', '', '', ''))
        self.localdir = root
        self.prefix = prefix
    
    def typename(self):
        return "hdfs"
    
    def normalize_path(self, path):
        if self.prefix:
            path = self.strip_prefix(path)
            
        if self.url and path.startswith(self.url):
            path = path[len(self.url):]
            if not path.startswith(os.sep):
                path = os.sep + path
        
        if self.localdir and path.startswith(self.localdir):
            return path
 
        while path and path[0] == os.sep:
            path = path[1:]
        return os.path.join(self.localdir, path)
    
    def normalize_fullpath(self, path):
        return urljoin(self.url, self.normalize_path(path))
    
    def strip_prefix(self, path):
        return path[len(self.prefix):] if self.prefix and path.startswith(self.prefix) else path
    
    def strip_root(self, path):
        path = self.strip_prefix(path)
        if path.startswith(self.url):
            path = path[len(self.url):]
            if not path.startswith(self.localdir):
                raise 'Invalid hdfs path. It must start with the root directory'
        return path[len(self.localdir):] if path.startswith(self.localdir) else path
    
    def makedirs(self, path):
        path = self.normalize_path(path)
        if self.exists(path):
            return path
        dirnames = self.strip_root(path).split(os.sep)
        done = ''
        for dirname in dirnames:
            done = os.path.join(done, dirname)
            self.mkdir(done)
        return path
        
    def mkdir(self, path):
        path = self.normalize_path(path)
        self.client.mkdir(path)
        return path
    
    def unique_fs_name(self, path, prefix, ext):
        return IOHelper.unique_fs_name(self, path, prefix, ext)
    
    def remove(self, path):
        try: 
            path = self.normalize_path(path)
            if not self.client.exists(path):
                self.client.rm(path, True)
        except Exception as e:
            print(e)
           
    def rename(self, oldpath, newpath):
        try:
            oldpath = self.normalize_path(oldpath)
            newpath = self.normalize_path(newpath)
            self.client.rename(oldpath, newpath)
            return self.strip_root(newpath)
        except Exception as e:
            print(e)
    
    def get_files(self, path):
        path = self.normalize_path(path)
        files = []
        for f in self.client.ls(path):
            if self.client.isfile(join(path, f)):
                files.append(f)
        return files
    
    def get_folders(self, path):
        path = self.normalize_path(path)
        folders = []
        for f in self.client.list(path):
            if self.client.isdir(join(path, f)):
                folders.append(f)
        return folders
    
    def copyfile(self, src, dst):
        content = self.read(src)
        if self.isdir(dst):
            dst = os.path.join(dst, os.path.basename(src))
        self.write(dst, content)
        return self.normalize_fullpath(dst)
    
    def listdir(self, path):
        path = self.normalize_path(path)
        for f in self.client.ls(path):
            yield f
    
    def unique_filename(self, path, prefix, ext):
        stem = pathlib.Path(prefix).stem
        if not ext:
            ext = pathlib.Path(prefix).suffix
        make_fn = lambda i: self.join(path, '{0}_{1}.{2}'.format(stem, i, ext) if ext else '{0}_{1}'.format(stem, i))

        for i in range(1, sys.maxsize):
            uni_fn = make_fn(i)
            if not self.exists(uni_fn):
                return uni_fn
    
    def make_unique_dir(self, path):
        unique_dir = self.join(path, str(uuid.uuid4()))
        self.makedirs(unique_dir)
        return unique_dir
    
    def exists(self, path):
        path = self.normalize_path(path)
        return self.client.exists(path)
        
    def isdir(self, path):
        path = self.normalize_path(path)
        return self.client.isdir(path)
    
    def isfile(self, path):
        path = self.normalize_path(path)
        return self.client.isfile(path)
    
    def join(self, path1, path2):
        path1 = self.normalize_path(path1)
        return os.path.join(path1, path2)
    
    def read(self, path):
        path = self.normalize_path(path)
        with self.client.open(path, 'rb') as reader:
            return reader.read()
    
    def write(self, path, content):
        path = self.normalize_path(path)
        with self.client.open(path, 'wb') as writer:
            return writer.write(content)
    
    def make_json_item(self, path):
        data_json = { 'path': urljoin(self.url, self.normalize_path(path)), 'text': os.path.basename(path) }
        if self.isdir(path):
            data_json['nodes'] = []
        return data_json
        
    def make_json(self, path):
        data_json = self.make_json_item(path)
        
        if self.isdir(path):
            data_json['nodes'] = [self.make_json_item(os.path.join(path, fn)) for fn in self.listdir(self.normalize_path(path))]
            data_json['loaded'] = True
        return data_json
    
    def make_json_r(self, path):
        data_json = self.make_json_item(path)
        if self.isdir(path):
            data_json['nodes'] = [self.make_json_r(os.path.join(path, fn)) for fn in self.listdir(self.normalize_path(path))]
            data_json['loaded'] = True
        return data_json
     
    def save_upload(self, file, path):       
        localpath = os.path.join(tempfile.gettempdir(), os.path.basename(file.filename))
            
        if os.path.exists(localpath):
            fs = PosixFileSystem('/')
            unique_dir = fs.make_unique_dir(os.path.dirname(localpath))
            localpath = os.path.join(unique_dir, os.path.basename(file.filename))
            
        try:
            file.save(localpath)
            if self.isfile(path):
                path = os.path.dirname(path)
            
            with open(localpath, 'rb') as reader:
                self.write(path, reader.read())
        except:
            pass
        
    def download(self, path):
        path = self.normalize_path(path)
        if self.client.isfile(path):
            localpath = os.path.join(tempfile.gettempdir(), os.path.basename(path))
            if os.path.exists(localpath):
                fs = PosixFileSystem('/')
                unique_dir = fs.make_unique_dir(os.path.dirname(localpath))
                localpath = os.path.join(unique_dir, os.path.basename(path))
            
            with open(localpath, "wb") as writer:
                writer.write(self.read(path))
            return localpath

class GalaxyFileSystem():
    urlKey = 0
    hlddTitleKey = 1
    hlddKey = 2
    folderKey = 3
    hdaKey = 3
    lddaKey = 4
    
    def __init__(self, url, user):
        u = urlsplit(url)
        if u.scheme != 'http' and u.scheme != 'https':
            raise ValueError("Invalid name node address")
        
        self.url = urlunparse((u.scheme, u.netloc, '', '', '', ''))
        self.localdir = ""
        self.prefix = 'GalaxyFS'
        self.lddaprefix = 'Libraries'
        self.hdaprefix = 'Histories'
        self.client = GalaxyInstance(self.url, user)
    
    def typename(self):
        return "gfs"
    
    def strip_prefix(self, path):
        return path[len(self.prefix):] if self.prefix and path.startswith(self.prefix) else path
    
    def normalize_path(self, path):
        if self.prefix:
            path = self.strip_prefix(path)
            
        if self.url and path.startswith(self.url):
            return path
        
        if not self.localdir or path.startswith(self.localdir):
                return os.path.join(self.url, path)
 
        while path and path[0] == os.sep:
            path = path[1:]
        return os.path.join(self.url, self.localdir, path)

    def normalize_fullpath(self, path):
        return self.normalize_path(path)
    
    def strip_root(self, path):
        path = self.strip_prefix(path)
        if path.startswith(self.url):
            path = path[len(self.url):]
            if not path.startswith(self.localdir):
                raise 'Invalid hdfs path. It must start with the root directory'
        return path[len(self.localdir):] if path.startswith(self.localdir) else path
    
    def make_fullpath(self, path):
        path = self.normalize_path(path)
        return os.path.join(self.prefix, path)
     
    def makedirs(self, path):
        return self.mkdir(path)
        
    def mkdir(self, path):
        try:
            path = self.normalize_path(path)
            parts = self.path_parts(path)
            
            if len(parts) > 4 or len(parts) < 3:
                return "" #raise ValueError("Galaxy path may have maximum 4 parts.")
            hd_ldd = ''
            if len(parts) == 3:
                hd_ldd = self.client.libraries.create_library(parts[-1]) if self.islibrary(parts[GalaxyFileSystem.hlddTitleKey]) else self.client.histories.create_history(parts[-1])
            elif len(parts) == 4:
                if not self.islibrary(parts[GalaxyFileSystem.hlddTitleKey]):
                    return ""
                hd_ldd = self.client.libraries.create_folder(parts[GalaxyFileSystem.lddaKey], parts[-1])
                
            parts[-1] = hd_ldd['id']
            
            return os.sep.join(parts['id'])
        except:
            return None
    
    def unique_fs_name(self, path, prefix, ext):
        return os.path.join(path, prefix + "_" + str(uuid.uuid4()) + ext)
    
    def remove(self, path):
        try:
            path = self.normalize_path(path)
            parts = self.path_parts(path)
            if len(parts) > 4:
                raise ValueError("Galaxy path may have maximum 4 parts.")
            if self.islibrary(parts[GalaxyFileSystem.hlddTitleKey]):
                self.client.libraries.delete_library(library_id = parts[-1])
            else:
                self.client.histories.delete_history(history_id = parts[-1])
        except Exception as e:
            print(e)
           
    def rename(self, oldpath, newpath):
        try:
            oldpath = self.normalize_path(oldpath)
            newpath = self.normalize_path(newpath)
            self.client.rename(oldpath, newpath)
            return self.strip_root(newpath)
        except Exception as e:
            print(e)
    
    def copyfile(self, src, dst):
        if self.islibrarydata(src) and not self.islibrarydata(dst):
            parts = self.path_parts(self.normalize_path(src))
            self.client.libraries.copy_from_dataset(parts[GalaxyFileSystem.hlddKey], self.id_from_path(dst), parts[GalaxyFileSystem.folderKey])
        elif not self.islibrary(parts[GalaxyFileSystem.hlddTitleKey]) and self.islibrarydata(dst):
            parts = self.path_parts(self.normalize_path(dst))
            self.client.histories.upload_dataset_from_library(parts[GalaxyFileSystem.hdaKey], self.id_from_path(src))
        else:
            content = self.read(src)
            if self.isdir(dst):
                dst = os.path.join(dst, os.path.basename(src))
                self.write(dst, content)
        return self.normalize_fullpath(dst)
    
    def get_files(self, path):
        path = self.normalize_path(path)
        files = []
        for f in self.client.list(path):
            status = self.client.status(join(path, f), False)
            if status['type'] != "DIRECTORY":
                files.append(f)
        return files
    
    def get_folders(self, path):
        try:
            normalized_path = self.normalize_path(path)
            parts = self.path_parts(normalized_path)
            if len(parts) > 4:
                raise ValueError("Galaxy path may have maximum 4 parts.")

            parts[-1] = self.client.libraries.create_library(parts[-1]) if self.islibrary(parts[GalaxyFileSystem.hlddTitleKey]) else self.client.histories.create_history(parts[-1])
            
            path = os.sep.join(parts)
            return self.make_fullpath(path)
        except:
            return []
        return path       
    
    def exists(self, path):
        return self.isdir(path) or self.isfile(path)
     
    def islibrary(self, name):
        return name == self.lddaprefix
    
    def islibrarydata(self, path):
        normalized_path = self.normalize_path(path)
        parts = self.path_parts(normalized_path)
        return self.islibrary(parts[GalaxyFileSystem.hlddTitleKey])
       
    def isdir(self, path):
        normalized_path = self.normalize_path(path)
        parts = self.path_parts(normalized_path)
        return len(parts) <= GalaxyFileSystem.lddaKey if self.islibrary(parts[GalaxyFileSystem.hlddTitleKey]) else len(parts) <= GalaxyFileSystem.hdaKey
    
    def isfile(self, path):
        return not self.isdir(path) and self.name_from_id(path) != ""
    
    def join(self, path1, path2):
        path1 = self.normalize_path(path1)
        return os.path.join(path1, path2)
    
    def make_unique_dir(self, path):
        unique_dir = self.join(path, str(uuid.uuid4()))
        self.makedirs(unique_dir)
        return unique_dir
        
    def read(self, path):
        path = self.normalize_path(path)
        with self.client.read(path, 'rb') as reader:
            return reader.read()
    
    def write(self, path, content):
        path = self.normalize_path(path)
        self.client.write(path, content)
    
    def id_from_path(self, path):
        normalized_path = self.normalize_path(path)
        parts = self.path_parts(normalized_path)
        if len(parts) <= GalaxyFileSystem.urlKey + 1:
            return ""
        elif len(parts) == GalaxyFileSystem.hlddTitleKey + 1:#Histories/Libraries
            return parts[GalaxyFileSystem.hlddTitleKey] 
        elif len(parts) == GalaxyFileSystem.hlddKey + 1: #library-name/history-name
            info = self.client.libraries.get_libraries(library_id = parts[GalaxyFileSystem.hlddKey])[0] if parts[GalaxyFileSystem.hlddTitleKey] == self.lddaprefix else self.client.histories.get_histories(history_id = parts[GalaxyFileSystem.hlddKey])[0]
            return info['id']
        elif len(parts) == GalaxyFileSystem.folderKey + 1: #Folder(library)/Dataset(history)
            if parts[GalaxyFileSystem.hlddTitleKey] == self.lddaprefix:
                folder = self.client.folders.show_folder(parts[3], False)
                return folder['id']
            else:
                info = self.client.datasets.show_dataset(dataset_id = parts[GalaxyFileSystem.hdaKey], hda_ldda = 'hda')
                return info['id']
        elif len(parts) == GalaxyFileSystem.lddaKey + 1:
            info = self.client.datasets.show_dataset(dataset_id = parts[GalaxyFileSystem.lddaKey], hda_ldda = 'ldda')
            return info['id']
    
    def path_parts(self, path):
        parts = []
        if path.startswith(self.prefix):
            parts.append(self.prefix)
            if len(path) > len(self.prefix):
                path = path[len(self.prefix) + 1:]
        elif path.startswith(self.url):
            parts.append(self.url)
            if len(path) > len(self.url):
                path = path[len(self.url) + 1:]
            
        parts.extend(pathlib.Path(path).parts)
        return parts
              
    def name_from_id(self, path):
        normalized_path = self.normalize_path(path)
        parts = self.path_parts(normalized_path)
        if len(parts) <= GalaxyFileSystem.urlKey + 1:
            return ""
        elif len(parts) == GalaxyFileSystem.hlddTitleKey + 1:#Histories/Libraries
            return parts[GalaxyFileSystem.hlddTitleKey] 
        elif len(parts) == GalaxyFileSystem.hlddKey + 1: #library-name/history-name
            info = self.client.libraries.get_libraries(library_id = parts[GalaxyFileSystem.hlddKey])[0] if self.islibrary(parts[GalaxyFileSystem.hlddTitleKey]) else self.client.histories.get_histories(history_id = parts[GalaxyFileSystem.hlddKey])[0]
            return info['name']
        elif len(parts) == GalaxyFileSystem.folderKey + 1: #Folder(library)/Dataset(history)
            if parts[GalaxyFileSystem.hlddTitleKey] == self.lddaprefix:
                folder = self.client.folders.show_folder(parts[GalaxyFileSystem.folderKey], False)
                return folder['name']
            else:
                info = self.client.datasets.show_dataset(dataset_id = parts[GalaxyFileSystem.hdaKey], hda_ldda = 'hda')
                return info['name']
        elif len(parts) == GalaxyFileSystem.lddaKey + 1:
            info = self.client.datasets.show_dataset(dataset_id = parts[GalaxyFileSystem.lddaKey], hda_ldda = 'ldda')
            return info['name']
    
    def make_json_item(self, path):
        data_json = { 'path': self.normalize_path(path), 'text': "{0}(id:{1})".format(self.name_from_id(path), self.id_from_path(path)) }
        if self.isdir(path):
            data_json['nodes'] = []
        return data_json
        
    def make_json(self, path):
        normalized_path = self.normalize_path(path)
        if not normalized_path or normalized_path == self.url:
            return [self.make_json_item(urljoin(self.url, self.lddaprefix)), self.make_json_item(urljoin(self.url, self.hdaprefix))]
        else:
            data_json = self.make_json_item(path)
            parts = self.path_parts(normalized_path)
            if self.islibrary(parts[GalaxyFileSystem.hlddTitleKey]):
                if len(parts) == GalaxyFileSystem.hlddTitleKey + 1:
                    libraries = self.client.libraries.get_libraries()
                    data_json['nodes'] = [self.make_json_item(os.path.join(path, fn['id'])) for fn in libraries]
                elif len(parts) == GalaxyFileSystem.hlddKey + 1:
                    folders = self.client.libraries.get_folders(library_id = parts[GalaxyFileSystem.hlddKey])
                    data_json['nodes'] = [self.make_json_item(os.path.join(path, fn['id'])) for fn in folders]
                elif len(parts) == GalaxyFileSystem.folderKey + 1:
                    folder = self.client.folders.show_folder(parts[GalaxyFileSystem.folderKey], True)
                    data_json['nodes'] = [self.make_json_item(os.path.join(path, fn['id'])) for fn in folder['folder_contents']]
            else:
                if len(parts) == GalaxyFileSystem.hlddTitleKey + 1:
                    histories = self.client.histories.get_histories()
                    data_json['nodes'] = [self.make_json_item(os.path.join(path, fn['id'])) for fn in histories]
                elif len(parts) == GalaxyFileSystem.hlddKey + 1:
                    datasets = self.client.histories.show_matching_datasets(parts[GalaxyFileSystem.hlddKey])
                    data_json['nodes'] = [self.make_json_item(os.path.join(path, fn['id'])) for fn in datasets]
            
            data_json['loaded'] = True
            return data_json
    
    @staticmethod    
    def get_history_path(url, history_id, data_id):
        return urljoin(url, os.path.join('Histories', history_id, data_id))
            
    def make_json_r(self, path):
        normalized_path = self.normalize_path(path)
        if not normalized_path or normalized_path == self.url:
            return [self.make_json_r(urljoin(self.url, self.lddaprefix)), self.make_json_r(urljoin(self.url, self.hdaprefix))]
        else:
            data_json = self.make_json_item(path)
            parts = self.path_parts(normalized_path)
            if self.islibrary(parts[GalaxyFileSystem.hlddTitleKey]):
                if len(parts) == GalaxyFileSystem.hlddTitleKey + 1:
                    libraries = self.client.libraries.get_libraries()
                    data_json['nodes'] = [self.make_json_r(os.path.join(path, fn['id'])) for fn in libraries]
                elif len(parts) == GalaxyFileSystem.hlddKey + 1:
                    folders = self.client.libraries.get_folders(library_id = parts[GalaxyFileSystem.hlddKey])
                    data_json['nodes'] = [self.make_json_r(os.path.join(path, fn['id'])) for fn in folders]
                elif len(parts) == GalaxyFileSystem.folderKey + 1:
                    folder = self.client.folders.show_folder(parts[GalaxyFileSystem.folderKey], True)
                    data_json['nodes'] = [self.make_json_item(os.path.join(path, fn['id'])) for fn in folder['folder_contents']]
            else :
                if len(parts) == GalaxyFileSystem.hlddTitleKey + 1:
                    histories = self.client.histories.get_histories()
                    data_json['nodes'] = [self.make_json_r(os.path.join(path, fn['id'])) for fn in histories]
                elif len(parts) == GalaxyFileSystem.hlddKey + 1:
                    datasets = self.client.histories.show_matching_datasets(parts[GalaxyFileSystem.hlddKey])
                    data_json['nodes'] = [self.make_json_item(os.path.join(path, fn['id'])) for fn in datasets]
                    
            data_json['loaded'] = True
            return data_json
     
    def save_upload(self, file, path):
        if self.isfile(path):
            path = os.path.dirname(path)
        elif not self.isdir(path):
            return ""
        parts = self.path_parts(path)
        if not parts or len(parts) < 3:
            return ""
        
        localpath = os.path.join(tempfile.gettempdir(), os.path.basename(file.filename))
            
        if os.path.exists(localpath):
            fs = PosixFileSystem('/')
            unique_dir = fs.make_unique_dir(os.path.dirname(localpath))
            localpath = os.path.join(unique_dir, os.path.basename(file.filename))
            
        try:
            file.save(localpath)
            
            dataset = ''        
            if self.islibrary(parts[1]):
                dataset = self.client.libraries.upload_file_from_local_path(parts[2], localpath, folder_id=parts[3] if len(parts) > 3 else None)
            else:
                dataset = self.client.tools.upload_file(localpath, parts[2])
            
            if dataset:
                return os.path.join(path, dataset['id']);
        except:
            pass
                
    def download(self, path):
        path = self.normalize_path(path)
        if self.isdir(path):
            return None
        
        dataset = self.client.datasets.show_dataset(dataset_id = os.path.basename(path), hda_ldda = 'ldda' if self.islibrarydata(path) else 'hda')
        name = dataset['name']
        if not pathlib.Path(name).suffix and dataset['file_ext']:
            name += '.' + dataset['file_ext']
        
        localpath = os.path.join(tempfile.gettempdir(), name)
            
        if os.path.exists(localpath):
            fs = PosixFileSystem('/')
            unique_dir = fs.make_unique_dir(os.path.dirname(localpath))
            localpath = os.path.join(unique_dir, name)
        
        self.client.datasets.download_dataset(os.path.basename(path), file_path = localpath, use_default_filename=False)
        return localpath
        
class IOHelper():
    @staticmethod
    def getFileSystem(url):
        try:
            u = urlsplit(url)
            if u.scheme == 'http' or u.scheme == 'https' or u.scheme == 'hdfs':
                return HadoopFileSystem(url, 'hdfs')
        except:
            pass
        return PosixFileSystem()
    
    @staticmethod
    def get_files(path):
        filesystem = IOHelper.getFileSystem(path)
        return filesystem.get_files(path)
    
    @staticmethod
    def get_folders(path):
        filesystem = IOHelper.getFileSystem(path)
        return filesystem.get_folders(path)
    
    @staticmethod
    def remove(path):
        filesystem = IOHelper.getFileSystem(path)
        filesystem.remove(path)
        
    @staticmethod
    def makedirs(path):
        filesystem = IOHelper.getFileSystem(path)
        return filesystem.makedirs(path)
    
    @staticmethod
    def rename(oldpath, newpath):
        filesystem = IOHelper.getFileSystem(oldpath)
        filesystem.rename(oldpath, newpath)
        
    @staticmethod
    def read(path):
        filesystem = IOHelper.getFileSystem(path)
        return filesystem.read(path)
    
    @staticmethod
    def normalize_path(path):
        filesystem = IOHelper.getFileSystem(path)
        return filesystem.normalize_path(path)
    
    @staticmethod
    def write(path, content):
        filesystem = IOHelper.getFileSystem(path)
        return filesystem.write(path, content)
    
    @staticmethod
    def unique_fs_name(filesystem, path, prefix, ext):

        make_fn = lambda i: os.path.join(path, '{0}({1}){2}'.format(prefix, i, ext))

        for i in range(1, sys.maxsize):
            uni_fn = make_fn(i)
            if not filesystem.exists(uni_fn):
                return uni_fn

    @staticmethod
    def unique_filename(path, prefix, ext):
        filesystem = IOHelper.getFileSystem(path)
        return IOHelper.unique_fs_name(filesystem, path, prefix, ext)
                        
if __name__ == "__main__":
    print("Hello World")