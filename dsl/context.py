import threading

from dsl.func_resolver import LibraryBase
from dsl.util import FileSystem


class SymbolTable():
    '''
    Table to hold program symbols (vars and functions).
    For local symbols, a stack of symbol tables is
    maintained.
    '''
    def __init__(self):
        '''
        Initializes the table.
        '''
        self.vars = {}
        self.funcs = {}
        
    def add_var(self, name, value):
        '''
        Adds/Overwrites a variable to the symbol table.
        :param name:
        :param value:
        '''
        self.vars[name] = value
    
    def update_var(self, name, value):
        '''
        Updates a variable in the symbol table
        :param name:
        :param value:
        '''
        self.check_var(name)
        self.vars[name] = value
    
    def var_exists(self, name):
        '''
        Checks if a variable exists in the symbol table
        :param name:
        '''
        return name in self.vars
    
    def check_var(self, name):
        if not self.var_exists(name):
            raise ValueError("var {0} does not exist".format(name))
        return True
            
    def add_func(self, module, internal_name, func_name, args):
        '''
        Add a new function in the symbol table. The function table is a dictionary with key-value pair as:
        [ <module_name, internal_name>, <func_name, parameters> ]
        :param module:
        :param internal_name:
        :param func_name:
        :param args:
        '''
        self.funcs[','.join([str(module), internal_name])] = [func_name, args]

    def get_var(self, name):
        '''
        Gets value of a variable
        :param name:
        '''
        self.check_var(name)
        return self.vars[name]
    
    def check_func(self, module, internal_name):
        key = ','.join([module, internal_name])
        if not key in self.funcs:
            raise ValueError("Function {0} does not exist".format(key))
        return True
    
    def get_func(self, module, internal_name):
        '''
        Gets function by key (module + internal_name)
        :param module:
        :param name:
        '''
        self.check_func(module, internal_name)
        return self.funcs[','.join([module, internal_name])]
    
    def get_funcs(self, module_name):
        '''
        Gets all functions in a module.
        :param module_name: Name of the module
        '''
        return [{k:v} for k,v in self.funcs.items() if k.split(',')[0] == module_name]     
    
    def __str__(self):
        '''
        A string representation of this table.
        '''
        display = ""
        if self.vars:
            sym_name = "Symbol Name"
            sym_len = max(max(len(i) for i in self.vars), len(sym_name))
            
            sym_value = "Value"
            value_len = max(max(len(str(v)) for i,v in self.vars.items()), len(sym_value))
    
            # print table header for vars
            display = "{0:3s} | {1:^{2}s} | {3:^{4}s}".format(" No", sym_name, sym_len, sym_value, value_len)
            display += ("\n-------------------" + "-" * (sym_len + value_len))
            # print symbol table
            i = 1
            for k, v in self.vars.items():
                display += "\n{0:3d} | {1:^{2}s} | {3:^{4}s}".format(i, k, sym_len, str(v), value_len)
                i += 1

        if self.funcs:
            mod_name = "Module name"
            mod_len = max(max(len(i.split(',')[0]) for i in self.funcs), len(mod_name))
         
            internal_name = "Internal Name"
            internal_len = max(max(len(i.split(',')[1]) for i in self.funcs), len(internal_name))
            
            func_name = "Function Name"
            func_len = max(max(len(v[0]) for i,v in self.funcs.items()), len(func_name))
           
            param_names = "Parameters"
            param_len = len(param_names)
            l = 0
            for k, a in self.funcs.items():
                for v in a[1]:
                    l += len(v)
                param_len = max(param_len, l)
            
            # print table header for vars
            display += "\n\n{0:3s} | {1:^{2}s} | {3:^{4}s} | {5:^{6}s} | {7:^{8}s}".format(" No", mod_name, mod_len, internal_name, internal_len, func_name, func_len, param_names, param_len)
            display += ("\n-------------------" + "-" * (mod_len + internal_len + func_len + param_len))
            # print symbol table
            i = 1
            for k, v in self.funcs.items():
                modfunc = k.split(',')
                
                parameters = ""
                for p in v[1]:
                    if parameters == "":
                        parameters = "{0}".format(p)
                    else:
                        parameters += ", {0}".format(p)
                display += "\n{0:3d} | {1:^{2}s} | {3:^{4}s} | {5:^{6}s} | {7:^{8}s}".format(i, modfunc[0], mod_len, modfunc[1], internal_len, v[0], func_len, parameters, param_len)
                i += 1
                
        return display
    
class Context:
    '''
    The context for parsing and interpretation.
    '''
    def __init__(self, library, tempdirs = {}):
        '''
        Initializes this object.
        :param tempdirs: {datasource_type: tempdir}
        '''
        self.library = library
        self.runnable = None
        self.user_id = None
        self.tempdirs = tempdirs
        self.reload()
    
    #mishuk
    def get_temp_dir(self, typename = "posix"):
        '''
        The user directory for a fs type is the temp directory.
        '''
        if self.user_id:
            if not typename in self.tempdirs:
                fs = FileSystem.FS_by_typename(typename)
                self.tempdirs[typename] = fs.make_unique_dir('Username')#User.query.get(self.user_id).username
            return self.tempdirs[typename]
        
    def reload(self):
        '''
        Reinitializes this object for new processing.
        '''
        self.symtab_stack = {}
        self.out = []
        self.err = []
        self.dci = []
        self.globals = {}
        self.ident = threading.get_ident()
        self.symtabs = [SymbolTable()]
        
    def get_var(self, name):
        '''
        Gets a variable from symbol stack and symbol table
        :param name:
        '''
        if threading.get_ident() in self.symtab_stack:
            for s in reversed(self.symtab_stack[threading.get_ident()]):
                if s.var_exists(name):
                    return s.get_var(name)
        for s in reversed(self.symtabs):
            if s.var_exists(name):
                return s.get_var(name)
    
    def add_var(self, name, value):
        if threading.get_ident() == self.ident:
            return self.symtabs[-1].add_var(name, value)
             
        if not threading.get_ident() in self.symtab_stack:
            self.symtab_stack[threading.get_ident()] = [SymbolTable()]
        return self.symtab_stack[threading.get_ident()][-1].add_var(name, value)
            
    def update_var(self, name, value):
        if threading.get_ident() in self.symtab_stack:
            for s in reversed(self.symtab_stack[threading.get_ident()]):
                if s.var_exists(name):
                    return s.update_var(name, value)
        if threading.get_ident() == self.ident:
            for s in reversed(self.symtabs):
                if s.var_exists(name):
                    return s.update_var(name, value)
    
    def add_or_update_var(self, name, value):
        if self.var_exists(name):
            return self.update_var(name, value)
        else:
            return self.add_var(name, value)
                                
    def var_exists(self, name):
        '''
        Checks if a variable exists in any of the symbol tables.
        :param name: variable name
        '''
        if threading.get_ident() in self.symtab_stack:
            for s in reversed(self.symtab_stack[threading.get_ident()]):
                if s.var_exists(name):
                    return True
                
        for s in reversed(self.symtabs):
            if s.var_exists(name):
                return True
    
    def append_local_symtab(self):
        '''
        Appends a new symbol table to the symbol table stack.
        '''
        if self.ident == threading.get_ident():
            self.symtabs.append(SymbolTable())
            return self.symtabs[-1]
            
        if threading.get_ident() in self.symtab_stack:
            self.symtab_stack[threading.get_ident()].append(SymbolTable())
        else:
            self.symtab_stack[threading.get_ident()] = [SymbolTable()]
        return self.symtab_stack[threading.get_ident()][len(self.symtab_stack[threading.get_ident()]) - 1]
    
    def pop_local_symtab(self):
        '''
        Pop a symbol table from the symbol table stack.
        '''
        if threading.get_ident() in self.symtab_stack:
            if self.symtab_stack[threading.get_ident()]:
                self.symtab_stack[threading.get_ident()].pop()
                if not self.symtab_stack[threading.get_ident()]: # no symbol table, remove the entry
                    del self.symtab_stack[threading.get_ident()]
        
        if self.ident == threading.get_ident():
            self.symtabs.pop()
        
#     def load_library(self, library_def_dir_or_file):
#         self.library = Library.load(library_def_dir_or_file)
     
    @property
    def library(self):
        return self.__library
         
    @library.setter
    def library(self, lib):
        self.__library = lib
        
    @property
    def runnable(self):
        return self.__runnable
    
    @runnable.setter
    def runnable(self, runnable):
        self.__runnable = runnable
                      
    def iequal(self, str1, str2):
        '''
        Compares two strings for case insensitive equality.
        :param str1:
        :param str2:
        '''
        if str1 == None:
            return str2 == None
        if str2 == None:
            return str1 == None
        return str1.lower() == str2.lower()
    
    def write(self, *args):
        '''
        Writes a line of strings in out context.
        '''
        self.out.append("{0}".format(', '.join(map(str, args))))
    
    def error(self, *args):
        '''
        Writes a line of strings in err context.
        '''
        self.err.append("{0}".format(', '.join(map(str, args))))

    def append_dci(self, server, user, password):
        self.dci.append([server, user, password])
    
    def pop_dci(self):
        if self.dci:
            return self.dci.pop()
    
    def get_activedci(self):
        if not self.dci:
            return [None, None, None]
        return self.dci[-1]