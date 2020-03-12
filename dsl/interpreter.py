import ast
import logging
import threading
import _thread

from dsl.func_resolver import LibraryBase
from dsl.util import TaskManager
from dsl.util import FolderItem
from dsl.context import Context

logging.basicConfig(level=logging.DEBUG)

class Interpreter(object):
    '''
    The interpreter for PhenoWL DSL
    '''
    def __init__(self, context):
        self.context = context
        self.line = 0
    
    def get_args(self, expr):
        v = []
        for e in expr:
            v.append(self.eval(e))
        return v
        
    def dofunc(self, expr):
        '''
        Execute func expression.
        :param expr:
        '''
        function = expr[0] if len(expr) < 3 else expr[1]
        package = expr[0][:-1] if len(expr) > 2 else None
        
        params = expr[1] if len(expr) < 3 else expr[2]
        v = self.get_args(params)
        
        # call task if exists
#         if package is None and function in self.context.library.tasks:
#             return self.context.library.run_task(function, v, self.dotaskstmt)

        if not LibraryBase.check_function(function, package):
            raise Exception(r"Function '{0}' doesn't exist.".format(function))
            
        return self.context.library.call_func(self.context, package, function, v)
        #return LibraryBase.call_func(self.context, package, function, v)

    def dorelexpr(self, expr):
        '''
        Executes relative expression.
        :param expr:
        '''
        left = self.eval(expr[0])
        if len(expr) == 1:
            return bool(left)
        
        right = self.eval(expr[2])
        operator = expr[1]
        if operator == '<':
            return left < right
        elif operator == '>':
            return left > right
        elif operator == '<=':
            return left <= right
        elif operator == '>=':
            return left >= right
        else:
            return left == right
    
    def doand(self, expr):
        '''
        Executes "and" expression.
        :param expr:
        '''
        if not expr:
            return True
        right = self.eval(expr[-1])
        if len(expr) == 1:
            return right
        left = expr[:-2]
        if len(left) > 1:
            left = ['ANDEXPR'] + left
        left = self.eval(left)
        return left and right
    
    def donot(self, expr):
        '''
        Executes "and" expression.
        :param expr:
        '''
        if not expr:
            return True
#        if len(expr) >= 1:
#            raise ValueError("Invalid number of operands for not operator")
        left = expr[-1:]
        if len(left) > 1:
            left = ['NOTEXPR'] + left
        return not self.eval(left)
    
    def dopar(self, expr):
        taskManager = TaskManager() 
        for stmt in expr:
            taskManager.submit_func(self.dopar_stmt, stmt)
        taskManager.wait();
            
    def dopar_stmt(self, expr):
        '''
        Execute a for expression.
        :param expr:
        '''
#         from wfdsl import dsl
#         with dsl.app_context():
#             self.run_multstmt(lambda: self.eval(expr))
    
    def run_multstmt(self, f):
        self.context.append_local_symtab()
        try:
            f()
        finally:
            self.context.pop_local_symtab()
            
    def dolog(self, expr):
        '''
        Executes a logical expression.
        :param expr:
        '''
        right = self.eval(expr[-1])
        if len(expr) == 1:
            return right
        left = expr[:-2]
        if len(left) > 1:
            left = ['LOGEXPR'] + left
        left = self.eval(left)
        return left or right
    
    def domult(self, expr):
        '''
        Executes a multiplication/division operation
        :param expr:
        '''
        right = self.eval(expr[-1])
        if len(expr) == 1:
            return right
        left = expr[:-2]
        if len(left) > 1:
            left = ['MULTEXPR'] + left
        left = self.eval(left)
        return left / right if expr[-2] == '/' else left * right

    def doarithmetic(self, expr):
        '''
        Executes arithmetic operation.
        :param expr:
        '''
        right = self.eval(expr[-1])
        if len(expr) == 1:
            return right
        left = expr[:-2]
        if len(left) > 1:
            left = ['NUMEXPR'] + left
        left = self.eval(left)
        if isinstance(left, FolderItem) or isinstance(left, list) or isinstance(left, FolderItem):
            return FolderItem.union(left, right) if expr[-2] == '+' else FolderItem.substract(left, right)
        else:
            return left + right if expr[-2] == '+' else left - right
    
    def doif(self, expr):
        '''
        Executes if statement.
        :param expr:
        '''
        cond = self.eval(expr[0])
        if cond:
            self.eval(expr[1])
        elif len(expr) > 3 and expr[3]:
            self.eval(expr[3])
    
    def dolock(self, expr):
        if not self.context.symtab.var_exists(expr[0]) or not isinstance(self.context.symtab.get_var(expr[0]), _thread.RLock):
            self.context.symtab.add_var(expr[0], threading.RLock())    
        with self.context.symtab.get_var(expr[0]):
            self.eval(expr[1])
        pass

    def dotupexpr(self, expr):
        t = ()
        for e in expr:
            t += (self.eval(e),)
        return t
    
    def doassign_noeval(self, left, right):
        '''
        Evaluates an assignment expression.
        :param expr:
        '''
        if len(left) == 1:
            self.context.add_var(left[0], right)
        elif left[0] == 'LISTIDX':
            left = left[1]
            idx = self.eval(left[1])
            if self.context.var_exists(left[0]):
                v = self.context.get_var(left[0])
                if isinstance(v, list):
                    while len(v) <= idx:
                        v.append(None)
                    v[int(idx)] = right
                elif isinstance(v, dict):
                    v[idx] = right
                else:
                    raise ValueError("Not a list or dictionary")
            else:
                v = []
                while len(v) <= idx:
                    v.append(None)
                v[int(idx)] = right
                self.context.add_var(left[0], v)
                
    def doassign(self, left, right):
        '''
        Evaluates an assignment expression.
        :param expr:
        '''
        if len(left) == 1:
            self.context.add_var(self.eval(left), self.eval(right))
        elif left[0] == 'LISTIDX':
            left = left[1]
            idx = self.eval(left[1])
            if self.context.var_exists(left[0]):
                v = self.context.get_var(left[0])
                if isinstance(v, list):
                    while len(v) <= idx:
                        v.append(None)
                    v[int(idx)] = self.eval(right)
                elif isinstance(v, dict):
                    v[idx] = self.eval(right)
                else:
                    raise ValueError("Not a list or dictionary")
            else:
                v = []
                while len(v) <= idx:
                    v.append(None)
                v[int(idx)] = self.eval(right)
                self.context.add_var(left[0], v)
                
        elif left[0] == 'TUPASSIGN':
            right = self.eval(right)
            for i in range(1, len(left)):
                if left[i] != '_':
                    self.doassign_noeval(left[i], right[i - 1])

    def dofor(self, expr):
        '''
        Execute a for expression.
        :param expr:
        '''
        self.context.add_var(expr[0], None)
        for var in self.eval(expr[1]):
            self.context.update_var(expr[0], var)
            self.eval(expr[2])
    
    def eval_value(self, str_value):
        '''
        Evaluate a single expression for value.
        :param str_value:
        '''
        try:
            t = ast.literal_eval(str_value)
            if type(t) in [int, float, bool, complex]:
                if type(t) is int:
                    return int(str_value)
                if type(t) is float:
                    return float(t)
                if type(t) is complex:
                    return complex(t)
                if t in set((True, False)):
                    return bool(t)
            else:
                if len(str_value) > 1:
                    if (str_value.startswith("'") and str_value.endswith("'")) or (str_value.startswith('"') and str_value.endswith('"')):
                        return str_value[1:-1]
            return str_value
        except ValueError:
            if self.context.var_exists(str_value):
                return self.context.get_var(str_value)
            return str_value
    
    def dolist(self, expr):
        '''
        Executes a list operation.
        :param expr:
        '''
        v = []
        for e in expr:
            v.append(self.eval(e))
        return v
    
    def remove_single_item_list(self, expr):
        if not isinstance(expr, list):
            return expr
        if len(expr) == 1:
            return self.remove_single_item_list(expr[0])
        return expr
        
    def dodict(self, expr):
        '''
        Executes a list operation.
        :param expr:
        '''
        v = {}
        for e in expr:
            #e = self.remove_single_item_list(e)
            v[self.eval(e[0])] = self.eval(e[1])
        return v
    
    def dolistidx(self, expr):
        val = self.context.get_var(expr[0])
        return val[self.eval(expr[1])]
    
    def dostmt(self, expr):
        if len(expr) > 1:
            logging.debug("Processing line: {0}".format(expr[0]))
            self.line = int(expr[0])
            return self.eval(expr[1:])
    
    #===========================================================================
    # dotaskdefstmt
    # if task has no name, it will be called at once.
    # if task has a name, it will be called like a function call afterwards
    #===========================================================================
    def eval_params(self, expr):
        params = []
        for e in expr:
            if e[0] is "NAMEDARG":
                param = self.donamedarg(e[1])
                self.context.add_var(param[0], param[1])
            else:
                params.extend([ex for ex in e])
                
        return params
                        
    def dotaskdefstmt(self, expr):
        if not expr[0]:
            #v = self.get_args(expr[1])
            return self.dotaskstmt(expr[1:], None) # anonymous task; run immediately
        else:
            self.context.library.add_task(expr[0], expr)
    
    def args_to_symtab(self, expr):
        for e in expr:
            if e[0] is not "NAMEDARG":
                continue
            param = self.donamedarg(e[1])
            
            if isinstance(param, tuple):
                self.context.add_var(param[0], param[1])
                
    def dotaskstmt(self, expr, args):
        
        local_symtab = self.context.append_local_symtab()
        params = self.eval_params(expr[0])
        
        dci_added = False
        try:
            if args:
                arguments, kwargs = LibraryBase.split_args(args)
                for k, v in kwargs.items():
                    local_symtab.add_var(k, v)
                
                for index, param in enumerate(params, start = 0):
                    if index >= len(arguments):
                        break
                    local_symtab.add_var(param, arguments[index])
             
            if not local_symtab.var_exists('server'):
                local_symtab.add_var('server', None)
            if not local_symtab.var_exists('user'):
                local_symtab.add_var('user', None)
            if not local_symtab.var_exists('password'):
                local_symtab.add_var('password', None)
                    
            # if no new server name given, parent dci is used
            if local_symtab.get_var('server') is not None:
                self.context.append_dci(local_symtab.get_var('server'), local_symtab.get_var('user'), local_symtab.get_var('password'))
                dci_added = True
                
            return self.eval(expr[1])
            
        finally:
            if dci_added:
                self.context.pop_dci()
            self.context.pop_local_symtab()
                    
    
    def donamedarg(self, expr):
        name = expr[0]
        arg = expr[2]
        return str(name), self.eval(arg) 
                
    def eval(self, expr):        
        '''
        Evaluate an expression
        :param expr: The expression in AST tree form.
        '''
        if not isinstance(expr, list):
            return self.eval_value(expr)
        if not expr:
            return
        if len(expr) == 1:
            if expr[0] == "LISTEXPR":
                return list()
            elif expr[0] == "DICTEXPR":
                return dict()
            else:
                return self.eval(expr[0])
        if expr[0] == "TUPEXPR":
            return self.dotupexpr(expr[1:])
        if expr[0] == "FOR":
            return self.dofor(expr[1])
        elif expr[0] == "ASSIGN":
            return self.doassign(expr[1], expr[2])
        elif expr[0] == "CONST":
            return self.eval_value(expr[1])
        elif expr[0] == "NUMEXPR":
            return self.doarithmetic(expr[1:])
        elif expr[0] == "MULTEXPR":
            return self.domult(expr[1:])
        elif expr[0] == "CONCAT":
            return self.doarithmetic(expr[1:])
        elif expr[0] == "LOGEXPR":
            return self.dolog(expr[1:])
        elif expr[0] == "ANDEXPR":
            return self.doand(expr[1:])
        elif expr[0] == "NOTEXPR":
            return self.donot(expr[1:])
        elif expr[0] == "RELEXPR":
            return self.dorelexpr(expr[1:])
        elif expr[0] == "IF":
            return self.doif(expr[1])
        elif expr[0] == "LISTEXPR":
            return self.dolist(expr[1:])
        elif expr[0] == "DICTEXPR":
            return self.dodict(expr[1:])
        elif expr[0] == "FUNCCALL":
            return self.dofunc(expr[1])
        elif expr[0] == "LISTIDX":
            return self.dolistidx(expr[1])
        elif expr[0] == "PAR":
            return self.dopar(expr[1])
        elif expr[0] == "LOCK":
            return self.dolock(expr[1:])
        elif expr[0] == "STMT":
            return self.dostmt(expr[1:])
        elif expr[0] == "MULTISTMT":
            return self.eval(expr[2:])
        elif expr[0] == "NAMEDARG":
            return self.donamedarg(expr[1])
        elif expr[0] == "RETURN":
            return self.eval(expr[1])
        elif expr[0] == "TASK":
            return self.dotaskdefstmt(expr[1:])
        else:
            val = []
            for subexpr in expr:
                val.append(self.eval(subexpr))
            if val:
                return val[-1]

    # Run it
    def run(self, prog):
        '''
        Run a new program.
        :param prog: Pyparsing ParseResults
        '''
        try:
            #self.context.reload()
            stmt = prog.asList()
            self.eval(stmt)
        except Exception as err:
            raise ValueError("Error at line {0}: {1}".format(self.line, err))
