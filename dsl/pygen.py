import logging
import threading
import _thread
from dsl.context import Context


class CodeGenerator(object):
    '''
    The code generator for PhenoWL DSL
    '''
    def __init__(self):
        self.context = Context()
        self.code = ''
        self.imports = set()
        self.indent = 0

    def get_params(self, expr):
        v = []
        for e in expr:
            v.append(self.eval(e))
        return v
            
    def indent_stmt(self, str):
        return " " * self.indent + str
    
    def dofunc(self, expr):
        '''
        Execute func expression.
        :param expr:
        '''
        function = expr[0] if len(expr) < 3 else expr[1]
        package = expr[0][:-1] if len(expr) > 2 else None
        
        params = expr[1] if len(expr) < 3 else expr[2]
        v = self.get_params(params)
        
        # call task if exists
        if package is None and function in self.context.library.tasks:
            return self.context.library.code_run_task(function, v, self.dotaskstmt)

        if not self.context.library.check_function(function, package):
            raise Exception(r"'{0}' doesn't exist.".format(function))
            
        return self.context.library.code_func(self.context, package, function, v)
    
    def dorelexpr(self, expr):
        '''
        Executes relative expression.
        :param expr:
        '''
        left = self.eval(expr[0])
        right = self.eval(expr[2])
        operator = expr[1]
        if operator == '<':
            return "{0} < {1}".format(str(left), str(right))
        elif operator == '>':
            return "{0} > {1}".format(str(left), str(right))
        elif operator == '<=':
            return "{0} <= {1}".format(str(left), str(right))
        elif operator == '>=':
            return "{0} >= {1}".format(str(left), str(right))
        else:
            return "{0} == {1}".format(str(left), str(right))
    
    def doand(self, expr):
        '''
        Executes "and" expression.
        :param expr:
        '''
        if expr is None:#if expr is empty:
            return True
        right = self.eval(expr[-1])
        if len(expr) == 1:
            return right
        left = expr[:-2]
        if len(left) > 1:
            left = ['ANDEXPR'] + left
        left = self.eval(left)
        return "{0} and {1}".format(str(left), str(right))
    
    def dopar(self, expr):
        code = 'taskManager = TaskManager()\n'
#         for stmt in expr:
#             code += 'taskManager.submit_func(lambda: ' + self.eval(stmt) + ')\n'
        return code
    
    def dopar_stmt(self, expr):
        '''
        Execute a parallel expression.
        :param expr:
        '''
        self.run_multstmt(lambda: self.eval(expr))
    
    def run_multstmt(self, f):
        return f()

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
        return "{0} or {1}".format(str(left), str(right))
    
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
        return "{0} / {1}".format(str(left), str(right)) if expr[-2] == '/' else "{0} * {1}".format(str(left), str(right))

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
        return "{0} + {1}".format(str(left), str(right)) if expr[-2] == '+' else "{0} - {1}".format(str(left), str(right))
    
    def doif(self, expr):
        '''
        Executes if statement.
        :param expr:
        '''
        code = "if " + self.eval(expr[0]) + ":\n"
        code += self.run_multstmt(lambda: self.eval(expr[1]))
        if len(expr) > 3:
            code += "else:\n"
            code += self.run_multstmt(lambda: self.eval(expr[3]))
        return code
    
    def dolock(self, expr):
        if not self.context.symtab.var_exists(expr[0]) or not isinstance(self.context.symtab.get_var(expr[0]), _thread.RLock):
            self.context.symtab.add_var(expr[0], threading.RLock())    
        with self.context.symtab.get_var(expr[0]):
            self.eval(expr[1])
        pass
        
    def doassign(self, expr):
        '''
        Evaluates an assignment expression.
        :param expr:
        '''
        return "{0} = {1}".format(expr[0], self.eval(expr[1]))
        
    def dofor(self, expr):
        '''
        Execute a for expression.
        :param expr:
        '''
        code = "for {0} in {1}:\n".format(self.eval(expr[0]), self.eval(expr[1]))
        code += self.run_multstmt(lambda: self.eval(expr[2]))
        return code
    
    def eval_value(self, str_value):
        '''
        Evaluate a single expression for value.
        :param str_value:
        '''
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
    
    def dolistidx(self, expr):
        val = self.context.get_var(expr[0])
        return val[self.eval(expr[1])]
    
    def dostmt(self, expr):
        if len(expr) > 1:
            logging.debug("Processing line: {0}".format(expr[0]))
            self.line = int(expr[0])
            return self.indent_stmt(self.eval(expr[1:])) + '\n'

    def dotaskdefstmt(self, expr):
        if not expr[0]:
            v = self.get_params(expr[1])
            return self.dotaskstmt(expr, v)
        else:
            self.context.library.add_task(expr[0], expr)
            return ''
    
    def dotaskstmt(self, expr, args):
        server = args[0] if len(args) > 0 else None
        user = args[1] if len(args) > 1 else None
        password = args[2] if len(args) > 2 else None
        
        if not server:
            server = self.eval(expr[1][0]) if len(expr[1]) > 0 else None
        if not user:
            user = self.eval(expr[1][1]) if len(expr[1]) > 1 else None
        if not password:
            password = self.eval(expr[1][2]) if len(expr[1]) > 2 else None
        
        try:
            self.context.append_dci(server, user, password)
            return 'if True:\n' + self.eval(expr[2])
        finally:
            self.context.pop_dci()
                    
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
            return self.eval(expr[0])
        if expr[0] == "FOR":
            return self.dofor(expr[1])
        elif expr[0] == "ASSIGN":
            return self.doassign(expr[1:])
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
        elif expr[0] == "RELEXPR":
            return self.dorelexpr(expr[1:])
        elif expr[0] == "IF":
            return self.doif(expr[1])
        elif expr[0] == "LIST":
            return self.dolist(expr[1])
        elif expr[0] == "FUNCCALL":
            code, imports = self.dofunc(expr[1])
            self.imports.update(imports)
            return code
        elif expr[0] == "LISTIDX":
            return self.dolistidx(expr[1])
        elif expr[0] == "PAR":
            return self.dopar(expr[1])
        elif expr[0] == "LOCK":
            return self.dolock(expr[1:])
        elif expr[0] == "STMT":
            return self.dostmt(expr[1:])
        elif expr[0] == "TASK":
            return self.dotaskdefstmt(expr[1:])
        elif expr[0] == "MULTISTMT":
            try:
                self.indent = int(expr[1].pop()) - 1
                return self.eval(expr[2:])
            finally:
                self.indent = int(expr[1].pop()) - 1
        else:
            code = ''
            for subexpr in expr:
                code += self.eval(subexpr)
            return code

    # Run it
    def run(self, prog):
        '''
        Run a new program.
        :param prog: Pyparsing ParseResults
        '''
        try:
            self.context.reload()
            stmt = prog.asList()
            code = self.eval(stmt)
            imports = ''
            for i in self.imports:
                imports = i + '\n';
            self.context.out = imports + '\n' + code 
        except Exception as err:
            self.context.err.append("Error at line {0}: {1}".format(self.line, err))
