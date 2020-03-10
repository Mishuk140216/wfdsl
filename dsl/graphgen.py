import ast
import logging
import threading
import _thread
import json

from func_resolver import LibraryBase
from externalInfo import TaskManager
from context import Context
from grammar import PythonGrammar
from parser import VizSciFlowParser
from provenance import BioProv

from py2neo import Graph, Subgraph
from py2neo.data import Node, Relationship
from py2neo import RelationshipMatcher

logging.basicConfig(level=logging.DEBUG)

class GraphGenerator(object):
    '''
    The Bio-DSL graph generator
    '''
    def __init__(self, url, username, password):
        self.context = Context()
        self.line = 0
        self.graph = Graph(url, username=username, password=password)
        self.graph.schema.create_uniqueness_constraint('Workflow', 'id')
    
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
        if package is None and function in self.context.library.tasks:
            return self.context.library.run_task(function, v, self.dotaskstmt)

        funcInstances = self.context.library.get_function(function, package)
        if not funcInstances:
            raise ValueError(r"'{0}' doesn't exist.".format(function))
        
        node = Node("Service", package=package, name=function, wid=self.graph_id)
        for arg in v:
            if isinstance(arg, Node):
                self.graph.create(Relationship(arg, "INPUT", node))
            elif isinstance(arg, tuple):
                for a in arg:
                    self.graph.create(Relationship(a, "INPUT", node))
        
        outnodes = ()
        if funcInstances["returns"]:
            if isinstance(funcInstances["returns"], list):
                for r in funcInstances["returns"]:
                    outnode = Node('Data', type=r['type'], name=r['name'], wid=self.graph_id)
                    self.graph.create(Relationship(node, "OUTPUT", outnode))
                    outnodes += (outnode, )
            else:
                outnodes = Node('Data', type=funcInstances["returns"], name="output", wid=self.graph_id)
                self.graph.create(Relationship(node, "OUTPUT", outnodes))
                
        return outnodes

    def dorelexpr(self, expr):
        '''
        Executes relative expression.
        :param expr:
        '''
        left = self.eval(expr[0])
        right = self.eval(expr[2])
        
        return self.createbinarynode(left, right, expr[1])
    
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
        
        return self.createbinarynode(left, right, "and")
    
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
        self.run_multstmt(lambda: self.eval(expr))
    
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
        return self.createbinarynode(left, right, "or")
    
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
        return self.createbinarynode(left, right, expr[-2])
    
    def createbinarynode(self, left, right, name):
        node = Node("Operator", name=name, wid=self.graph_id)
        self.graph.create(Relationship(left, "INPUT", node))
        self.graph.create(Relationship(right, "INPUT", node))
        outnode = Node('Data', wid=self.graph_id)
        self.graph.create(Relationship(node, "OUTPUT", outnode))
        return outnode
            
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
        
        return self.createbinarynode(left, right, expr[-2])
    
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
            rightNode = self.eval(right)
            self.context.add_var(left[0], rightNode)
            
            #node = Node("Service", package="built-in", name=left[0]+"=")
            #self.graph.create(Relationship(rightNode, "=", node))
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
    
    def eval_value_node(self, typename, value):
        node = Node("Data", datatype=typename, name=value, wid=self.graph_id)
        #relationship = Relationship(self.workflow_node, "DATASET", node)
        #self.graph.create(relationship)
        self.graph.create(node)
        return node
    
    def eval_value(self, str_value):
        '''
        Evaluate a single expression for value.
        :param str_value:
        '''
        try:
            t = ast.literal_eval(str_value)
            if type(t) in [int, float, bool, complex]:
                if t in set((True, False)):
                    return self.eval_value_node('bool', bool(t))
                if type(t) is int:
                    return self.eval_value_node('int', int(t))
                if type(t) is float:
                    return self.eval_value_node('float', float(t))
                if type(t) is complex:
                    return self.eval_value_node('complex', complex(t))
            else:
                if len(str_value) > 1:
                    if (str_value.startswith("'") and str_value.endswith("'")) or (str_value.startswith('"') and str_value.endswith('"')):
                        return self.eval_value_node('str', str_value[1:-1]) 
            return str_value
        except ValueError:
            if self.context.var_exists(str_value):
                return self.context.get_var(str_value)
            return self.graph.create(Relationship(Node('Data', type(str_value), name=str_value, wid=self.graph_id), "DATASET", self.workflow_node))
    
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
    def get_params(self, expr):
        params = []
        for e in expr:
            param = self.eval(e)
#             if not isinstance(param, tuple):
#                 param = param, None
            params.append(param)
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

        params, kwparams = LibraryBase.split_args(self.get_params(expr[0]))
        
        symtab_added, dci_added = False, False
        try:
            local_symtab = self.context.append_local_symtab()
            symtab_added = True
            for k,v in kwparams.items():
                self.context.add_or_update_var(k, v)
            
            if args:
                arguments, kwargs = LibraryBase.split_args(args)
                for k, v in kwargs.items():
                    self.context.add_or_update_var(k, v)
                
                for index, param in enumerate(params, start = 0):
                    if index >= len(arguments):
                        break
                    self.context.add_or_update_var(k, v)
             
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
                
            if local_symtab.var_exists('provenance') and local_symtab.get_var('provenance'):
                prov = BioProv(lambda: self.eval(expr[1]))
                result = prov.run()
                return result[0].ref if result else None
            else:
                return self.eval(expr[1])
            
        finally:
            if dci_added:
                self.context.pop_dci()
            if symtab_added:
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
        elif expr[0] == "TASK":
            return self.dotaskdefstmt(expr[1:])
        else:
            val = []
            for subexpr in expr:
                val.append(self.eval(subexpr))
            return val

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
            self.context.err.append("Error at line {0}: {1}".format(self.line, err))
    
    def run_workflow(self, workflow):
        parser = VizSciFlowParser(PythonGrammar())
        prog = parser.parse(workflow.script)
        
        self.graph_id= str(workflow.id) +'#' + str(workflow.user_id)
        cypher = "MATCH (n) WHERE n.wid='{0}' DETACH DELETE n".format(self.graph_id)
        #self.graph.run(cypher)
        
#         self.workflow_node = self.graph.nodes.match("Workflow", wid=self.graph_id).first()
#         if self.workflow_node:
#             self.graph.delete(self.workflow_node)
        
        #MATCH (n) WHERE n.wid='358#2' DETACH DELETE n    
#         self.graph.create(Node("Workflow", wid=self.graph_id, name=workflow.name))
#         self.workflow_node = self.graph.nodes.match("Workflow", wid=self.graph_id).first()
#         if not self.workflow_node:
#             raise ValueError("Graph is not created")
        #self.run(prog)
        #wf_graph = self.graph.run("MATCH (w:Workflow)-[r*]->(x) WHERE w.id='{0}' RETURN *".format(graph_id))
        #cypher = "MATCH (a)-[r]->(b) WITH collect({ source: id(a), target: id(b), type: type(r) }) AS links RETURN links"
        #cypher = "MATCH(w:Workflow{wid:'{0}'})-[r*]-() WITH last(r) AS rx RETURN {source:startNode(rx), type: type(rx), target:endNode(rx)}".format(graph_id)
        #cypher = "MATCH(w:Workflow{wid:'" + graph_id + "'})-[r*]-() WITH last(r) AS rx RETURN {source:{label: labels(startNode(rx))[0], id: ID(startNode(rx))}, type: type(rx), target:{label: labels(endNode(rx))[0], id: ID(endNode(rx))}} AS links"
        #cypher = "MATCH(w:Workflow{wid:'" + self.graph_id + "'})-[r*]-() WITH last(r) AS rx RETURN {source:{label: labels(startNode(rx))[0], id: ID(startNode(rx)), name: startNode(rx).name}, type: type(rx), target:{label: labels(endNode(rx))[0], id: ID(endNode(rx)), name: endNode(rx).name}}"
        cypher = "MATCH (n) WHERE n.wid='{0}' return n".format(self.graph_id)
        self.graph.run(cypher)
        #self.run(prog)
        
        wf_graph = self.graph.run(cypher).data()
        links = []
        nodes = []
        for g in wf_graph:
            node = g['n']
            labels = list(node.labels)
            if 'Data' in labels:
                outrels = self.graph.run("MATCH (n)<-[:OUTPUT]-() WHERE ID(n)={0} return n".format(node.identity))
                if not outrels.data():
                    nodes.append({"key": node.identity, "type": "Data", "name": node.__name__})  
#                 relmatcher = RelationshipMatcher(self.graph)
#                 rels = relmatcher.match(node if isinstance(node, list) else (node,), r_type = 'OUTPUT')
#                 rels = list(rels)
            elif 'Service' in labels or 'Operator' in labels:
                label = 'Service' if 'Service' in labels else 'Operator' 
                nodes.append({"key": node.identity, "type": label, "name": node.__name__})
                outrels = self.graph.run("MATCH (n)<-[:INPUT]-(m) WHERE ID(n)={0} return m".format(node.identity))
                for rel in outrels.data():
                    # check till the service which generated the data, otherwise the data direct
                    prevdatanode = rel['m']
                    outfromprevservice = self.graph.run("MATCH (n)<-[:OUTPUT]-(s) WHERE ID(n)={0} return s".format(prevdatanode.identity))
                    for outservice in outfromprevservice.data():
                        prevdatanode = outservice['s']
                        break
                    
                    #links.append({ "from": prevdatanode.identity, "frompid": prevdatanode.__name__, "to": node.identity, "topid": node.__name__, "value": "Input"})               
                    links.append({ "from": prevdatanode.identity, "frompid": str(node.identity), "to": node.identity, "topid": str(prevdatanode.identity), "value": "Input"})
#                 relmatcher = RelationshipMatcher(self.graph)
#                 rels = relmatcher.match(node if isinstance(node, list) else [node], r_type = 'INPUT')
#                 rels = list(rels)
            
        return json.dumps({ "nodeDataArray" : nodes, "linkDataArray":links})