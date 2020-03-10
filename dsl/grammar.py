from pyparsing import *
from pyparsing import _bslash

#
# global helpers
#
def delimitedMustOneList( expr, delim=",", combine=False ):
    """
    Helper to define a delimited list of expressions - the delimiter defaults to ','.
    By default, the list elements and delimiters can have intervening whitespace, and
    comments, but this can be overridden by passing C{combine=True} in the constructor.
    If C{combine} is set to C{True}, the matching tokens are returned as a single token
    string, with the delimiters included; otherwise, the matching tokens are returned
    as a list of tokens, with the delimiters suppressed.

    Example::
        delimitedList(Word(alphas)).parseString("aa,bb,cc") # -> ['aa', 'bb', 'cc']
        delimitedList(Word(hexnums), delim=':', combine=True).parseString("AA:BB:CC:DD:EE") # -> ['AA:BB:CC:DD:EE']
    """
    dlName = str(expr)+" ["+ str(delim)+" "+ str(expr)+"]..."
    if combine:
        return Combine( expr + Suppress( delim ) + ZeroOrMore( expr ) ).setName(dlName)
    else:
        return ( expr + Suppress( delim ) + ZeroOrMore(expr ) ).setName(dlName)
    
def myIndentedBlock(blockStatementExpr, indentStack, indent=True):
    '''
    Modifies the pyparsing indentedBlock to build the AST correctly
    '''
    def checkPeerIndent(s,l,t):
        if l >= len(s): return
        curCol = col(l,s)
        if curCol != indentStack[-1]:
            if curCol > indentStack[-1]:
                raise ParseFatalException(s,l,"illegal nesting")
            raise ParseException(s,l,"not a peer entry")

    def checkSubIndent(s,l,t):
        curCol = col(l,s)
        if curCol > indentStack[-1]:
            indentStack.append( curCol )
        else:
            raise ParseException(s,l,"not a subentry")

    def checkUnindent(s,l,t):
        if l >= len(s): return
        curCol = col(l,s)
        if not(indentStack and curCol < indentStack[-1] and curCol <= indentStack[-2]):
            raise ParseException(s,l,"not an unindent")
#        indentStack.pop()

    NL = OneOrMore(LineEnd().setWhitespaceChars("\t ").suppress())
    INDENT = (Empty() + Empty().setParseAction(checkSubIndent)).setName('INDENT')
    PEER   = Empty().setParseAction(checkPeerIndent).setName('')
    UNDENT = Empty().setParseAction(checkUnindent).setName('UNINDENT')
    if indent:
        smExpr = Group( Optional(NL) +
            #~ FollowedBy(blockStatementExpr) +
            INDENT + (OneOrMore( PEER + Group(blockStatementExpr) + Optional(NL) )) + UNDENT)
    else:
        smExpr = Group( Optional(NL) +
            (OneOrMore( PEER + Group(blockStatementExpr) + Optional(NL) )) )
    blockStatementExpr.ignore(_bslash + LineEnd())
    return smExpr.setName('indented block')

class BasicGrammar():
    '''
    The base grammar for PhenoWL parser.
    '''
    RELATIONAL_OPERATORS = "< > <= >= == !=".split()
    def __init__(self):
        self.build_grammar()
    
    def build_grammar(self):
        
        self.identifier = Word(alphas + "_", alphanums + "_")
        
        point = Literal('.')
        e = CaselessLiteral('E')
        plusorminus = Literal('+') | Literal('-')

        self.number = Word(nums)
        self.integer = Combine(Optional(plusorminus) + self.number).setParseAction(lambda x : x[0])
        self.floatnumber = Combine(self.integer + Optional(point + Optional(self.number)) + Optional(e + self.integer)).setParseAction(lambda x : x[0])
        self.string = quotedString.setParseAction(lambda x : x[0])
        self.constant = Group((self.floatnumber | self.integer | self.string).setParseAction(lambda t : ['CONST'] + [t[0]]))
        self.relop = oneOf(BasicGrammar.RELATIONAL_OPERATORS)
        self.multop = oneOf("* /")
        self.addop = oneOf("+ -")

        # Definitions of rules for numeric expressions
        self.expr = Forward()
        self.multexpr = Forward()
        self.numexpr = Forward()
        self.arguments = Forward()
        self.stringaddexpr = Forward()
        modpref = Combine(OneOrMore(self.identifier + Literal(".")))
        self.funccall = Group((Optional(modpref) + self.identifier + FollowedBy("(")) + 
                              Group(Suppress("(") + Optional(self.arguments) + Suppress(")"))).setParseAction(lambda t : ['FUNCCALL'] + t.asList())
        self.listidx = Group(self.identifier + Suppress("[") + self.expr + Suppress("]")).setParseAction(lambda t : ['LISTIDX'] + t.asList())
        self.dictdecl = Forward()
        self.listdecl = Forward() 
        
        pi = CaselessKeyword( "PI" )
        fnumber = Regex(r"[+-]?\d+(?:\.\d*)?(?:[eE][+-]?\d+)?")
        self.lpar, self.rpar = map(Suppress, "()")
        expop = Literal( "**" )
        
        parexpr = self.lpar + self.expr + self.rpar
        atom = (( pi | e | fnumber | self.string | self.identifier + parexpr | self.identifier) | Group(parexpr))
        
        # by defining exponentiation as "atom [ ^ factor ]..." instead of "atom [ ^ atom ]...", we get right-to-left exponents, instead of left-to-righ
        # that is, 2^3^2 = 2^(3^2), not (2^3)^2.
        factor = Forward()
        factor << atom + ZeroOrMore(expop + factor )
        
        self.multexpr << Group((factor + ZeroOrMore(self.multop + factor)).setParseAction(lambda t: ['MULTEXPR'] + t.asList()))
        self.numexpr << Group((self.multexpr + ZeroOrMore(self.addop + self.multexpr)).setParseAction(lambda t: ['NUMEXPR'] + t.asList()))
        self.stringaddexpr << Group((self.string + ZeroOrMore(Literal("+") + (self.identifier | self.string))).setParseAction(lambda t: ['CONCAT'] + t.asList()))
                
        self.expr << (self.stringaddexpr | self.string | self.funccall | self.listidx | self.listdecl | self.dictdecl | self.numexpr).setParseAction(lambda x : x.asList())
        
        self.namedarg = Group(self.identifier + Literal("=") + Group(self.expr)).setParseAction(lambda t: ['NAMEDARG'] + t.asList())
        self.arguments << delimitedList(Group(self.namedarg | self.expr))
        
        self.params = delimitedList(Group(self.namedarg | self.identifier))
        
        self.exprexpr = Forward()
        self.exprexpr << (self.stringaddexpr | self.string | self.funccall | self.listidx | self.numexpr)#.setParseAction(lambda x : x.asList())
        
        # Definitions of rules for logical expressions (these are without parenthesis support)
        self.relexpr = Group((Suppress(Optional(self.lpar)) + Group(self.exprexpr) + Optional(self.relop + Group(self.exprexpr)) + Suppress(Optional(self.rpar))).setParseAction(lambda t: ['RELEXPR'] + t.asList()))
        
        self.andexpr = Forward()
        self.andexpr << Group((self.relexpr("exp") + ZeroOrMore(Keyword("and") + self.relexpr("exp"))).setParseAction(lambda t : ["ANDEXPR"] + t.asList()))

        self.notexpr = Forward() 
        self.notexpr << Group((Keyword("not") + self.relexpr("and")).setParseAction(lambda t : ["NOTEXPR"] + t.asList()))
        
        self.logexpr = Forward()
        self.logexpr << Group(( ZeroOrMore(self.notexpr) +  ZeroOrMore(self.andexpr("exp") + ZeroOrMore(Keyword("or") + self.andexpr("exp"))) ).setParseAction(lambda t : ["LOGEXPR"] + t.asList()))
        
        
        #Group(self.andexpr("exp") + ZeroOrMore(Keyword("or") + self.andexpr("exp"))).setParseAction(lambda t : ["LOGEXPR"] + t.asList())

        # Definitions of rules for statements
        self.stmt = Forward()
        self.stmtlist = Forward()
        self.retstmt = (Suppress(Keyword("return")) + self.expr("exp")).setParseAction(lambda t: ["RETURN"] + t.asList())
#       
        self.listdecl << (Suppress("[") + Optional(delimitedList(Group(self.expr))) + Suppress("]")).setParseAction(lambda t: ["LISTEXPR"] + t.asList())
        self.dictdecl << (Suppress("{") + Optional(delimitedList(Group(self.expr + Suppress(Literal(":")) + Group(self.dictdecl | self.expr)))) + Suppress("}")).setParseAction(lambda t: ["DICTEXPR"] + t.asList())
        
        self.tupdecl = Forward()
        self.tupdecl << (delimitedMustOneList(Group(self.expr))).setParseAction(lambda t: ["TUPEXPR"] + t.asList())
        
        self.tupassign = Forward()
        self.tupassign << (delimitedMustOneList(Group(self.identifier))).setParseAction(lambda t: ["TUPASSIGN"] + t.asList())
        self.assignstmt = (Group(self.tupassign | self.listidx | self.identifier) + Suppress(Literal("=")) + Group(self.tupdecl | self.expr | self.listidx | self.listdecl | self.dictdecl)).setParseAction(lambda t: ['ASSIGN'] + t.asList())
        
        self.funccallstmt = self.funccall
        
    def build_program(self):
        self.stmt << Group((self.taskdefstmt | self.parstmt | self.retstmt | self.ifstmt | self.forstmt | self.lockstmt | self.funccallstmt | self.assignstmt | self.expr).setParseAction(lambda s,l,t :  ['STMT'] + [lineno(l, s)] + [t]))
        self.stmtlist << ZeroOrMore(self.stmt)
        self.program = self.stmtlist

class PythonGrammar(BasicGrammar):
    '''
    A Python style grammar.
    '''
    
    def __init__(self):
        self.build_grammar()
    
    def parseCompoundStmt(self, s, l, t):
        expr = ["MULTISTMT"] + [list(self.indentStack)] + t.asList()
        self.indentStack.pop()
        return expr
    
    def build_grammar(self):
        super().build_grammar()
        
        self.indentStack = [1]
        self.compoundstmt = Group(myIndentedBlock(self.stmt, self.indentStack, True).setParseAction(self.parseCompoundStmt))
        self.ifstmt = Group(Suppress("if") + self.logexpr  + Suppress(":") + self.compoundstmt + Optional((Suppress("else") + Suppress(":") + self.compoundstmt).setParseAction(lambda t : ['ELSE'] + t.asList()))).setParseAction(lambda t : ['IF'] + t.asList())
        self.forstmt = Group(Suppress("for") + self.identifier("var") + Suppress("in") + Group(self.expr("range"))  + Suppress(":") + self.compoundstmt).setParseAction(lambda t : ['FOR'] + t.asList())
        self.parstmt = Group(Suppress("parallel") + Suppress(":") + self.compoundstmt + OneOrMore(Suppress("with:") + self.compoundstmt)).setParseAction(lambda t : ['PAR'] + t.asList())
        self.lockstmt = (Suppress("lock") + Suppress(self.lpar) + self.identifier + Suppress(self.rpar) + Suppress(":") + self.compoundstmt).setParseAction(lambda t : ['LOCK'] + t.asList())
        self.taskdefstmt = (Suppress("task") + Optional(self.identifier, None) + Suppress("(")  + Group(Optional(self.params)) + Suppress(")") + Suppress(":") + self.compoundstmt).setParseAction(lambda t : ['TASK'] + t.asList())         
        super().build_program()                                 
                                 
class VizSciFlowGrammar(BasicGrammar):
    '''
    The PhenoWL grammar.
    '''
    
    def __init__(self):
        self.build_grammar()
    
    def build_grammar(self):
        super().build_grammar()
        
        self.compoundstmt = Group(Suppress("{") + self.stmtlist + Suppress("}"))
        self.ifstmt = Group(Keyword("if") + self.logexpr + self.compoundstmt + Group(Optional(Keyword("else") + self.compoundstmt)).setParseAction(lambda t : ['ELSE'] + t.asList())).setParseAction(lambda t : ['IF'] + t.asList())
        self.forstmt = Group(Keyword("for") + self.identifier("var") + Keyword("in") + Group(self.expr("range")) + self.compoundstmt).setParseAction(lambda t : ['FOR'] + t.asList())                                 
        self.parstmt = Group(Keyword("parallel") + self.compoundstmt).setParseAction(lambda t : ['PAR'] + t.asList())
                                 
        super().build_program()