from sys import exit
import sys

from pyparsing import *

from grammar import PythonGrammar, VizSciFlowGrammar
from interpreter import Interpreter
 
class VizSciFlowParser(object):
    '''
    The parser for Bio-DSL.
    '''

    def __init__(self, grammar = None):
        self.grammar = grammar if grammar else VizSciFlowGrammar()
        self.tokens = ParseResults()
        self.err = []
    
    def error(self, *args):
        self.err.append("{0}".format(', '.join(map(str, args))))

    def parse(self, text):
        self.tokens = self.grammar.program.ignore(pythonStyleComment).parseString(text, parseAll=True)
        return self.tokens
    
    def parse_subgrammar(self, subgrammer, text):
        try:
            self.tokens = subgrammer.ignore(pythonStyleComment).parseString(text, parseAll=True)
            return self.tokens
        except ParseException as err:
            print(err)
            self.error(err)
        except Exception as err:
            print(err)
            self.error(err)

    def parse_file(self, filename):
        try:
            self.tokens = self.grammar.program.ignore(pythonStyleComment).parseFile(filename, parseAll=True)
            return self.tokens
        except ParseException as err:
            print(err)
            exit(3)
        except Exception as err:
            print(err)
            self.error(err)
        
if __name__ == "__main__":
    from externalInfo import Timer
    with Timer() as t:
        p = VizSciFlowParser(PythonGrammar())
        if len(sys.argv) > 1:
            tokens = p.parse_file(sys.argv[1])
        else:
            test_program_example = 'print(2+3)'
        test_program_example = 'print(2+3)'
        tokens = p.parse(test_program_example)
        #tokens = p.grammar.assignstmt.ignore(pythonStyleComment).parseString(test_program_example)
            
        print(tokens)
        print(tokens.asXML())
        integrator = Interpreter()
       # integrator = PhenoWLCodeGenerator()
        
        #integrator.context.load_library("libraries")
        integrator.run(tokens)
    print(integrator.context.library)
    print(integrator.context.out)
    print(integrator.context.err)
    #print(integrator.code)