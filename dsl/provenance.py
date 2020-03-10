from externalInfo import Object, Module

class BioProv(Module):
    def body(self):
        eval_lambda = self.P[0]
        return (Object(eval_lambda()),)