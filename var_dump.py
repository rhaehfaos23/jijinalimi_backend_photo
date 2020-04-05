import copy
import builtins
from pprint import pprint


def vars_dump(cls):
    variables = copy.deepcopy(vars(cls))
    builtin_types = tuple([getattr(builtins, d) for d in dir(builtins) if isinstance(getattr(builtins, d), type)])
    for key, value in variables.items():
        if type(value) not in builtin_types:
            variables[key] = vars_dump(value)

    return variables


def print_vars(cls):
    pprint(vars_dump(cls))

