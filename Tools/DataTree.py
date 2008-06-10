#! /usr/bin/env python2.5

from __future__ import with_statement
import sys

#  Parse options
kws = dict([arg[2:].split('=', 1)
            for arg in sys.argv[1:]
            if arg.startswith('--') and '=' in arg])
options = set([arg[2:]
               for arg in sys.argv[1:]
               if arg.startswith('--') and '=' not in arg])
files = [arg
         for arg in sys.argv[1:]
         if not arg.startswith('--')]

def getModelPath(path):
    model = __import__(kws['model'])
    for item in path.split('.')[1:]:
        model = getattr(model, item)
    return model

model = None
if 'model' in kws:
    model = getModelPath(kws['model'])
else:
    options.add('help')

initial = None
if 'initial' in kws:
    initial = getModelPath(kws['initial'])
else:
    options.add('help')

hide = []
if 'hide' in kws:
    hide = [getModelPath(path) for path in kws['hide'].split(',')]

if 'help' in options:
    print """Usage: DataTree.py --model=MODEL --initial=INITIAL OPTIONS
    Where MODEL  is the python path to the ORM Model to query.
    Where INITIAL is the python path to the initial object to query.
    Where OPTIONS are
        --hide=python.class.path,python.class.path,...
    """
    if model is None:
        print """        Other model specific arguments"""
    else:
        for key, value in getattr(model, 'initial_data_params', {}).iteritems():
            print """        --%s
            %s""" % (key, value)
    sys.exit(0)

if "sqllogging" in options:
    import logging
    logging.basicConfig()
    logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

import sqlalchemy, sqlalchemy.orm, elixir

def visit(obj, done = set(), indent = ''):
    try:
        local = []
        foreign = []
        for name in obj.get_column_names():
            if obj.column_is_foreign(name):
                foreign.append(name)
            else:
                local.append(name)
        local.sort()
        foreign.sort()
        print "%s%s.%s @ %s" % (indent,
                                type(obj).__module__,
                                type(obj).__name__,
                                id(obj))
        print "%s| %s" % (indent,
                        ', '.join(["%s=%s" % (name, getattr(obj, name))
                                   for name in local]))
        if id(obj) in done:
            print indent + "+ See above for details"
        elif type(obj) not in hide:
            done.add(id(obj))
            for name in foreign:
                sub = getattr(obj, name)
                if obj.column_is_scalar(name):
                    print indent + '| ' + name
                    if sub is None:
                        print indent + '| + ' + "NULL"
                    else:
                        visit(sub, done, indent + '|   ')
                else:
                    print indent + '| ' + name + "[]"
                    if sub is None:
                        print indent + '| + ' + "NULL"
                    else:
                        for sub_part in sub:
                            visit(sub_part, done, indent + '| | ')
    except Exception, e:
        print indent + str(e)

with model.engine.Session() as session:
    for obj in session.query(initial).all():
        visit(obj)
