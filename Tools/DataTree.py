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

hide_children = []
if 'hide-children' in kws:
    hide_children = [getModelPath(path) for path in kws['hide-children'].split(',')]

hide_attributes = []
if 'hide-attributes' in kws:
    hide_attributes = [getModelPath(path) for path in kws['hide-attributes'].split(',')]

compact_attributes = []
if 'compact-attributes' in kws:
    compact_attributes = kws['compact-attributes'].split(',')

if 'help' in options:
    print """Usage: DataTree.py --model=MODEL --initial=INITIAL OPTIONS
    Where MODEL  is the python path to the ORM Model to query.
    Where INITIAL is the python path to the initial object to query.
    Where OPTIONS are
        --hide=python.class.path,python.class.path,...
            Hide all nodes _beneath_ this class and all attributes of this class
        --hide-children=python.class.path,python.class.path,...
            Hide all nodes _beneath_ this class
        --hide-attributes=python.class.path,python.class.path,...
            Hide attributes of this class
        --compact
            Compact rendering (of e.g. revisited nodes)
        --compact-attributes=attribute_name,attribute_name
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

        extra = ""

        if 'compact' in options and id(obj) in done:
            extra = " Reoccured: See above for details"
        else:
            if type(obj) not in hide_attributes and type(obj) not in hide:
                extra = "\n%s| %s" % (indent,
                                      ', '.join(["%s=%s" % (name, getattr(obj, name))
                                                 for name in local]))
            else:
                extra = " %s" % (', '.join(("%s=%s" % (name, getattr(obj, name))
                                            for name in local
                                            if name in compact_attributes)),)
            if id(obj) in done:
                extra += "\n%s+%s" % (indent, " Reoccured: See above for details")
        
        print "%s%s.%s @ %s%s" % (indent,
                                  type(obj).__module__,
                                  type(obj).__name__,
                                  id(obj),
                                  extra)

        if id(obj) not in done and type(obj) not in hide_children and type(obj) not in hide:
            done.add(id(obj))
            for name in foreign:
                sub = getattr(obj, name)
                if obj.column_is_scalar(name):
                    if sub is None:
                        print indent + '| ' + name + "=NULL"
                    else:
                        print indent + '| ' + name
                        visit(sub, done, indent + '|   ')
                else:
                    if sub is None:
                        print indent + '| ' + name + "[] = NULL"
                    else:
                        print indent + '| ' + name + "[]"
                        for sub_part in sub:
                            visit(sub_part, done, indent + '| | ')
    except Exception, e:
        print indent + str(e)

with model.engine.Session() as session:
    for obj in session.query(initial).all():
        visit(obj)
