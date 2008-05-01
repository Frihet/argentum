#! /usr/bin/env python2.5

from __future__ import with_statement

import sqlalchemy, sqlalchemy.orm, elixir, sys


kws = dict([arg[2:].split('=', 1)
            for arg in sys.argv[1:]
            if arg.startswith('--') and '=' in arg])
options = set([arg[2:]
               for arg in sys.argv[1:]
               if arg.startswith('--') and '=' not in arg])
files = [arg
         for arg in sys.argv[1:]
         if not arg.startswith('--')]

model = None
if 'model' in kws:
    model = __import__(kws['model'])
    for item in kws['model'].split('.')[1:]:
        model = getattr(model, item)
else:
    options.add('help')
    
if 'help' in options:
    print """Usage: SetupDatabase.py --model=ORM.Model.Python.Module.Path OPTIONS
    Where OPTIONS are
        --drop
            Drop all database objects (tables, views etc). ALL DATA WILL BE LOST!
        --all
            Equivalent to --schema and --data
        --schema
            Create all tables and views
        --data
            Insert initial data into tables"""
    if model is None:
        print """        Other model specific arguments"""
    else:
        for key, value in getattr(model, 'initial_data_params', {}).iteritems():
            print """        --%s
            %s""" % (key, value)
    sys.exit(0)

if 'drop' in options:
    print "DROPPING ALL TABLES"
    elixir.drop_all(bind=model.engine)

if 'schema' in options or 'all' in options:
    print "CREATING TABLES"
    elixir.create_all(bind=model.engine)

if 'data' in options or 'all' in options:
    print "INSERTING ORIGINAL DATA"
    with model.engine.Session() as session:
        model.createInitialData(session, *options, **kws)
