#! /usr/bin/env python2.5

from __future__ import with_statement

import sys, Argentum
import sqlalchemy, sqlalchemy.orm, elixir

def parse_options(argv):
    #  Parse options
    kws = dict([arg[2:].split('=', 1)
                for arg in argv
                if arg.startswith('--') and '=' in arg])
    options = set([arg[2:]
                   for arg in argv
                   if arg.startswith('--') and '=' not in arg])
    files = [arg
             for arg in argv
             if not arg.startswith('--')]

    return (kws, options, files)

def help(model):
    print """Usage: SetupDatabase.py --model=ORM.Model.Python.Module.Path OPTIONS
    Where OPTIONS are
        --drop
            Drop all database objects (tables, views etc). ALL DATA WILL BE LOST!
        --all
            Equivalent to --schema and --data
        --schema
            Create all tables and views
        --views
        --skip-materialized
            Do not (re)create materialized views
        --data
            Insert initial data into tables
        --sqllogging
            Print DDL statements"""
    if model is None:
        print """        Other model specific arguments"""
    else:
        for key, value in getattr(model, 'initial_data_params', {}).iteritems():
            print """        --%s
            %s""" % (key, value)
            
    sys.exit(0)

def load_model(options, kws, files):
    if 'model' not in kws:
        return None

    model = model = __import__(kws['model'])
    for item in kws['model'].split('.')[1:]:
        model = getattr(model, item)

    return model

def setup(options, kws, files, model = None):
    if model is None:
        model = load_model(options, kws, files)
    
    if 'drop' in options:
        print "DROPPING ALL TABLES"
        elixir.drop_all(bind=model.engine)

    if 'schema' in options or 'all' in options:
        print "CREATING TABLES"
        elixir.create_all(bind=model.engine)

    if 'views' in options:
        print "(RE)CREATING VIEWS"
        with model.engine.Session() as session:
            for view_method in elixir.metadata.ddl_listeners['after-create']:
                view = view_method.im_self
                if isinstance(view, Argentum.View):
                    if not 'skip-materialized' in options or not view.is_materialized:
                        view_method(None, elixir.metadata, session.bind)

    if 'data' in options or 'all' in options:
        print "INSERTING ORIGINAL DATA"
        with model.engine.Session() as session:
            model.createInitialData(session, *options, **kws)

if __name__ == '__main__':
    # Get options
    kws, options, files = parse_options(sys.argv[1:])

    model = load_model(options, kws, files)

    # Handle command line only options
    if 'help' in options or 'model' not in kws:
        help(model)

    if "sqllogging" in options:
        import logging
        logging.basicConfig()
        logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
        logging.getLogger('sqlalchemy.sql.compiler.IdentifierPreparer').setLevel(logging.INFO)

    setup(options, kws, files, model)
