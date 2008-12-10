#!/usr/bin/env python

import sys, types
import Argentum

def introspect_model(session, views, node):
    """
    Introspect model, traverse down and find other modules
    and views.
    """
    for child_name in dir(node):
        child = getattr(node, child_name)
        if isinstance(child,types.ModuleType) and child.__name__.find(node.__name__ + ".") == 0:
            introspect_model(session, views, child)
        elif hasattr(child, '__metaclass__') and child.__metaclass__ is Argentum.ViewEntityMeta:
            introspect_view(session, views, child)

def introspect_view(session, views, entity):
    # Skip already introspected elements
    view = entity.table
    if view in views:
        return

    print "Checking view", entity
    
    view.entity = entity
    #if not view.is_materialized:
    #    view.refresh(session.connection())

    # Get count
    try:
        orm_query = session.query(entity)
        if hasattr(entity.c, 'id'):
            orm_query = orm_query.order_by('id')
            order_by = ' order by id'
        else:
            order_by = ''
        

        count_orm = 0
        for row in orm_query.all():
            count_orm += 1    
        count_query = session.connection().execute('select count(*) from ' + view.get_name(session.connection()) + order_by).fetchone()[0]
    except:
        count_orm = -1
        count_query = -1

    views[view] = (count_query, count_orm)

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

def help():
    print """Usage: %s --model=ORM.Model.Python.Module.Path""" % (sys.argv[0], )
    sys.exit(1)

def main():
    # Parse options
    kws, options, files = parse_options(sys.argv[1:])
    if 'help' in options or 'model' not in kws:
        help()

    # Get model to introspect
    model = __import__(kws['model'])
    for item in kws['model'].split('.')[1:]:
        model = getattr(model, item)
    session = model.engine.Session()

    # Introspect model and fill in views with count_query, count_orm tuples.
    views = {}
    introspect_model(session, views, model)

    # Sort names, easier to see the results.
    views_sorted = views.keys()
    views_sorted.sort()

    # Display mismatching views
    for view in views_sorted:
        count_query, count_orm = views[view]
        if count_query != count_orm:
            print "VIEW MISMATCH: %s COUNT(*): %d ORM: %d" % (view.entity, count_query, count_orm)

    print "\n*******************************************************\n"

    # Display broken views
    for view in views_sorted:
        count_query, count_orm = views[view]
        if count_query == -1:
            print "VIEW BROKEN: %s COUNT(*): %d ORM: %d" % (view.entity, count_query, count_orm)

if __name__ == '__main__':
    main()
