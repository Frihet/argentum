#!/usr/bin/env python

import sys, types
import Argentum, sqlalchemy

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
    print """Usage: %s --model=ORM.Model.Python.Module.Path --table=ORM.Table.Class.Python.Module.Path""" % (sys.argv[0], )
    sys.exit(1)

def print_row(row, indent=''):
    print indent + ', '.join("%s=%s" % (key, value)
                             for (key, value) in row)

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
    conn = session.connection()

    def load_mod(cls):
        while True:
            try:
                return __import__(cls)
            except:
                if '.' not in cls:
                    raise
                cls = cls.rsplit('.', 1)[0]

    tbl = load_mod(kws['table'])
    for item in kws['table'].split('.')[1:]:
        tbl = getattr(tbl, item)

    counts = sqlalchemy.select([tbl.table.c.id, sqlalchemy.func.count(tbl.table.c.id).label('count')]).group_by(tbl.table.c.id)
    rowids = conn.execute(sqlalchemy.select([counts.c.id], counts.c.count > 1))
    for rowid in rowids:
        print "Rowid:", rowid[0]
        res = conn.execute(sqlalchemy.select([tbl.table], tbl.table.c.id == rowid[0]))
        last_row = None
        for row in res:
            row = zip(res.keys, row)
            if not last_row:
                print_row(row, '    ')
            else:
                print_row(((name, value)
                           for ((last_name, last_value), (name, value))
                           in zip(last_row, row)
                           if value != last_value), '    ')
            last_row = row

if __name__ == '__main__':
    main()
