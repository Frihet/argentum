#! /bin/env python
# -*- coding: utf-8 -*-
# vim: set fileencoding=utf-8 :

# Argentum SQLAlchemy extension
# Copyright (C) 2008 Egil Moeller <egil.moller@freecode.no>

# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
# General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307
# USA

import sqlalchemy, operator, elixir, sys

def nulliszero(value):
    return sqlalchemy.func.coalesce(value, 0.0)

def group_by_list(expr, *cols):
    """
    Applies all group_by for all columns in cols on expr.

    @param expr SQL Expression.
    @param *cols SQL Expression columns to group by.
    @return Grouped by SQL Expression.
    """
    for col in cols:
        expr = expr.group_by(col)
    return expr

def select_list(cols, *lst):
    """
    Construct union select.

    @param cols (name, type) list that select returns.
    @param *lst list of lists containing values for cols.
    @return Union SQL Expression.
    """

    res = [sqlalchemy.select([sqlalchemy.cast(sqlalchemy.literal(row[pos]), cols[pos][1]).label(cols[pos][0])
                              for pos in xrange(0, len(cols))])
           for row in lst]
    return sqlalchemy.union(*res)

def interleave(op, sep):
    def interleave(a, b):
        return op(op(a, sep), b)
    return interleave

def compound_id(*id_cols):
    return reduce(interleave(operator.add, sqlalchemy.literal("|")),
                  (sqlalchemy.cast(col, sqlalchemy.Unicode(255))
                   for col in id_cols))


def abstract_join(method, table, *clauses):
    for clause in clauses:
        if isinstance(clause, (list, tuple)):
            join_table = clause[0]
            clause = clause[1:]
            if len(clause) > 1:
                clause = [sqlalchemy.and_(*clause)]
            table = getattr(table, method)(join_table, clause[0])
        else:
            table = getattr(table, method)(clause)
    return table


def join(table, *clauses):
    return abstract_join("join", table, *clauses)

def outerjoin(table, *clauses):
    return abstract_join("outerjoin", table, *clauses)

def attributes_from_table(table, *attrs):
    res = []
    for attr in attrs:
        label = None
        if isinstance(attr, (list, tuple)):
            (attr, label) = attr
        attr = getattr(table.c, attr)
        if label:
            attr = attr.label(label)
        res.append(attr)
    return res


def make_class(module_name, name, base_clss, **members):
    members['__module__'] = module_name

    res = type(name, base_clss, members)
    setattr(sys.modules[module_name], name, res)
    return res

def sum_view(module_name, name, base_cls, id_cols, sum_output_cols, sum_cols, filter):
    base_table = base_cls.table.alias()
    base_cols = base_table.c.keys()
    
    cols = set(base_cols) - (set(['id', 'rowid_']) | set(sum_cols) | set(sum_output_cols))
    id_expr = compound_id(*[getattr(base_table.c, id_col) for id_col in id_cols])

    expression = group_by_list(
        sqlalchemy.select(
            [id_expr.label('rowid_'),
             id_expr.label('id')] +
            [getattr(base_table.c, col)
             for col in cols] +
            [sqlalchemy.func.sum(getattr(base_table.c, col)).label(col)
             for col in sum_output_cols],
            *filter(base_table)),
        *[getattr(base_table.c, col)
          for col in cols])

    # Handle foreign keys
    members = {}
    for sum_col in sum_cols:
        members[sum_col[:-3]] = None

    members['expression'] = expression

    return make_class(module_name, name, (base_cls,), **members)
