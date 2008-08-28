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

import sqlalchemy, operator

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
    return reduce(interleave(operator.add, "|"),
                  (sqlalchemy.cast(col, sqlalchemy.Unicode(255))
                   for col in id_cols))
