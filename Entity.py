#! /bin/env python
# -*- coding: utf-8 -*-
# vim: set fileencoding=utf-8 :

# Argentum SQLAlchemy extension
# Copyright (C) 2008 Egil Moeller <egil.moller@freecode.no>
# Copyright (C) 2008 FreeCode AS, Egil Moeller <egil.moller@freecode.no>

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

# Some parts developed as part of the CliqueClique project and
# extracted from there.

# Some parts developed as part of the Worm project and extracted from
# there.


import sqlalchemy, sqlalchemy.sql, sqlalchemy.orm, elixir

True_ = sqlalchemy.sql.text("(1 = 1)")
False_ = sqlalchemy.sql.text("(1 = 2)")

class View(sqlalchemy.schema.SchemaItem, sqlalchemy.sql.expression.TableClause):
    __visit_name__ = 'table'

    def __init__(self, name, metadata, expression, primary_key = None, **kw):
        super(View, self).__init__(name, **kw)
        metadata.append_ddl_listener('after-create', self.create)
        metadata.append_ddl_listener('before-drop', self.drop)
        self.metadata = metadata
        
        self._expression = expression

        attributes_to_copy = ("nullable",
                              "default",
                              "_is_oid",
                              "index",
                              "unique",
                              "autoincrement",
                              "quote")
        if primary_key is None:
            attributes_to_copy += ('primary_key')

        for key in  self._expression.columns.keys():
            column = self._expression.columns[key]
            copy = sqlalchemy.schema.Column(key,
                                            column.type,
                                            *(  [constraint.copy() for constraint in column.constraints]
                                                + [foreign_key.copy() for foreign_key in column.foreign_keys]),
                                            **dict([(col_name, getattr(column, col_name))
                                                    for col_name in attributes_to_copy]))
            copy._set_parent(self)

        if primary_key is not None:
            if not isinstance(primary_key, (str, unicode)):
                # Get the name of the primary key column obect
                primary_key = primary_key.comparator.prop.key
            self.columns[primary_key].primary_key = True
            self._primary_key = sqlalchemy.sql.expression.ColumnCollection(self.columns[primary_key])

    def create(self, event, metadata, bind):
        try:
            self.drop(event, metadata, bind)
        except:
            pass
        
        select = self._expression.compile(bind = bind)
        params = select.construct_params()

        bind.execute("create view %(name)s as %(select)s" %
                     {'name': self.name,
                      'select': select},
                     params)

        # FIXME: sqlalchemy.schema.DDL unescapes its parameters
#         sqlalchemy.schema.DDL("create view %(name)s as %(select)s" %
#                               {'name': self.name,
#                                'select': select},
#                               context=params).execute(bind)
                              
    def drop(self, event, metadata, bind):
        bind.execute("drop view %(name)s" %
                     {'name': self.name})

        # FIXME: sqlalchemy.schema.DDL unescapes its parameters
#         sqlalchemy.schema.DDL("drop view %(name)s" %
#                               {'name': self.name}).execute(bind)

class ViewEntityMeta(type):
    def __init__(self, name, bases, members):
        super(ViewEntityMeta, self).__init__(name, bases, members)

        if bases != (object,):
            self.table = View(
                ("%s_%s" % (self.__module__, self.__name__)).replace('.', '_').lower(),
                elixir.metadata,
                self.expression,
                self.primary_key,
                **self.clause_arguments)

            sqlalchemy.orm.mapper(self, self.table)

class ViewEntity(object):
    __metaclass__ = ViewEntityMeta
    primary_key = 'id'
    clause_arguments = {}
    
