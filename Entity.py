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

class View(sqlalchemy.sql.expression.TableClause, sqlalchemy.schema.SchemaItem):
    __visit_name__ = 'table'

    def __init__(self, name, metadata, expression, primary_key = 'id', **kw):
        self._expression = expression
        metadata.append_ddl_listener('after-create', self.create)
        metadata.append_ddl_listener('before-drop', self.drop)
        sqlalchemy.sql.expression.TableClause.__init__(self, name, *self._expression.columns, **kw)
        if isinstance(primary_key, str):
            primary_key = sqlalchemy.sql.expression.ColumnCollection(self._expression.columns[primary_key])
	self._primary_key = primary_key

    def create(self, event, metadata, bind):
        try:
            self.drop(event, metadata, bind)
        except:
            pass
        
        select = self._expression.compile(bind = bind)
        params = select.construct_params()

        bind.execute(sqlalchemy.schema.DDL("create view %(name)s as %(select)s" %
                                           {'name': self.name,
                                            'select': select},
                                           ),
                     **params)
                              
    def drop(self, event, metadata, bind):
        bind.execute(sqlalchemy.schema.DDL("drop view %(name)s" %
                                           {'name': self.name}))

class ViewEntityMeta(type):
    def __init__(self, name, bases, members):
        super(ViewEntityMeta, self).__init__(name, bases, members)

        if bases != (object,):
            self.table = View(
                ("%s_%s" % (self.__module__, self.__name__)).lower(),
                elixir.metadata,
                self.expression,
                self.primary_key,
                **self.clause_arguments)

            sqlalchemy.orm.mapper(self, self.table)

class ViewEntity(object):
    __metaclass__ = ViewEntityMeta
    primary_key = 'id'
    clause_arguments = {}
    
