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


import sqlalchemy, sqlalchemy.sql, sqlalchemy.orm, elixir, re

True_ = sqlalchemy.sql.literal(1) == sqlalchemy.sql.literal(1)
False_ = sqlalchemy.sql.literal(1) == sqlalchemy.sql.literal(2)

TrueWhere = True_ = sqlalchemy.sql.literal(1) == sqlalchemy.sql.literal(1)
FalseWhere = False_ = sqlalchemy.sql.literal(1) == sqlalchemy.sql.literal(2)

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
            # FIXME: How should this be handled in the real world?
            column = self._expression.columns[key]

            constraints = []
            if hasattr(column , 'constraints'):
                constraints = [constraint.copy() for constraint in column.constraints]

            copy = sqlalchemy.schema.Column(key,
                                            column.type,
                                            *(  constraints
                                                + [foreign_key.copy() for foreign_key in column.foreign_keys]),
                                            **dict([(col_name, getattr(column, col_name))
                                                    for col_name in attributes_to_copy if hasattr(column, col_name)]))
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

        # Get preparer to quote table/view name
        preparer = bind.dialect.preparer(bind.dialect)

        #### fixme ###
        # name = "Workaround for Oracles broken create view"
        # description = """The Oracle client lib crashes on create
        # view parameters, so format them in by hand. Sadly, we have
        # no proper way to quote them, esp. not in a database agnostic
        # way."""
        #### end ####
        findparams1 = re.compile(r""":([a-zA-Z_0-9]*)""")
        findparams2 = re.compile(r""":({[^}]*})""")
        select = findparams1.sub(r"'%(\1)s'",
                                 findparams2.sub(r"'%(\1)s'",
                                                 str(select))) % params
        params = {}

        bind.execute("create view %(name)s as %(select)s" %
                     {'name': preparer.format_table(self),
                      'select': select},
                     params)

        # FIXME: sqlalchemy.schema.DDL unescapes its parameters
#         sqlalchemy.schema.DDL("create view %(name)s as %(select)s" %
#                               {'name': self.name,
#                                'select': select},
#                               context=params).execute(bind)
                              
    def drop(self, event, metadata, bind):
        # Get preparer to quote table/view name
        preparer = bind.dialect.preparer(bind.dialect)
        bind.execute("drop view %(name)s" %
                     {'name': preparer.format_table(self)})

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
                **self.get_clause_arguments())

            sqlalchemy.orm.mapper(self, self.table, properties=self.get_relation_arguments())

class ViewEntity(object):
    __metaclass__ = ViewEntityMeta
    primary_key = 'id'

    @classmethod
    def get_clause_arguments(cls):
        return {}

    @classmethod
    def get_relation_arguments(cls):
        return {}
    
relarg = {}    
relarg_many_to_one = {'use_alter': True}
relarg_one_to_many = {}
relarg_many_to_many = {}

def ManyToOne(*arg, **kwarg):
    kwarg.update(relarg)
    kwarg.update(relarg_many_to_one)
    return elixir.ManyToOne(*arg, **kwarg)

def OneToMany(*arg, **kwarg):
    kwarg.update(relarg)
    kwarg.update(relarg_one_to_many)
    return elixir.OneToMany(*arg, **kwarg)

def ManyToMany(*arg, **kwarg):
    kwarg.update(relarg)
    kwarg.update(relarg_many_to_many)
    return elixir.ManyToMany(*arg, **kwarg)


relarg_many_belongs_to_one = {'cascade': 'save-update, merge, expunge, refresh-expire'}
relarg_one_has_many_parts = {'cascade': 'delete-orphan, delete, save-update, merge, expunge, refresh-expire'}

def ManyBelongsToOne(*arg, **kwarg):
    kwarg.update(relarg)
    kwarg.update(relarg_many_to_one)
    kwarg.update(relarg_many_belongs_to_one)
    return elixir.ManyToOne(*arg, **kwarg)

def OneHasManyParts(*arg, **kwarg):
    kwarg.update(relarg)
    kwarg.update(relarg_one_to_many)
    kwarg.update(relarg_one_has_many_parts)
    return elixir.OneToMany(*arg, **kwarg)


relarg_one_groups_many = {'cascade': 'save-update, merge, expunge, refresh-expire'}
relarg_many_grouped_by_one = {'cascade': 'save-update, merge, expunge, refresh-expire'}
relarg_many_groups_many = {'cascade': 'save-update, merge, expunge, refresh-expire'}

def OneGroupsMany(*arg, **kwarg):
    kwarg.update(relarg)
    kwarg.update(relarg_one_to_many)
    kwarg.update(relarg_one_groups_many)
    return elixir.OneToMany(*arg, **kwarg)

def ManyGroupedByOne(*arg, **kwarg):
    kwarg.update(relarg)
    kwarg.update(relarg_many_to_one)
    kwarg.update(relarg_many_grouped_by_one)
    return elixir.ManyToOne(*arg, **kwarg)

def ManyGroupsMany(*arg, **kwarg):
    kwarg.update(relarg)
    kwarg.update(relarg_many_to_many)
    kwarg.update(relarg_many_groups_many)
    return elixir.ManyToMany(*arg, **kwarg)

