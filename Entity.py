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

    create_statement = "create %(options)s %(name)s as %(select)s"
    drop_statement = "drop %(options)s %(name)s"

    def __init__(self, name, metadata, expression, primary_key = None, column_args = {}, column_kws = {}, is_materialized = False, **kw):
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

            args = []
            if hasattr(column , 'constraints'):
                args = [constraint.copy() for constraint in column.constraints]
            args.extend(
                (foreign_key.copy()
                 for foreign_key
                 in column.foreign_keys))

            if key in column_args:
                if isinstance(column_args[key], (list, tuple)):
                    args.extend(column_args[key])
                else:
                    args.append(column_args[key])
                
            kws = dict(
                ((col_name, getattr(column, col_name))
                 for col_name in attributes_to_copy if hasattr(column, col_name)))
            
            if key in column_kws:
                kws.update(column_kws[key])

            copy = sqlalchemy.schema.Column(key,
                                            column.type,
                                            *args,
                                            **kws)
            copy._set_parent(self)

        if primary_key is not None:
            if not isinstance(primary_key, (str, unicode)):
                # Get the name of the primary key column obect
                primary_key = primary_key.comparator.prop.key
            self.columns[primary_key].primary_key = True
            self._primary_key = sqlalchemy.sql.expression.ColumnCollection(self.columns[primary_key])

        self.is_materialized = is_materialized

    def get_options(self, preparer):
        if self.is_materialized:
            print "MATERIALIZED", preparer.format_table(self)
            return "materialized view"
        else:
            return "view"

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

        bind.execute(self.create_statement % {'options': self.get_options(preparer),
                                              'name': preparer.format_table(self),
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
        bind.execute(self.drop_statement %
                     {'options': self.get_options(preparer),
                      'name': preparer.format_table(self)})

        # FIXME: sqlalchemy.schema.DDL unescapes its parameters
#         sqlalchemy.schema.DDL("drop view %(name)s" %
#                               {'name': self.name}).execute(bind)

    def update_materialized_view(self, bind):
        # Get preparer to quote table/view name
        preparer = bind.dialect.preparer(bind.dialect)

        bind.execute("begin dbms_mview.refresh('%(name)s','C'); end;" % {
            'name': preparer.format_table(self)})

class StashedRelation(object):
    def __init__(self, rel):
        self.rel_type, self.target = type(rel), rel.target
        self.inv_foreign_key = None
        if isinstance(rel, elixir.OneToMany):
            # Don't use rel.inverse.foreign_key as we don't really
            # have an inverse relation...
            self.inv_foreign_key = getattr(self.target.table.c, "%s_id" % rel.inverse_name)

class ViewEntityMeta(elixir.EntityMeta, type):
    # Note: we only inherit from elixir.EntityMeta since elixir
    # doesn't really do duck-typing, so some relationships and stuff
    # crashes if we don't. But we don't use any functionality from it.
    def __init__(self, name, bases, members):
        type.__init__(self, name, bases, members)

        if bases != (object,):

            # Save all col-specs so mapper can't get at them...
            for col_name in dir(self):
                if not col_name.startswith('_argentum_'):
                    col = getattr(self, col_name)
                    stach_name = "_argentum_%s" % col_name
                    if isinstance(col, elixir.relationships.Relationship):
                        setattr(self, stach_name, StashedRelation(col))
                    #Handle overrides (remove all attribute magic)
                    elif (    hasattr(self, stach_name)
                          and not isinstance(col, (sqlalchemy.orm.attributes.InstrumentedAttribute,
                                                   sqlalchemy.sql.expression.Operators))):
                        setattr(self, stach_name, "DEADBEEF")

            columns = dict((name, value)
                           for (name, value)
                           in ((col_name[len('_argentum_'):],
                                getattr(self, col_name))
                               for col_name in dir(self)
                               if col_name.startswith('_argentum_'))
                           if isinstance(value, StashedRelation))

            column_args = {}
            for col_name, rel in columns.iteritems():
                if rel.rel_type is elixir.ManyToOne:
                    column_args["%s_id" % col_name] = sqlalchemy.ForeignKey(rel.target.table.c.id)
                        
            self.table = View(
                ("%s_%s" % (self.__module__, self.__name__)).replace('.', '_').lower(),
                elixir.metadata,
                self.expression,
                self.primary_key,
                column_args,
                {},
                self.__dict__.get('ag_is_materialized', False) # Note: Don't inherit ag_is_materialized, it has to be set on each class separately!
                )

            relation_args = {}
            for col_name, rel in columns.iteritems():
                if rel.rel_type is elixir.ManyToOne:
                    foreign_key = getattr(self.table.c, "%s_id" % col_name)
                    relation_args[col_name] = sqlalchemy.orm.relation(
                        rel.target,
                        primaryjoin = foreign_key == rel.target.table.c.id)
                elif rel.rel_type is elixir.OneToMany:
                    relation_args[col_name] = sqlalchemy.orm.relation(
                        rel.target,
                        primaryjoin = self.table.c.id == rel.inv_foreign_key,
                        foreign_keys = [rel.inv_foreign_key])

            self._descriptor = self.Descriptor(self)

            sqlalchemy.orm.mapper(self, self.table, properties=relation_args)

    # Override method from elixir.EntityMeta
    def __call__(self, *arg, **kw):
        return type.__call__(self, *arg, **kw)

class ViewEntity(object):
    __metaclass__ = ViewEntityMeta
    primary_key = 'id'

    ag_is_materialized = False

    class Descriptor(object):
        # This is just to fool Elixir we're a table :)
        
        def __init__(self, view):
            self.view = view
            
        def find_relationship(self, name):
            return getattr(self.view, name)

    @classmethod
    def update_materialized_view(self, session):
        print "Update materialized", self
        self.table.update_materialized_view(session.bind)

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

