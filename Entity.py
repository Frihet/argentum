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
import weakref

def refresh_views(expression, connection):
    for view in find_views(expression):
        if not view.is_materialized:
            view.refresh(connection)


def find_views(expression, include_tables=False):
    def locate_froms_in_all_subselects_and_everywhere(obj):
        froms = set()
        
        if hasattr(obj, '_get_from_objects'):
            try:
                _froms = obj._get_from_objects()
            except NotImplementedError:
                pass
            else:
                froms.update(_froms)
                for el in _froms:
                    if el is not obj:
                        froms.update(locate_froms_in_all_subselects_and_everywhere(el))

        for col in getattr(obj, '_raw_columns', []):
            froms.update(locate_froms_in_all_subselects_and_everywhere(col))

        if getattr(obj, '_whereclause', None) is not None:
            froms.update(locate_froms_in_all_subselects_and_everywhere(obj._whereclause))

        for elem in getattr(obj, '_froms', []):
            froms.add(elem)
            froms.update(locate_froms_in_all_subselects_and_everywhere(elem))

        if hasattr(obj, 'elem'):
            froms.update(locate_froms_in_all_subselects_and_everywhere(obj.elem))

        return froms

    def alias_to_selectable(alias_or_selectable):
        if isinstance(alias_or_selectable, sqlalchemy.sql.expression.Alias):
            return alias_or_selectable.selectable
        return alias_or_selectable

    res = [from_clause for from_clause
            in set(alias_to_selectable(obj) for obj
                   in locate_froms_in_all_subselects_and_everywhere(expression))
           if (   isinstance(from_clause, View)
               or (    include_tables
                   and isinstance(from_clause, sqlalchemy.schema.Table)))]
    return res

True_ = sqlalchemy.sql.literal(1) == sqlalchemy.sql.literal(1)
False_ = sqlalchemy.sql.literal(1) == sqlalchemy.sql.literal(2)

TrueWhere = True_ = sqlalchemy.sql.literal(1) == sqlalchemy.sql.literal(1)
FalseWhere = False_ = sqlalchemy.sql.literal(1) == sqlalchemy.sql.literal(2)

debug_materialized = False

all_pseudo_materialized_views=[]

def soil_all_pseudo_materialized():
    for view in all_pseudo_materialized_views:
        view.soil()

class View(sqlalchemy.schema.SchemaItem, sqlalchemy.sql.expression.TableClause):
    __visit_name__ = 'table'

    create_statement = "create %(options)s %(name)s %(tail_options)s as %(select)s"
    drop_statement = "drop %(options)s %(name)s"
    insert_statement = "insert into %(name)s %(select)s"
    delete_statement = "delete from %(name)s"
    materialize_statement = "begin dbms_mview.refresh('%(name)s','C'); end;"

    def __init__(self,
                 name,
                 metadata,
                 expression,
                 primary_key = None,
                 column_args = {},
                 column_kws = {},
                 is_materialized = False,
                 is_pseudo_materialized = False,
                 **kw):
#        is_pseudo_materialized = False
#        is_materialized = False
        super(View, self).__init__(name, **kw)
        self.fulhack = name
        metadata.append_ddl_listener('after-create', self.create)
        metadata.append_ddl_listener('before-drop', self.drop)
        self.select_internal = None
        self.is_pseudo_materialized = is_pseudo_materialized
        self._expression = expression
        self._dependants = weakref.WeakValueDictionary()
        self._dependencies = [None, None]
        self.dirty = not is_materialized
        self._soil_count = 0
        if is_pseudo_materialized:
            all_pseudo_materialized_views.append(self)

        attributes_to_copy = ("nullable",
                              "default",
                              "_is_oid",
                              "index",
                              "unique",
                              "autoincrement",
                              "quote")

        if primary_key is None:
            attributes_to_copy += ('primary_key')

        entities = [self]

        for entity in [self]:
            entity.metadata = metadata
            for key in  expression.columns.keys():
                # FIXME: How should this be handled in the real world?
                column = expression.columns[key]

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
                copy._set_parent(entity)

        for table in self.get_dependencies():
            table._dependants[id(self)] = self

        if primary_key is not None:
            if not isinstance(primary_key, (str, unicode)):
                # Get the name of the primary key column obect
                primary_key = primary_key.comparator.prop.key
            self.columns[primary_key].primary_key = True
            self._primary_key = sqlalchemy.sql.expression.ColumnCollection(self.columns[primary_key])

        self.is_materialized = is_materialized


    def get_dependencies(self, include_tables=False):
        if self._dependencies[include_tables] is None:
            self._dependencies[include_tables] = find_views(self._expression, include_tables)
        return self._dependencies[include_tables]

    def get_options(self):
        if self.is_pseudo_materialized:
            return "global temporary table"
        elif self.is_materialized:
            return "materialized view"
        else:
            return "view"

    def get_tail_options(self):
        return ""
    
    def get_drop_options(self):
        if self.is_pseudo_materialized:
            return "table"
        elif self.is_materialized:
            return "materialized view"
        else:
            return "view"

    def create(self, event, metadata, bind):
        for my_type in ['view','materialized view', 'table']:
            try:
                sql = "drop %(type)s %(name)s" % {'type': my_type, 'name': self.get_name(bind)}
                bind.execute(sql)
            except:
                pass

        if self.is_pseudo_materialized:
            print "create pseudo-materialized view", self.get_name(bind)

        if self.is_materialized:
            print "create materialized view", self.get_name(bind)

        bind.execute(self.create_statement % {'options': self.get_options(),
                                              'name': self.get_name(bind),
                                              'select': self.get_select(bind),
                                              'tail_options': self.get_tail_options()},
                     {})
        self.dirty = False

    def get_select(self, bind):
        if self.select_internal:
            return self.select_internal
        
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

        self.select_internal = select
        return select

    # FIXME: sqlalchemy.schema.DDL unescapes its parameters
    #         sqlalchemy.schema.DDL("create view %(name)s as %(select)s" %
    #                               {'name': self.name,
    #                                'select': select},
    #                               context=params).execute(bind)

    def get_name(self, bind):
        preparer = bind.dialect.preparer(bind.dialect)
        return preparer.format_table(self)
                              
    def drop(self, event, metadata, bind):
        bind.execute(self.drop_statement %
                     {'options': self.get_drop_options(),
                      'name': self.get_name(bind)})

    # FIXME: sqlalchemy.schema.DDL unescapes its parameters
    #         sqlalchemy.schema.DDL("drop view %(name)s" %
    #                               {'name': self.name}).execute(bind)

    # def update_materialized_view(self, bind):
    # Get preparer to quote table/view name


    def refresh(self, connection):
        # Start by refreshing things we depend on

        for view in self.get_dependencies():
            if not view.is_materialized:
                view.refresh(connection)

        if self.is_pseudo_materialized:
            if not hasattr(connection, "_soil_count"):
                connection._soil_count = {}
            if not self in connection._soil_count:
                do_refresh = True
            else:
                do_refresh = connection._soil_count[self] != self._soil_count

            connection._soil_count[self] = self._soil_count

            if do_refresh:
                connection.execute(self.delete_statement % { 'name':   self.get_name(connection)})
                connection.execute(self.insert_statement % { 'name':   self.get_name(connection),
                                                           'select': self.get_select(connection)})
                #print "inserted", self.name
        elif self.is_materialized:
            connection.execute(self.materialize_statement % { 'name': self.get_name(connection) })            
            self.dirty = False
                
    def soil(self):
        self._soil_count += 1
        for dependant in self._dependants.itervalues():
            dependant.soil()

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
                self.__dict__.get('ag_is_materialized', False), # Note: Don't inherit ag_is_materialized, it has to be set on each class separately!
                self.__dict__.get('ag_is_pseudo_materialized', False) # Note: Don't inherit ag_is_pseudo_materialized, it has to be set on each class separately!
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

    def soil():
        """
        Mark this pseudo-view as dirty, i.e. in need of a refresh
        before it is used again. This should only be run on
        pseudo_materialized views.
        """
        if not self.ag_is_pseudo_materialized:
            print "Warning: Regular view marked as dirty!"
        self.__metaclass__.table.dirty = True

    @property
    def all_pre_queries(self):
        return self.__metaclass__.table.get_all_pre_queries()


    class Descriptor(object):
        # This is just to fool Elixir we're a table :)
        
        def __init__(self, view):
            self.view = view
            
        def find_relationship(self, name):
            return getattr(self.view, name)

    @classmethod
    def refresh(self, session):
        if debug_materialized: print "Update materialized", self
        self.table.refresh(session.connection())

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

