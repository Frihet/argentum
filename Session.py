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


import sqlalchemy, sqlalchemy.sql, sqlalchemy.orm

True_ = sqlalchemy.sql.text("(1 = 1)")
False_ = sqlalchemy.sql.text("(1 = 2)")

def create_engine(url):
    engine = sqlalchemy.create_engine(url)
    engine.session_arguments = {'autoflush': True,
                                'transactional': True,
                                }
    def sessions(**kws):
        real_kws = {}
        real_kws.update(engine.session_arguments)
        real_kws.update(kws)
        BaseSession = sqlalchemy.orm.sessionmaker(bind=engine, **real_kws)
        class Session(BaseSession):
            def __enter__(self):
                return self

            def __exit__(self, type, value, traceback):
                if type is None and value is None and traceback is None:
                    self.commit()
                else:
                    self.rollback()
                self.close()

            def save(self, obj):
                BaseSession.save(self, obj)
                return  obj
                
            def expire(self, obj):
                BaseSession.expire(self, obj)
                return  obj
                
            def save_and_expire(self, obj):
                self.save(obj)
                self.flush()
                return self.expire(obj)

            def load_from_session(self, obj):
                #### fixme ####
                # name = """SQLAlchemy: merge clashes with
                # many-to-many"""
                # description = """Uggly hack since merge does not
                # seem to work when you have many-to-many
                # relationships!!! So when no fields are changed, at
                # least you can do this instead..."""
                #### end ####
                t = type(obj)
                return self.query(t).filter(t.id == obj.id)[0]
            
        return Session
    engine.sessions = sessions
    engine.Session = sessions()
    return engine

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
        
        select =self._expression.compile(bind = bind)
        params = select.construct_params()
        sqlalchemy.schema.DDL("create view %(name)s as %(select)s" %
                              {'name': self.name,
                               'select': select},
                              context=params).execute(bind)
                              
    def drop(self, event, metadata, bind):
        sqlalchemy.schema.DDL("drop view %(name)s" %
                              {'name': self.name}).execute(bind)
