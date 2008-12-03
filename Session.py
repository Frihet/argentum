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


import sqlalchemy, sqlalchemy.engine, sqlalchemy.exceptions
import sqlalchemy.sql, sqlalchemy.orm, sqlalchemy.pool

def create_engine(url):
    # Setup a connection pool. To get this working parsing of the URL
    # to get the dialect -> dbapi to create a connect function.
    url_parsed = sqlalchemy.engine.url.make_url(url)
    dialect_cls = url_parsed.get_dialect()
    dbapi = dialect_cls.dbapi()
    dialect = dialect_cls(dbapi=dbapi)
    (cargs, cparams) = dialect.create_connect_args(url_parsed)

    def connect():
        try:
            return dbapi.connect(*cargs, **cparams)
        except Exception, e:
            raise sqlalchemy.exceptions.DBAPIError.instance(None, None, e)

    pool = sqlalchemy.pool.QueuePool(connect, max_overflow=-1, timeout=15)

    engine = sqlalchemy.create_engine(url, pool=pool)
    engine.session_arguments = {'autoflush': True,
                                'transactional': True,
                                }
    def sessions(**kws):
        real_kws = {}
        real_kws.update(engine.session_arguments)
        real_kws.update(kws)
        print "Real kws",real_kws
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

            def save(self, instance, *arg, **kw):
                BaseSession.save(self, instance, *arg, **kw)
                return instance
                
            def save_or_update(self, instance, *arg, **kw):
                BaseSession.save_or_update(self, instance, *arg, **kw)
                return instance
                
            def expire(self, instance = None, attrs = None):
                if instance is not None:
                    if self.autoflush:
                        self.flush([instance])
                    if attrs is not None:
                        BaseSession.expire(self, instance, attrs)
                    else:
                        BaseSession.expire(self, instance)
                    return instance
                if self.autoflush:
                    self.flush()
                for instance in self.identity_map.values():
                    self.expire(instance)
                return None
                
            def save_and_expire(self, instance = None, *arg, **kw):
                if instance:
                    self.save(instance, *arg, **kw)
                    self.flush()
                    return self.expire(instance, *arg, **kw)
                for instance in self.identity_map.values():
                    self.save_and_expire(instance, *arg, **kw)

            def load_from_session(self, instance):
                #### fixme ####
                # name = """SQLAlchemy: merge clashes with
                # many-to-many"""
                # description = """Uggly hack since merge does not
                # seem to work when you have many-to-many
                # relationships!!! So when no fields are changed, at
                # least you can do this instead..."""
                #### end ####
                t = type(instance)
                return self.query(t).filter(t.id == instance.id)[0]

        return Session
    engine.sessions = sessions
    engine.Session = sessions()
    return engine
