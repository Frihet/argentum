import sqlalchemy

debug_copy = False

class BaseModel(object):
    """This class extends SQLAlchemy models with some extra utility
    methods, and provides widgets for editing the database fields of
    the model."""

    def __init__(self, *arg, **kw):
        super(BaseModel, self).__init__(*arg, **kw)
        for col in dir(type(self)):
            if col.endswith('__default'):
                value_col = col[:-len('__default')]

                # Only set default values if no values are provided.
                if not getattr(self, value_col, None):
                    setattr(self, value_col, getattr(self, col))

    def __unicode__(self):
        if hasattr(self, 'title'):
            return unicode(self.title)
        if hasattr(self, 'id'):
            return "%s.%s %s" % (type(self).__module__, type(self).__name__, self.id)
        return "%s.%s" % (type(self).__module__, type(self).__name__)

    def __str__(self):
        return str(unicode(self))

    def __repr__(self):
        def strattr(name):
            value = getattr(self, name)
            if self.column_is_scalar(name):
                return unicode(value)
            else:
                return ', '.join(unicode(part) for part in value)
        return "<%s.%s%s>" % (type(self).__module__, type(self).__name__,
                              ','.join(["\n %s=%s" % (name, strattr(name))
                                        for name in self.get_column_names()]))

    @classmethod
    def get_columns(cls, exclude_primary_keys = False, exclude_foreign_keys = False):
        return [(name, col)
                for (name, col) in [(name, getattr(cls, name))
                                    for name in dir(cls)]
                if (# Filter out non-column attributes
                        hasattr(col, 'impl')
                    and isinstance(col.impl, sqlalchemy.orm.attributes.AttributeImpl)

                    and not (exclude_foreign_keys
                             and isinstance(col.impl, (sqlalchemy.orm.attributes.ScalarObjectAttributeImpl,
                                                       sqlalchemy.orm.attributes.CollectionAttributeImpl)))

                    # Filter out properties, primary and foreign keys, if requested
                    and not (isinstance(col.comparator.prop, sqlalchemy.orm.properties.ColumnProperty)
                             and col.comparator.prop.columns
                             and (exclude_primary_keys and col.comparator.prop.columns[0].primary_key)))]

    @classmethod
    def get_many_to_many_crossref_info(cls, column_name):
        # Don't just return the column object, as the user might want
        # to do alias() on the table first!
        columns = getattr(cls, column_name).impl.callable_.im_self.remote_side
        return columns[0].table, columns[0].name, columns[1].name

    def get_columns_and_instances(self, *arg, **kw):
        return [(name, col, getattr(self, name))
                for (name, col) in self.get_columns(*arg, **kw)]

    def get_column_instances(self, *arg, **kw):
        return [(name, value)
                for (name, col, value) in self.get_columns_and_instances(*arg, **kw)]

    @classmethod
    def get_column_names(cls, *arg, **kw):
        return [name
                for (name, col) in cls.get_columns(*arg, **kw)]


    @classmethod
    def column_is_scalar(cls, name):
        cls_member = getattr(cls, name)
        return isinstance(cls_member.impl, (sqlalchemy.orm.attributes.ScalarAttributeImpl,
                                            sqlalchemy.orm.attributes.ScalarObjectAttributeImpl))

    @classmethod
    def column_is_foreign(cls, name):
        cls_member = getattr(cls, name)
        return isinstance(cls_member.impl, (sqlalchemy.orm.attributes.ScalarObjectAttributeImpl,
                                            sqlalchemy.orm.attributes.CollectionAttributeImpl))

    @classmethod
    def column_is_sortable(cls, name):
        return name in (x[0] for x in cls.get_columns(cls, exclude_foreign_keys=True))

    @classmethod
    def get_column_subtype(cls, name):
        cls_member = getattr(cls, name)
        # Yes, this sucks, it is icky, but it's the only way to get at it
        # :(
        return cls_member.impl.is_equal.im_self

    @classmethod
    def get_column_foreign_class(cls, name):
        """This fetches the foreign key-pointed-to class for a column
        given the class member. The class member should be of one of the
        two types sqlalchemy.orm.attributes.ScalarObjectAttributeImpl and
        sqlalchemy.orm.attributes.CollectionAttributeImpl"""
        cls_member = getattr(cls, name)
        # Yes, this sucks, it is icky, but it's the only way to get at it
        # :(
        return cls_member.impl.callable_.im_self.mapper.class_

    @classmethod
    def get_column_foreign_column(cls, name, return_none_for_none = False):
        cls_member = getattr(cls, name)
        for ext in cls_member.impl.extensions:
            if isinstance(ext, sqlalchemy.orm.attributes.GenericBackrefExtension):
                return ext.key
        if return_none_for_none: return None
        foreign_cls = cls.get_column_foreign_class(name)
        raise Exception("Column %s of class %s.%s does not have a back-ref column in foreign class %s.%s" % (name,
                                                                                                             cls.__module__, cls.__name__,
                                                                                                             foreign_cls.__module__, foreign_cls.__name__))

    @classmethod
    def get_column_foreign_keys(cls, name):
        return getattr(cls, name).impl.callable_.im_self.foreign_keys

    @classmethod
    def get_column_primary_join(cls, name):
        return getattr(cls, name).impl.callable_.im_self.primaryjoin

    def copy(self, override = {}, copy_foreign = True, indent = '', session=None):
        if debug_copy:
            print "%sCOPY %s.%s @ %s" % (indent, type(self).__module__, type(self).__name__, self.id)
        res = {}
        for name, value in self.get_column_instances(exclude_primary_keys = True,
                                                     exclude_foreign_keys = not copy_foreign):
            if name in override:
                res[name] = override[name]
            else:
                if self.column_is_foreign(name):
                    foreign_name = self.get_column_foreign_column(name, return_none_for_none = True)
                    if self.column_is_scalar(name):
                        if foreign_name and self.get_column_foreign_class(name).column_is_scalar(foreign_name):
                            if getattr(self, name + '__ww_copy_foregin', False) and copy_foreign:
                                res[name] = value.copy(override = {foreign_name:None}, indent = indent + '  ')
                                if hasattr(value, "is_current"):
                                    value.is_current = False
                        else:
                            res[name] = value
                    else:
                        res[name] = []
                        if foreign_name and self.get_column_foreign_class(name).column_is_scalar(foreign_name):
                            if getattr(self, name + '__ww_copy_foregin', False) and copy_foreign:
                                for foreign in value:
                                    res[name].append(foreign.copy(override = {foreign_name:None}, indent = indent + '  '))
                        else:
                            res[name].extend(value)
                else:
                    res[name] = value

                    # FIXME: Figure out how to make the ORM load this on access instead
                    if session and name.endswith('_id'):
                        self._load_foreign(session, res, name, value)


        if hasattr(self, "is_current"):
            self.is_current = False

        return type(self)(**res)

    def _load_foreign(self, session, res, name, value):
        name_object = name[:-3]
        foreign = getattr(self, name_object, None)

        if foreign is not None:
            foreign_type = type(foreign)
            res[name_object] = session.query(foreign_type).filter(foreign_type.id == value).one()




    def debug_row_ids(self, id_col = 'ID'):
        c = self.db_session.connection()

        res = c.execute(sqlalchemy.select([self.ww_filter.DBModel.table]))

        cols = res.keys
        id_pos = cols.index(id_col)

        dct = {}
        for row in res:
            if row[id_pos] not in dct:
                dct[row[id_pos]] = []
            dct[row[id_pos]].append(row)

        dct_duplicates = dict((name, value) for (name, value) in dct.iteritems()
                              if len(value) > 1)

        if dct_duplicates:
            duplicate = dct_duplicates[dct_duplicates.keys()[0]]

            print "ROW1", ', '.join('%s=%s' % (name, value) for (name, value) in zip(cols, duplicate[0]))

            print "ROW2", ', '.join('%s=%s' % (name, value1)
                                    for (name, value0, value1)
                                    in zip(cols, duplicate[0], duplicate[1])
                                    if value0 != value1)
        else:
            print "No duplicate id:s"

