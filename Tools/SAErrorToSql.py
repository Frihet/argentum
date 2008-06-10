#! /usr/bin/python

inp = raw_input("Please enter SA output> ")
sql, params = inp.split('{')
sql = eval(sql)
params = eval('{' + params)

for key, value in params.iteritems():
    sql = sql.replace(':' + key, "'%s'" % value)

print
print
print sql
print
print

