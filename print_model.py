from __future__ import with_statement

import Argentum
import types, time, cgi

calc_perf_data=True
model_name = "Greencycle.Model"
views = {}
global_counter = 0

model = __import__(model_name)
for item in model_name.split('.')[1:]:
    model = getattr(model, item)
session = model.engine.Session()

connection=session.connection()
perf_data = {}

views = []

if not calc_perf_data:
    with open("perf_data.py",'r') as f:
        perf_data = eval(f.read())
        
def format_sql(sql):
    for i in (','):
        sql = sql.replace(i, i+'\n')
    for i in (' AND', ' OR ',' || ',' * ', ' LEFT OUTER JOIN', ' ON', ' GROUP BY'):
        sql = sql.replace(i, '\n'+i)
    return sql

def introspect_model(node):
    for child_name in dir(node):
        child = getattr(node, child_name)
        if isinstance(child,types.ModuleType) and child.__name__.find(node.__name__ + ".") == 0:
            introspect_model(child)
        elif hasattr(child, '__metaclass__') and child.__metaclass__ is Argentum.ViewEntityMeta:
            introspect_view(child)

def get_dependency_count(node, skip_pseudo=True):
    if skip_pseudo and (node.is_pseudo_materialized or node.is_materialized):
        return 1
    if len(node.get_dependencies()) == 0:
        return 1
    num = 1
    for child in node.get_dependencies():
        num += get_dependency_count(child, skip_pseudo)
    return num
    
def get_all_dependants(node):
    dep = node._dependants.values()
    res = list(dep)
    for parent in dep:
        res = res + get_all_dependants(parent)
    return res

def get_dependant_count(node):
    return len(get_all_dependants(node))

def format_dependants(node):
    global connection

    dep = get_all_dependants(node)
    if len(dep) == 0:
        return "0"
    out = ""
    for parent in dep:
        out += "<div>%(link)s</div>" % {
            'link'         :make_link(parent),
            }

    return togglable(str(len(dep)), out)

def togglable(desc, inner):
    global global_counter

    outer_param = {
        'desc'                : desc,
        'id'                  : global_counter,
        'inner'               : inner
        }
    global_counter += 1
    out = """
    <button onclick="toggleVisible(getElementById('hide_%(id)d'), getElementById('hide_button_%(id)d'));" id='hide_button_%(id)d'>+</button> %(desc)s
    <span id='hide_%(id)d' style='display: none;'>
    %(inner)s
    </span>
    """ % outer_param
    return out

def make_link(node):
    global connection
    return "<a href='#%(mangled_name)s'>%(name)s</a>" % {
        'mangled_name':node.get_name(connection),
        'name':node.entity.__name__,
        }

def format_dependencies(node):
    inner = ""
    global connection
    
    if len(node.get_dependencies())==0:
        return "<div>%s</div>" % make_link(node)


    for child in node.get_dependencies():

        inner_param = {
            'sub_dependencies'   : format_dependencies(child),
            }
                
        inner += """
		<div class='dependency'>
                %(sub_dependencies)s
                </div>""" % inner_param
    desc = make_link(node)
    out = togglable(desc,inner)

    return out


def meassure_time(func):
    time1 = time.time()
    res = func()
    time_elapsed = time.time()-time1
    return (res, time_elapsed)


def introspect_view(entity):
    global connection
    global views

    view = entity.table

    if view in views:
        return
    
    view.entity = entity
    if not view.is_materialized:
        view.refresh(connection)
    views.append(view)


def meassure_view(view):
    global perf_data
    global connection

    try:
        (count, count_time) = meassure_time(lambda: connection.execute('select count(*) from ' + view.get_name(connection)))
        count = list(count)[0][0]
        perf_data[view.entity.__name__] = (count, count_time)
        count_time = "%.2f" % count_time
    except:
        perf_data[view.entity.__name__] = (None, None)

def format_view(view):
    global perf_data
    global connection
    global calc_perf_data
    entity = view.entity

    (count, count_time) = (None, None)
    if entity.__name__ in perf_data:
        (count, count_time) = perf_data[entity.__name__]

#    if count is None:
#        return ""

    dependency_count_full_str = ""
    dependency_count_full = get_dependency_count(view,False)

    view_type = "Regular"
    if view.is_materialized:
        view_type = "Materialized"
    if view.is_pseudo_materialized:
        view_type = "Pseudo-materialized"

    if dependency_count_full != get_dependency_count(view):
        dependency_count_full_str = """
<tr>
  <th>
    Dependency count (without materialization):
  </th>
  <td>
    %(dependency_count_full)d
  </td>
</tr>
""" % {'dependency_count_full': dependency_count_full }

    param = {'name':entity.__name__,
             'dependant_count':format_dependants(view),
             'dependency_count':get_dependency_count(view),
             'dependency_count_full':dependency_count_full_str,
             'number_of_rows':count,
             'count_time':count_time,
             'mangled_name': view.get_name(connection),
             'definition' : togglable("","<pre>"+cgi.escape(format_sql(str(view._expression.compile(connection))))+"</pre>"),
             'mangled_name': view.get_name(connection),
             'view_type': view_type
             }

    

    desc = """<h2><a name='%(mangled_name)s'>%(name)s</a></h2>
<table>

<tr>
  <th>
    Definition:
  </th>
  <td>
    %(definition)s
  </td>
</tr>
<tr>

<tr>
  <th>
    Number of rows in view:
  </th>
  <td>
    %(number_of_rows)s
  </td>
</tr>
<tr>

<tr>
  <th>
    Mangled name:
  </th>
  <td>
    %(mangled_name)s
  </td>
</tr>
<tr>

<tr>
  <th>
    View type:
  </th>
  <td>
    %(view_type)s
  </td>
</tr>
<tr>

<tr>
  <th>
    Time used to count rows:
  </th>
  <td>
    %(count_time)s
  </td>
</tr>
<tr>

<tr>
  <th>
    Dependant count:
  </th>
  <td>
    %(dependant_count)s
  </td>
</tr>
<tr>

<tr>
  <th>
    Dependency count:
  </th>
  <td>
    %(dependency_count)d
  </td>
</tr>

%(dependency_count_full)s

</table>
    """ % param
    dependencies = format_dependencies(view)
    out = "<div class='view_root'> %s %s </div>\n\n" % (desc, dependencies)

    print '.'

    return out


introspect_model(model)

if calc_perf_data:
    for view in views:
        meassure_view(view)

views.sort(lambda x, y: cmp(perf_data[x.entity.__name__][1], perf_data[y.entity.__name__][1]))

with open("model.html",'w') as f:
    f.write("""<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01//EN"
"http://www.w3.org/TR/html4/strict.dtd">
<html>
        <head>
                <meta http-equiv="Content-Type" content="text/html;charset=utf-8">
                <style type="text/css">
body
{
        font-size: small;
        font-family: "Bitstream Charter";
}

button
{
        width: 1.6em;
        height: 1.6em;
        border: 1px solid black;
        background: white;
        margin: 0px;
        padding: 0px;
}

ul li
{
        list-style: none;
}

th
{
        text-align: left;
}

.dependency
{
    margin-left:25px;
}


                </style>
		<script type="text/javascript">

function toggleVisible(el, btn)
{
    if(el.style.display=='none') {
        el.style.display = 'block';
        btn.innerHTML = '-'
    } else {
        el.style.display = 'none';
        btn.innerHTML = '+'
    }
}

		</script>
                <title>View dependencies</title>
        </head>
        <body>
                <h1><a name='anchor_top'>View dependencies</a></h1>
""")


    for view in views:
        f.write(format_view(view))

    


if calc_perf_data:
    with open("perf_data.py",'w') as f:
        f.write(repr(perf_data))
    





sql = """
SELECT dynamiccolumnview_rowid_,
 dynamiccolumnview_id,
 dynamiccolumnview_contract_id,
 dynamiccolumnview_operator_id,
 dyn_view_operator_title_3a0d80,
 dyn_nview_contractor_id_fb7c93,
 dyn_ew_contractor_title_2139a6,
 dynamiccolumnview_principal_id,
 dyn_iew_principal_title_4698f3,
 dynamiccolumnview_fraction_id,
 dyn_view_fraction_title_55dacf,
 dynamiccolumnview_uom_id,
 dynamiccolumnview_uom_title,
 dynamiccolumnview_cur_sent,
 dyn_from_sold_fractions_54cbee,
 dynamiccolumnview_cur_received,
 dyn__cur_total_received_30a259,
 dyn_view_cur_total_sent_f9fa6c,
 dyn_ew_cur_mass_balance_fc578d,
 dyn_prev_total_received_18dd0b,
 dyn_iew_prev_total_sent_4cc2bc,
 dyn_w_prev_mass_balance_c44b56,
 dyn_w_dyncol_1455_units_6c47da,
 dyn_w_dyncol_1456_units_0382e6 
FROM (SELECT dynamiccolumnview.rowid_ AS dynamiccolumnview_rowid_,
 dynamiccolumnview.id AS dynamiccolumnview_id,
 dynamiccolumnview.contract_id AS dynamiccolumnview_contract_id,
 dynamiccolumnview.operator_id AS dynamiccolumnview_operator_id,
 dynamiccolumnview.operator_title AS dyn_view_operator_title_3a0d80,
 dynamiccolumnview.contractor_id AS dyn_nview_contractor_id_fb7c93,
 dynamiccolumnview.contractor_title AS dyn_ew_contractor_title_2139a6,
 dynamiccolumnview.principal_id AS dynamiccolumnview_principal_id,
 dynamiccolumnview.principal_title AS dyn_iew_principal_title_4698f3,
 dynamiccolumnview.fraction_id AS dynamiccolumnview_fraction_id,
 dynamiccolumnview.fraction_title AS dyn_view_fraction_title_55dacf,
 dynamiccolumnview.uom_id AS dynamiccolumnview_uom_id,
 dynamiccolumnview.uom_title AS dynamiccolumnview_uom_title,
 dynamiccolumnview.cur_sent AS dynamiccolumnview_cur_sent,
 dynamiccolumnview.cur_income_from_sold_fractions AS dyn_from_sold_fractions_54cbee,
 dynamiccolumnview.cur_received AS dynamiccolumnview_cur_received,
 dynamiccolumnview.cur_total_received AS dyn__cur_total_received_30a259,
 dynamiccolumnview.cur_total_sent AS dyn_view_cur_total_sent_f9fa6c,
 dynamiccolumnview.cur_mass_balance AS dyn_ew_cur_mass_balance_fc578d,
 dynamiccolumnview.prev_total_received AS dyn_prev_total_received_18dd0b,
 dynamiccolumnview.prev_total_sent AS dyn_iew_prev_total_sent_4cc2bc,
 dynamiccolumnview.prev_mass_balance AS dyn_w_prev_mass_balance_c44b56,
 dynamiccolumnview.dyncol_1455_units AS dyn_w_dyncol_1455_units_6c47da,
 dynamiccolumnview.dyncol_1456_units AS dyn_w_dyncol_1456_units_0382e6,
 ROW_NUMBER() OVER (ORDER BY dynamiccolumnview.id ASC,
 dynamiccolumnview.id ASC) AS ora_rn 
FROM (SELECT gre_e_massbalancetpview_456232.rowid_ AS rowid_,
 gre_e_massbalancetpview_456232.id AS id,
 gre_e_massbalancetpview_456232.contract_id AS contract_id,
 gre_e_massbalancetpview_456232.operator_id AS operator_id,
 gre_e_massbalancetpview_456232.operator_title AS operator_title,
 gre_e_massbalancetpview_456232.contractor_id AS contractor_id,
 gre_e_massbalancetpview_456232.contractor_title AS contractor_title,
 gre_e_massbalancetpview_456232.principal_id AS principal_id,
 gre_e_massbalancetpview_456232.principal_title AS principal_title,
 gre_e_massbalancetpview_456232.fraction_id AS fraction_id,
 gre_e_massbalancetpview_456232.fraction_title AS fraction_title,
 gre_e_massbalancetpview_456232.uom_id AS uom_id,
 gre_e_massbalancetpview_456232.uom_title AS uom_title,
 gre_e_massbalancetpview_456232.cur_sent AS cur_sent,
 gre_e_massbalancetpview_456232.cur_income_from_sold_fractions AS cur_income_from_sold_fractions,
 gre_e_massbalancetpview_456232.cur_received AS cur_received,
 gre_e_massbalancetpview_456232.cur_total_received AS cur_total_received,
 gre_e_massbalancetpview_456232.cur_total_sent AS cur_total_sent,
 gre_e_massbalancetpview_456232.cur_mass_balance AS cur_mass_balance,
 gre_e_massbalancetpview_456232.prev_total_received AS prev_total_received,
 gre_e_massbalancetpview_456232.prev_total_sent AS prev_total_sent,
 gre_e_massbalancetpview_456232.prev_mass_balance AS prev_mass_balance,
 coalesce(gre_currentperiodview_2_00fede.units,
 0.0) AS dyncol_1455_units,
 coalesce(gre_currentperiodview_3_f21812.units,
 0.0) AS dyncol_1456_units 
FROM gre_e_massbalancetpview_456232 LEFT OUTER JOIN gre_alcurrentperiodview_ac2862 gre_currentperiodview_2_00fede ON sqlalchemy.is_and(sqlalchemy.is_equal(gre_currentperiodview_2_00fede.treatment_group_id,
 1455),
 sqlalchemy.is_and(sqlalchemy.is_equal(gre_e_massbalancetpview_456232.contract_id,
 gre_currentperiodview_2_00fede.contract_id),
 sqlalchemy.is_equal(gre_e_massbalancetpview_456232.fraction_id,
 gre_currentperiodview_2_00fede.fraction_id))) = 1 LEFT OUTER JOIN gre_alcurrentperiodview_ac2862 gre_currentperiodview_3_f21812 ON sqlalchemy.is_and(sqlalchemy.is_equal(gre_currentperiodview_3_f21812.treatment_group_id,
 1456),
 sqlalchemy.is_and(sqlalchemy.is_equal(gre_e_massbalancetpview_456232.contract_id,
 gre_currentperiodview_3_f21812.contract_id),
 sqlalchemy.is_equal(gre_e_massbalancetpview_456232.fraction_id,
 gre_currentperiodview_3_f21812.fraction_id))) = 1) dynamiccolumnview 
WHERE (dynamiccolumnview.operator_id IN (1340,
 1462,
 1469,
 1470,
 1471,
 1472,
 1473,
 1463,
 1464,
 1465,
 1466,
 1467,
 1468)
 OR dynamiccolumnview.contractor_id IN (1340,
 1462,
 1469,
 1470,
 1471,
 1472,
 1473,
 1463,
 1464,
 1465,
 1466,
 1467,
 1468)
 OR dynamiccolumnview.principal_id IN (1340,
 1462,
 1469,
 1470,
 1471,
 1472,
 1473,
 1463,
 1464,
 1465,
 1466,
 1467,
 1468))
 AND 1 = 1) 
WHERE ora_rn > '0' AND ora_rn <= '20'"""


import time

t1 = time.time()
connection.execute(sql)
print "time used by query", (time.time()-t1)




