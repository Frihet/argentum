#! /usr/bin/env python

from __future__ import with_statement

import cx_Oracle
import Argentum
import types, time, cgi
import elixir

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
refresh_data={}
views = []

if not calc_perf_data:
    with open("perf_data.py",'r') as f:
        perf_data = eval(f.read())
    with open("refresh_data.py",'r') as f:
        refresh_data = eval(f.read())
        
def format_sql(sql):
    for i in (','):
        sql = sql.replace(i, i+'\n')
    for i in (' AND', ' OR ',' || ',' * ', ' JOIN', ' ON', ' GROUP BY'):
        sql = sql.replace(i, '\n'+i)
    return sql

def introspect_model(node):
    for child_name in dir(node):
        child = getattr(node, child_name)
        if isinstance(child,types.ModuleType) and child.__name__.find(node.__name__ + ".") == 0:
            introspect_model(child)
        elif hasattr(child, '__metaclass__') and child.__metaclass__ is Argentum.ViewEntityMeta:
            introspect_view(child)
        elif hasattr(child, '__metaclass__') and child.__metaclass__ is elixir.entity.EntityMeta:
            child.table.entity = child

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
            'link'            : make_link(parent),
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
    global perf_data

    style='view'
    extra = ""
    name = node.entity.__name__
    mangled_name = ""
    if not hasattr(node, "get_dependencies"):
        style = "table"
        preparer = connection.dialect.preparer(connection.dialect)
        mangled_name = preparer.format_table(node)
        (count, count_time) = measure_time(lambda: connection.execute('select count(*) from ' + mangled_name))
        count = list(count)[0][0]
        extra = '(%d lines, %.2f s)' % (count, count_time)

    else:
        mangled_name = node.get_name(connection)
        extra = '(%d lines, %.2f s)' % perf_data[node.entity.__name__]
        if node.is_pseudo_materialized:
            style = 'pseudo_materialized_view'
        elif node.is_materialized:
            style = 'materialized_view'
        
    return "<a class='%(style)s' href='#%(mangled_name)s'>%(name)s %(extra)s</a>" % {
        'mangled_name':mangled_name,
        'name':name,
        'style':style,
        'extra': extra
        }

def format_dependencies(node):
    inner = ""
    global connection

    if not hasattr(node, "get_dependencies") or len(node.get_dependencies(True))==0:
        return "<div>%s</div>" % make_link(node)


    for child in node.get_dependencies(True):

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


def measure_time(func):
    time1 = time.time()
    res = func()
    time_elapsed = time.time()-time1
    return (res, time_elapsed)


def introspect_view(entity):
    global connection
    global views
    global refresh_data
    global calc_perf_data
    view = entity.table

    if view in views:
        return
    
    view.entity = entity
    if not view.is_materialized:
        if calc_perf_data:
            Argentum.soil_all_pseudo_materialized()
            (florp, refresh_time) = measure_time(lambda: view.refresh(connection))
            view.soil()
            (florp, refresh_time_single) = measure_time(lambda: view.refresh(connection))
            refresh_data[entity.__name__] = (refresh_time, refresh_time_single)
        else:
            view.refresh(connection)
    views.append(view)


def measure_view(view):
    global perf_data
    global connection
    print "Measure", view.entity.__name__

    try:
        (count, count_time) = measure_time(lambda: connection.execute('select count(*) from ' + view.get_name(connection)))
        count = list(count)[0][0]
        perf_data[view.entity.__name__] = (count, count_time)
    except:
        perf_data[view.entity.__name__] = (None, None)

def format_view(view):
    global perf_data
    global connection
    global calc_perf_data
    global refresh_data
    entity = view.entity

    (count, count_time) = (None, None)
    if entity.__name__ in perf_data:
        (count, count_time) = perf_data[entity.__name__]
        count_time = "%.2f" % count_time


#    if count is None:
#        return ""

    dependency_count_full_str = ""
    dependency_count_full = get_dependency_count(view,False)

    view_type = "Regular"
    if view.is_materialized:
        view_type = "Real materialized"
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

    refresh_str = ""
    if view.is_pseudo_materialized:
        refresh_str = """
<tr>
  <th>
    Refresh time (including dependencies):
  </th>
  <td>
    %(refresh_time).2f
  </td>
</tr>

<tr>
  <th>
    Refresh time (excluding dependencies):
  </th>
  <td>
    %(refresh_time_single).2f
  </td>
</tr>
""" % {'refresh_time': refresh_data[entity.__name__][0],
       'refresh_time_single': refresh_data[entity.__name__][1],
       }


    param = {'name':entity.__name__,
             'dependant_count':format_dependants(view),
             'dependency_count':get_dependency_count(view),
             'dependency_count_full':dependency_count_full_str,
             'number_of_rows':count,
             'count_time':count_time,
             'mangled_name': view.get_name(connection),
             'definition' : togglable("","<pre>"+cgi.escape(format_sql(str(view._expression.compile(connection))))+"</pre>"),
             'mangled_name': view.get_name(connection),
             'view_type': view_type,
             'refresh_str': refresh_str
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

%(refresh_str)s

</table>
    """ % param
    dependencies = format_dependencies(view)
    out = "<div class='view_root'> %s %s </div>\n\n" % (desc, dependencies)

    print '.'

    return out


introspect_model(model)

if calc_perf_data:
    for view in views:
        measure_view(view)

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

.table
{
        color: green;
}      

.materialized_view
{
        color: orange;
}

.pseudo_materialized_view
{
        color: orange;
}

.view
{
        color: red;
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
        if view.is_pseudo_materialized and calc_perf_data:
            view.refresh(connection)

    for view in views:
        f.write(format_view(view))

    


if calc_perf_data:
    with open("perf_data.py",'w') as f:
        f.write(repr(perf_data))
with open("refresh_data.py",'w') as f:
    f.write(repr(refresh_data))
    
sql = "select * from dual"

sql = """
SELECT dynamiccolumnview_rowid_, dynamiccolumnview_id, dynamiccolumnview_contract_id, dynamiccolumnview_operator_id, dyn_view_operator_title_3a0d80, dyn_nview_contractor_id_fb7c93, dyn_ew_contractor_title_2139a6, dynamiccolumnview_principal_id, dyn_iew_principal_title_4698f3, dynamiccolumnview_fraction_id, dyn_view_fraction_title_55dacf, dynamiccolumnview_uom_id, dynamiccolumnview_uom_title, dynamiccolumnview_cur_sent, dyn_from_sold_fractions_54cbee, dynamiccolumnview_cur_received, dyn__cur_total_received_30a259, dyn_view_cur_total_sent_f9fa6c, dyn_ew_cur_mass_balance_fc578d, dyn_prev_total_received_18dd0b, dyn_iew_prev_total_sent_4cc2bc, dyn_w_prev_mass_balance_c44b56, dyn_ew_dyncol_893_units_7b31e5, dyn_ew_dyncol_992_units_d40b8b, dyn_ew_dyncol_982_units_03d4a0, dyn_ew_dyncol_983_units_1e3771, dyn_ew_dyncol_985_units_f31fbe, dyn_ew_dyncol_984_units_d61ab5, dyn_ew_dyncol_989_units_6bfe7e, dyn_ew_dyncol_990_units_b5f4f5, dyn_ew_dyncol_976_units_8375b1, dyn_ew_dyncol_977_units_c9c55e, dyn_ew_dyncol_978_units_3dc8fe, dyn_ew_dyncol_979_units_afd671, dyn_ew_dyncol_980_units_2661ac, dyn_ew_dyncol_981_units_5f9440, dyn_ew_dyncol_974_units_6347e4 
FROM (SELECT dynamiccolumnview.rowid_ AS dynamiccolumnview_rowid_, dynamiccolumnview.id AS dynamiccolumnview_id, dynamiccolumnview.contract_id AS dynamiccolumnview_contract_id, dynamiccolumnview.operator_id AS dynamiccolumnview_operator_id, dynamiccolumnview.operator_title AS dyn_view_operator_title_3a0d80, dynamiccolumnview.contractor_id AS dyn_nview_contractor_id_fb7c93, dynamiccolumnview.contractor_title AS dyn_ew_contractor_title_2139a6, dynamiccolumnview.principal_id AS dynamiccolumnview_principal_id, dynamiccolumnview.principal_title AS dyn_iew_principal_title_4698f3, dynamiccolumnview.fraction_id AS dynamiccolumnview_fraction_id, dynamiccolumnview.fraction_title AS dyn_view_fraction_title_55dacf, dynamiccolumnview.uom_id AS dynamiccolumnview_uom_id, dynamiccolumnview.uom_title AS dynamiccolumnview_uom_title, dynamiccolumnview.cur_sent AS dynamiccolumnview_cur_sent, dynamiccolumnview.cur_income_from_sold_fractions AS dyn_from_sold_fractions_54cbee, dynamiccolumnview.cur_received AS dynamiccolumnview_cur_received, dynamiccolumnview.cur_total_received AS dyn__cur_total_received_30a259, dynamiccolumnview.cur_total_sent AS dyn_view_cur_total_sent_f9fa6c, dynamiccolumnview.cur_mass_balance AS dyn_ew_cur_mass_balance_fc578d, dynamiccolumnview.prev_total_received AS dyn_prev_total_received_18dd0b, dynamiccolumnview.prev_total_sent AS dyn_iew_prev_total_sent_4cc2bc, dynamiccolumnview.prev_mass_balance AS dyn_w_prev_mass_balance_c44b56, dynamiccolumnview.dyncol_893_units AS dyn_ew_dyncol_893_units_7b31e5, dynamiccolumnview.dyncol_992_units AS dyn_ew_dyncol_992_units_d40b8b, dynamiccolumnview.dyncol_982_units AS dyn_ew_dyncol_982_units_03d4a0, dynamiccolumnview.dyncol_983_units AS dyn_ew_dyncol_983_units_1e3771, dynamiccolumnview.dyncol_985_units AS dyn_ew_dyncol_985_units_f31fbe, dynamiccolumnview.dyncol_984_units AS dyn_ew_dyncol_984_units_d61ab5, dynamiccolumnview.dyncol_989_units AS dyn_ew_dyncol_989_units_6bfe7e, dynamiccolumnview.dyncol_990_units AS dyn_ew_dyncol_990_units_b5f4f5, dynamiccolumnview.dyncol_976_units AS dyn_ew_dyncol_976_units_8375b1, dynamiccolumnview.dyncol_977_units AS dyn_ew_dyncol_977_units_c9c55e, dynamiccolumnview.dyncol_978_units AS dyn_ew_dyncol_978_units_3dc8fe, dynamiccolumnview.dyncol_979_units AS dyn_ew_dyncol_979_units_afd671, dynamiccolumnview.dyncol_980_units AS dyn_ew_dyncol_980_units_2661ac, dynamiccolumnview.dyncol_981_units AS dyn_ew_dyncol_981_units_5f9440, dynamiccolumnview.dyncol_974_units AS dyn_ew_dyncol_974_units_6347e4, ROW_NUMBER() OVER (ORDER BY dynamiccolumnview.id ASC, dynamiccolumnview.id ASC) AS ora_rn 
FROM (SELECT gre_massbalancetpview_1_23a4b2.rowid_ AS rowid_, gre_massbalancetpview_1_23a4b2.id AS id, gre_massbalancetpview_1_23a4b2.contract_id AS contract_id, gre_massbalancetpview_1_23a4b2.operator_id AS operator_id, gre_massbalancetpview_1_23a4b2.operator_title AS operator_title, gre_massbalancetpview_1_23a4b2.contractor_id AS contractor_id, gre_massbalancetpview_1_23a4b2.contractor_title AS contractor_title, gre_massbalancetpview_1_23a4b2.principal_id AS principal_id, gre_massbalancetpview_1_23a4b2.principal_title AS principal_title, gre_massbalancetpview_1_23a4b2.fraction_id AS fraction_id, gre_massbalancetpview_1_23a4b2.fraction_title AS fraction_title, gre_massbalancetpview_1_23a4b2.uom_id AS uom_id, gre_massbalancetpview_1_23a4b2.uom_title AS uom_title, gre_massbalancetpview_1_23a4b2.cur_sent AS cur_sent, gre_massbalancetpview_1_23a4b2.cur_income_from_sold_fractions AS cur_income_from_sold_fractions, gre_massbalancetpview_1_23a4b2.cur_received AS cur_received, gre_massbalancetpview_1_23a4b2.cur_total_received AS cur_total_received, gre_massbalancetpview_1_23a4b2.cur_total_sent AS cur_total_sent, gre_massbalancetpview_1_23a4b2.cur_mass_balance AS cur_mass_balance, gre_massbalancetpview_1_23a4b2.prev_total_received AS prev_total_received, gre_massbalancetpview_1_23a4b2.prev_total_sent AS prev_total_sent, gre_massbalancetpview_1_23a4b2.prev_mass_balance AS prev_mass_balance, coalesce(gre_currentperiodview_2_00fede.units, cast('0.0' as NUMERIC(10, 2))) AS dyncol_893_units, coalesce(gre_currentperiodview_3_f21812.units, cast('0.0' as NUMERIC(10, 2))) AS dyncol_992_units, coalesce(gre_currentperiodview_4_ad49c6.units, cast('0.0' as NUMERIC(10, 2))) AS dyncol_982_units, coalesce(gre_currentperiodview_5_c1b55c.units, cast('0.0' as NUMERIC(10, 2))) AS dyncol_983_units, coalesce(gre_currentperiodview_6_7f5922.units, cast('0.0' as NUMERIC(10, 2))) AS dyncol_985_units, coalesce(gre_currentperiodview_7_80f86c.units, cast('0.0' as NUMERIC(10, 2))) AS dyncol_984_units, coalesce(gre_currentperiodview_8_cc3db8.units, cast('0.0' as NUMERIC(10, 2))) AS dyncol_989_units, coalesce(gre_currentperiodview_9_286225.units, cast('0.0' as NUMERIC(10, 2))) AS dyncol_990_units, coalesce(gre_urrentperiodview_10_4446e3.units, cast('0.0' as NUMERIC(10, 2))) AS dyncol_976_units, coalesce(gre_urrentperiodview_11_741dde.units, cast('0.0' as NUMERIC(10, 2))) AS dyncol_977_units, coalesce(gre_urrentperiodview_12_c99ac3.units, cast('0.0' as NUMERIC(10, 2))) AS dyncol_978_units, coalesce(gre_urrentperiodview_13_dcbf9a.units, cast('0.0' as NUMERIC(10, 2))) AS dyncol_979_units, coalesce(gre_urrentperiodview_14_12c93a.units, cast('0.0' as NUMERIC(10, 2))) AS dyncol_980_units, coalesce(gre_urrentperiodview_15_ef90bc.units, cast('0.0' as NUMERIC(10, 2))) AS dyncol_981_units, coalesce(gre_urrentperiodview_16_314ef7.units, cast('0.0' as NUMERIC(10, 2))) AS dyncol_974_units 
FROM gre_e_massbalancetpview_456232 gre_massbalancetpview_1_23a4b2 LEFT OUTER JOIN gre_alcurrentperiodview_ac2862 gre_currentperiodview_2_00fede ON gre_currentperiodview_2_00fede.treatment_group_id = cast('893' as INTEGER) AND gre_massbalancetpview_1_23a4b2.contract_id = gre_currentperiodview_2_00fede.contract_id AND gre_massbalancetpview_1_23a4b2.fraction_id = gre_currentperiodview_2_00fede.fraction_id LEFT OUTER JOIN gre_alcurrentperiodview_ac2862 gre_currentperiodview_3_f21812 ON gre_currentperiodview_3_f21812.treatment_group_id = cast('992' as INTEGER) AND gre_massbalancetpview_1_23a4b2.contract_id = gre_currentperiodview_3_f21812.contract_id AND gre_massbalancetpview_1_23a4b2.fraction_id = gre_currentperiodview_3_f21812.fraction_id LEFT OUTER JOIN gre_alcurrentperiodview_ac2862 gre_currentperiodview_4_ad49c6 ON gre_currentperiodview_4_ad49c6.treatment_group_id = cast('982' as INTEGER) AND gre_massbalancetpview_1_23a4b2.contract_id = gre_currentperiodview_4_ad49c6.contract_id AND gre_massbalancetpview_1_23a4b2.fraction_id = gre_currentperiodview_4_ad49c6.fraction_id LEFT OUTER JOIN gre_alcurrentperiodview_ac2862 gre_currentperiodview_5_c1b55c ON gre_currentperiodview_5_c1b55c.treatment_group_id = cast('983' as INTEGER) AND gre_massbalancetpview_1_23a4b2.contract_id = gre_currentperiodview_5_c1b55c.contract_id AND gre_massbalancetpview_1_23a4b2.fraction_id = gre_currentperiodview_5_c1b55c.fraction_id LEFT OUTER JOIN gre_alcurrentperiodview_ac2862 gre_currentperiodview_6_7f5922 ON gre_currentperiodview_6_7f5922.treatment_group_id = cast('985' as INTEGER) AND gre_massbalancetpview_1_23a4b2.contract_id = gre_currentperiodview_6_7f5922.contract_id AND gre_massbalancetpview_1_23a4b2.fraction_id = gre_currentperiodview_6_7f5922.fraction_id LEFT OUTER JOIN gre_alcurrentperiodview_ac2862 gre_currentperiodview_7_80f86c ON gre_currentperiodview_7_80f86c.treatment_group_id = cast('984' as INTEGER) AND gre_massbalancetpview_1_23a4b2.contract_id = gre_currentperiodview_7_80f86c.contract_id AND gre_massbalancetpview_1_23a4b2.fraction_id = gre_currentperiodview_7_80f86c.fraction_id LEFT OUTER JOIN gre_alcurrentperiodview_ac2862 gre_currentperiodview_8_cc3db8 ON gre_currentperiodview_8_cc3db8.treatment_group_id = cast('989' as INTEGER) AND gre_massbalancetpview_1_23a4b2.contract_id = gre_currentperiodview_8_cc3db8.contract_id AND gre_massbalancetpview_1_23a4b2.fraction_id = gre_currentperiodview_8_cc3db8.fraction_id LEFT OUTER JOIN gre_alcurrentperiodview_ac2862 gre_currentperiodview_9_286225 ON gre_currentperiodview_9_286225.treatment_group_id = cast('990' as INTEGER) AND gre_massbalancetpview_1_23a4b2.contract_id = gre_currentperiodview_9_286225.contract_id AND gre_massbalancetpview_1_23a4b2.fraction_id = gre_currentperiodview_9_286225.fraction_id LEFT OUTER JOIN gre_alcurrentperiodview_ac2862 gre_urrentperiodview_10_4446e3 ON gre_urrentperiodview_10_4446e3.treatment_group_id = cast('976' as INTEGER) AND gre_massbalancetpview_1_23a4b2.contract_id = gre_urrentperiodview_10_4446e3.contract_id AND gre_massbalancetpview_1_23a4b2.fraction_id = gre_urrentperiodview_10_4446e3.fraction_id LEFT OUTER JOIN gre_alcurrentperiodview_ac2862 gre_urrentperiodview_11_741dde ON gre_urrentperiodview_11_741dde.treatment_group_id = cast('977' as INTEGER) AND gre_massbalancetpview_1_23a4b2.contract_id = gre_urrentperiodview_11_741dde.contract_id AND gre_massbalancetpview_1_23a4b2.fraction_id = gre_urrentperiodview_11_741dde.fraction_id LEFT OUTER JOIN gre_alcurrentperiodview_ac2862 gre_urrentperiodview_12_c99ac3 ON gre_urrentperiodview_12_c99ac3.treatment_group_id = cast('978' as INTEGER) AND gre_massbalancetpview_1_23a4b2.contract_id = gre_urrentperiodview_12_c99ac3.contract_id AND gre_massbalancetpview_1_23a4b2.fraction_id = gre_urrentperiodview_12_c99ac3.fraction_id LEFT OUTER JOIN gre_alcurrentperiodview_ac2862 gre_urrentperiodview_13_dcbf9a ON gre_urrentperiodview_13_dcbf9a.treatment_group_id = cast('979' as INTEGER) AND gre_massbalancetpview_1_23a4b2.contract_id = gre_urrentperiodview_13_dcbf9a.contract_id AND gre_massbalancetpview_1_23a4b2.fraction_id = gre_urrentperiodview_13_dcbf9a.fraction_id LEFT OUTER JOIN gre_alcurrentperiodview_ac2862 gre_urrentperiodview_14_12c93a ON gre_urrentperiodview_14_12c93a.treatment_group_id = cast('980' as INTEGER) AND gre_massbalancetpview_1_23a4b2.contract_id = gre_urrentperiodview_14_12c93a.contract_id AND gre_massbalancetpview_1_23a4b2.fraction_id = gre_urrentperiodview_14_12c93a.fraction_id LEFT OUTER JOIN gre_alcurrentperiodview_ac2862 gre_urrentperiodview_15_ef90bc ON gre_urrentperiodview_15_ef90bc.treatment_group_id = cast('981' as INTEGER) AND gre_massbalancetpview_1_23a4b2.contract_id = gre_urrentperiodview_15_ef90bc.contract_id AND gre_massbalancetpview_1_23a4b2.fraction_id = gre_urrentperiodview_15_ef90bc.fraction_id LEFT OUTER JOIN gre_alcurrentperiodview_ac2862 gre_urrentperiodview_16_314ef7 ON gre_urrentperiodview_16_314ef7.treatment_group_id = cast('974' as INTEGER) AND gre_massbalancetpview_1_23a4b2.contract_id = gre_urrentperiodview_16_314ef7.contract_id AND gre_massbalancetpview_1_23a4b2.fraction_id = gre_urrentperiodview_16_314ef7.fraction_id 
WHERE cast('1' as INTEGER) = cast('1' as INTEGER)) dynamiccolumnview 
WHERE (dynamiccolumnview.operator_id IN (cast('1214' as INTEGER), cast('833' as INTEGER), cast('1464' as INTEGER), cast('1542' as INTEGER), cast('1458' as INTEGER), cast('1468' as INTEGER), cast('1087' as INTEGER), cast('1088' as INTEGER), cast('17432' as INTEGER), cast('2677' as INTEGER), cast('2679' as INTEGER), cast('1572' as INTEGER), cast('1215' as INTEGER), cast('850' as INTEGER), cast('964' as INTEGER), cast('958' as INTEGER), cast('962' as INTEGER), cast('965' as INTEGER), cast('966' as INTEGER), cast('7799' as INTEGER), cast('2675' as INTEGER), cast('9216' as INTEGER), cast('9217' as INTEGER), cast('25185' as INTEGER), cast('8592' as INTEGER), cast('9208' as INTEGER), cast('9218' as INTEGER), cast('9212' as INTEGER)) OR dynamiccolumnview.contractor_id IN (cast('1214' as INTEGER), cast('833' as INTEGER), cast('1464' as INTEGER), cast('1542' as INTEGER), cast('1458' as INTEGER), cast('1468' as INTEGER), cast('1087' as INTEGER), cast('1088' as INTEGER), cast('17432' as INTEGER), cast('2677' as INTEGER), cast('2679' as INTEGER), cast('1572' as INTEGER), cast('1215' as INTEGER), cast('850' as INTEGER), cast('964' as INTEGER), cast('958' as INTEGER), cast('962' as INTEGER), cast('965' as INTEGER), cast('966' as INTEGER), cast('7799' as INTEGER), cast('2675' as INTEGER), cast('9216' as INTEGER), cast('9217' as INTEGER), cast('25185' as INTEGER), cast('8592' as INTEGER), cast('9208' as INTEGER), cast('9218' as INTEGER), cast('9212' as INTEGER)) OR dynamiccolumnview.principal_id IN (cast('1214' as INTEGER), cast('833' as INTEGER), cast('1464' as INTEGER), cast('1542' as INTEGER), cast('1458' as INTEGER), cast('1468' as INTEGER), cast('1087' as INTEGER), cast('1088' as INTEGER), cast('17432' as INTEGER), cast('2677' as INTEGER), cast('2679' as INTEGER), cast('1572' as INTEGER), cast('1215' as INTEGER), cast('850' as INTEGER), cast('964' as INTEGER), cast('958' as INTEGER), cast('962' as INTEGER), cast('965' as INTEGER), cast('966' as INTEGER), cast('7799' as INTEGER), cast('2675' as INTEGER), cast('9216' as INTEGER), cast('9217' as INTEGER), cast('25185' as INTEGER), cast('8592' as INTEGER), cast('9208' as INTEGER), cast('9218' as INTEGER), cast('9212' as INTEGER))) AND cast('1' as INTEGER) = cast('1' as INTEGER)) 
WHERE ora_rn > '0' AND ora_rn <= '20'
"""

#import time
#t1 = time.time()
#connection.execute(sql)
#print "time used by query", (time.time()-t1)


