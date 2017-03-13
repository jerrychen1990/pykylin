from __future__ import absolute_import

import re

from .log import logger

field_pattern = r"(\s+)(.*)\s+AS\s+([a-z_]*)"
group_py_pattern = r"GROUP BY (.*?)\s+"
where_pattern = r"(WHERE|AND|OR)\s+(.*?)\s+"

table_pattern = r"FROM (.*?)\s"
date_pattern = r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}"


def transfer_sql(sql):
    table = get_table(sql)
    if table == "":
        return sql
    sql = re.sub(field_pattern, lambda x: transfer_field(x, table), sql)
    sql = re.sub(group_py_pattern, lambda x: transfer_group_by(x, table), sql)
    sql = re.sub(where_pattern, lambda x: transfer_where(x, table), sql)
    sql = re.sub(date_pattern, transfer_date, sql)

    sql = sql.upper()
    return sql


def transfer_date(date_sql):
    date_sql = date_sql.group(0)
    date_sql = date_sql.split(u" ")[0]
    if date_sql < "2016-01-01":
        date_sql = "2016-01-01"
    return date_sql


def get_table(sql):
    tables = re.findall(table_pattern, sql)
    if len(tables) < 1:
        logger.error("getting table name error!")
        return ""
    return tables[0]


def transfer_group_by(group_by, table):
    group_by = group_by.group(1)
    group_by = transfer_src(group_by, table)
    return "group by {}\n".format(group_by)


def transfer_where(where, table):
    cond = where.group(1)
    field = where.group(2)
    field = transfer_src(field, table)
    return "{} {}".format(cond, field)


def transfer_field(field, table):
    space = field.group(1)
    src = field.group(2)
    dst = field.group(3)
    rs = "{}{} AS {}".format(space, transfer_src(src, table), transfer_dst(dst))
    return rs


def transfer_src(src, table):
    if "(" not in src:
        src = '''"{}"."{}"'''.format(table, src)

    field_list = src.split("/")
    if len(field_list) == 2:
        src = "{}*1.0/{}".format(field_list[0], field_list[1])

    return src


def transfer_dst(dst):
    # if dst.upper() == 'COUNT':
    #     return "total_count"
    return '"{}"'.format(dst)


test_sql = u'''SELECT cat_name AS cat_name,
       sum(click)/sum(impression) AS click_rate
FROM DSP_ANALYSE_D
WHERE day >= '2017-03-06 15:27:53'
  AND day <= '2017-03-13 15:27:53'
GROUP BY cat_name
ORDER BY click_rate DESC LIMIT 50000'''

test_sql = transfer_sql(test_sql)

print(test_sql)
#
# items = re.findall(where_pattern, test_sql)
# for item in items:
#     print(item)
