from __future__ import absolute_import

import re

from .log import logger

field_pattern = r"(\s+)(.*) AS ([a-z_]*)"
group_py_pattern = r"GROUP BY (.*)\s"

table_pattern = r"FROM (.*?)\s"


def transfer_sql(sql):
    table = get_table(sql)
    if table == "":
        return sql
    sql = re.sub(field_pattern, lambda x: transfer_field(x, table), sql)
    sql = re.sub(group_py_pattern, lambda x: transfer_group_by(x, table), sql)

    sql = sql.upper()
    return sql


def get_table(sql):
    tables = re.findall(table_pattern, sql)
    if len(tables) != 1:
        logger.error("getting table name error!")
        return ""
    return tables[0]


def transfer_group_by(group_by, table):
    group_by = group_by.group(1)
    group_by = transfer_src(group_by, table)
    return "group by {}\n".format(group_by)


def transfer_field(field, table):
    space = field.group(1)
    src = field.group(2)
    dst = field.group(3)
    rs = "{}{} AS {}".format(space, transfer_src(src, table), transfer_dst(dst))
    return rs


def transfer_src(src, table):
    if "(" not in src:
        src = '''"{}"."{}"'''.format(table, src)
    return src


def transfer_dst(dst):
    # if dst.upper() == 'COUNT':
    #     return "total_count"
    return '"{}"'.format(dst)


# test_sql = u'''SELECT day AS day,
#               SUM(click) AS sum_click
# FROM DSP_ANALYSE_D
# GROUP BY day
# ORDER BY sum_click DESC LIMIT 50000'''
#
# test_sql = transfer_sql(test_sql)
#
# print(test_sql)
#
# items = re.findall(group_py_pattern, test_sql)
# for item in items:
#     print(item)
