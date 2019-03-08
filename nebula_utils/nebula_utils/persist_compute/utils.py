# -*- coding: utf-8 -*-

Group_Key_To_Dimension = dict(
    c_ip = 'ip',
    uid = 'user',
    page = 'page',
    did = 'did',
#    c_ipc = 'ipc',
)

Avail_Dimensions = tuple(Group_Key_To_Dimension.values())

# dimension : variable_name(获取点击量的变量名)
Click_Variable_Names = dict(
    ip='ip__visit__dynamic_count__1h__slot',
    did='did__visit__dynamic_count__1h__slot',
    user='user__visit__dynamic_count__1h__slot',
    page='page__visit__dynamic_count__1h__slot'
)

IP_Stat_Type = 2
IPC_Stat_Type = 3
DID_Stat_Type = 4
UID_Stat_Type = 5
PAGE_Stat_Type = 6

Dimension_Stat_Prefix = dict(
    ip = IP_Stat_Type,
    ipc = IPC_Stat_Type,
    did = DID_Stat_Type,
    user = UID_Stat_Type,
    page = PAGE_Stat_Type,
)

Category = ['VISITOR', 'ACCOUNT', 'ORDER',
            'TRANSACTION', 'MARKETING', 'OTHER']


Scene_Variable_Names = dict(
    VISITOR='total__visit__visitor_incident_count__1h__slot',
    ACCOUNT='total__visit__account_incident_count__1h__slot',
    ORDER='total__visit__order_incident_count__1h__slot',
    TRANSACTION='total__visit__transaction_incident_count__1h__slot',
    MARKETING='total__visit__marketing_incident_count__1h__slot',
    OTHER='total__visit__other_incident_count__1h__slot'
)


def get_dimension(group_key_name):
    """
    根据groupby的key获取对应统计Stat_Dict中维度的key值
    """
    return Group_Key_To_Dimension.get(group_key_name, None)

def dict_merge(src_dict, dst_dict):
    """
    将两个dict中的数据对应键累加, 
    不同类型值的情况:
    >>> s = dict(a=1,b='2')
    >>> d = {'b': 3, 'c': 4}
    >>> dict_merge(s,d)
    >>> t = {'a': 1, 'b': 5, 'c': 4}
    >>> s == t
    True
    >>> s = dict(a=set([1,2]), )
    >>> d = dict(a=set([2, 3]),)
    >>> dict_merge(s,d)
    >>> t = {'a':set([1,2,3])}
    >>> s == t
    True
    >>> s = dict(a={'a':1, 'b':2})
    >>> d = dict(a={'a':1, 'b':2})
    >>> dict_merge(s, d)
    >>> t = dict(a={'a':2, 'b':4})
    >>> s == t
    True
    """
    for k,v in dst_dict.iteritems():
        if not src_dict.has_key(k):
            src_dict[k] = v
        else:
            
            if isinstance(v, (basestring, int, float)):
                src_dict[k] = int(v) + int(src_dict[k])
            elif isinstance(v, set):
                assert type(v) == type(src_dict[k]), 'key %s,dst_dict value: %s type: %s, src_dict value: %s type:%s' % (k, v, type(v), src_dict[k], type(src_dict[k]))
                src_dict[k].update(v)
            elif isinstance(v, dict):
                assert type(v) == type(src_dict[k]), 'key %s,dst_dict value: %s type: %s, src_dict value: %s type:%s' % (k, v, type(v), src_dict[k], type(src_dict[k]))
                dict_merge(src_dict[k], v)
