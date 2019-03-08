#!/usr/bin/env python
# -*- encoding: utf-8 -*-

import base64
import copy
import datetime
import functools
import hashlib
import ipaddr
import json
import logging
import math
import re
import six
import time
import types
import urllib
import uuid

from six import iteritems

logger = logging.getLogger('threathunter_common.util')

__author__ = 'lw'

md5string = lambda x: hashlib.md5(utf8(x)).hexdigest()


def province_filter(province):
    """
    去掉省份 市等信息
    """
    return province.replace(u"市", '').replace(u"省", '').replace(u"自治区", '').replace(u"维吾尔自治区", '').replace(u"特别行政区",
                                                                                                           '').replace(
        u"回族自治区", '')


def chunks(listdata, n):
    """
    Author:wxt

    将一个列表的数据平分
    """
    m = int(math.ceil(len(listdata) / float(n)))
    return [listdata[i:i + m] for i in range(0, len(listdata), m)]


def get_root_domain(url):
    """
    Author:wxt
    获取url的根域名
    """
    import re
    reg = r'^https?:\/\/([a-z0-9\-\.]+)[\/\?]?'
    m = re.match(reg, url)
    if m:
        uri = m.groups()[0]
    else:
        uri = url
        uri = uri[:uri.find('/')]
    return uri[uri.rfind('.', 0, uri.rfind('.')) + 1:]


def random16bit():
    """
    Author:wxt
    返回随机16位字符串
    """
    return hashlib.md5(str(uuid.uuid4())).hexdigest().upper()[:16]


class AttribDict(dict):
    """
    Author:wxt

    使用.遍历value
    >>> foo = AttribDict()
    >>> foo.bar = 1
    >>> foo.bar
    1
    """

    def __init__(self, indict=None, attribute=None):
        if indict is None:
            indict = {}

        # Set any attributes here - before initialisation
        # these remain as normal attributes
        self.attribute = attribute
        dict.__init__(self, indict)
        self.__initialised = True

        # After initialisation, setting attributes
        # is the same as setting an item

    def __getattr__(self, item):
        """
        Maps values to attributes
        Only called if there *is NOT* an attribute with this name
        """

        try:
            ret = self.__getitem__(item)
            if hasattr(ret, '__get__'):
                return ret.__get__(self, AttribDict)
            return ret
        except KeyError:
            return None
            # raise Exception("unable to access item '%s'" % item)

    def __setattr__(self, item, value):
        """
        Maps attributes to values
        Only if we are initialised
        """

        # This test allows attributes to be set in the __init__ method
        if "_AttribDict__initialised" not in self.__dict__:
            return dict.__setattr__(self, item, value)

        # Any normal attributes are handled normally
        elif item in self.__dict__:
            dict.__setattr__(self, item, value)

        else:
            self.__setitem__(item, value)

    def __getstate__(self):
        return self.__dict__

    def __setstate__(self, dict):
        self.__dict__ = dict

    def __deepcopy__(self, memo):
        retVal = self.__class__()
        memo[id(self)] = retVal

        for attr in dir(self):
            if not attr.startswith('_'):
                value = getattr(self, attr)
                if not isinstance(value, (
                        types.BuiltinFunctionType, types.BuiltinFunctionType, types.FunctionType, types.MethodType)):
                    setattr(retVal, attr, copy.deepcopy(value, memo))

        for key, value in self.items():
            retVal.__setitem__(key, copy.deepcopy(value, memo))

        return retVal


def cn_name_match(cn_name):
    """
        中文姓名合法性验证
    """
    chre = re.compile(r'^[\u2e80-\ufe4f]+([\u00b7][\u2e80-\ufe4f]+)*$')
    cn_name = unicode(cn_name.decode("utf-8"))

    mat = chre.match(cn_name)

    if mat and 1 < len(cn_name) <= 30:
        return True
    else:
        return False


def ip_match(ip, check_public=False):
    """
    Author: wxt
    IP地址验证
    @param check_public True：验证公网地址
    """
    ip_exp = re.compile(
        "^(25[0-5]|2[0-4][0-9]|[0-1]{1}[0-9]{2}|[1-9]{1}[0-9]{1}|[1-9])\.(25[0-5]|2[0-4][0-9]|[0-1]{1}[0-9]{2}|[1-9]{1}[0-9]{1}|[1-9]|0)\.(25[0-5]|2[0-4][0-9]|[0-1]{1}[0-9]{2}|[1-9]{1}[0-9]{1}|[1-9]|0)\.(25[0-5]|2[0-4][0-9]|[0-1]{1}[0-9]{2}|[1-9]{1}[0-9]{1}|[0-9])$")
    if ip_exp.match(ip):
        if check_public:
            _ip = ipaddr.IPAddress(ip)
            if _ip.is_link_local or _ip.is_loopback or _ip.is_private:
                return False
        return True
    else:
        return False


def mobile_match(mobile):
    """
    Author:wxt
    手机号码验证
    """
    # mobile_exp = re.compile("^(13[0-9]|14[01345789]|15[0-9]|17[012356789]|18[0-9])[0-9]{8}$")
    # mobile_exp = re.compile("^(13[0-9]|14[01345789]|15[0-9]|17[012356789]|18[0-9]|199)[0-9]{8}$")
    mobile_exp = re.compile("^(13[0-9]|14[01345789]|15[0-9]|16[6]|17[012356789]|18[0-9]|19[89])[0-9]{8}$")

    if mobile_exp.match(mobile):
        return True
    else:
        return False


def identity_card_match(id_number):
    """
    身份证号码验证
    :param id_number:
    :return: Boolean
    """
    if type(id_number) is int:
        id_number = str(id_number)
    if type(id_number) is str:
        try:
            int(id_number[:17])
        except ValueError:
            return False

    regex = r'^(^[1-9]\d{7}((0\d)|(1[0-2]))(([0|1|2]\d)|3[0-1])\d{3}$)|(^[1-9]\d{5}[1-9]\d{3}((0\d)|(1[0-2]))(([0|1|2]\d)|3[0-1])((\d{4})|\d{3}[Xx])$)$'

    if len(re.findall(regex, id_number)) == 0:
        return False
    if len(id_number) == 15:
        return True
    if len(id_number) == 18:
        Wi = [7, 9, 10, 5, 8, 4, 2, 1, 6, 3, 7, 9, 10, 5, 8, 4, 2]
        Ti = ['1', '0', 'x', '9', '8', '7', '6', '5', '4', '3', '2']

        sum = 0
        code = id_number[:17]

        for i in range(17):
            sum += int(code[i]) * Wi[i]

        if id_number[17:].lower() == Ti[sum % 11]:
            return True

    return False


class I18n(object):
    """
    Author: wxt
    翻译模块
    """
    default_word_dict = {
        "": "空"
    }

    def __init__(self):
        pass

    def set_word_dict(self, dict_key="", word_dict={}):
        setattr(self, dict_key, word_dict)

    def translate(self, word, word_dict=None):
        if not word_dict:
            word_dict = self.default_word_dict
        elif isinstance(word_dict, dict):
            pass
        else:
            word_dict = getattr(self, word_dict)
        if word_dict.has_key(word):
            return word_dict[word]
        else:
            return word


def curr_timestamp():
    """
    Author: wxt

    返回当前10位时间戳
    """
    return int(time.time())


def json_dumps(obj, pretty=False):
    return json.dumps(obj, sort_keys=True, indent=4, separators=(',', ': ')) if pretty else json.dumps(obj)


def get_system_encoding():
    import codecs
    import locale
    return codecs.lookup(locale.getpreferredencoding(())).name


class ReadOnlyDict(dict):
    """A Read Only Dict"""

    def __setitem__(self, key, value):
        raise Exception("dict is read-only")


def getitem(obj, key=0, default=None):
    """Get first element of list or return default"""
    try:
        return obj[key]
    except:
        return default


def hide_me(tb, g=globals()):
    """Hide stack traceback of given stack"""
    base_tb = tb
    try:
        while tb and tb.tb_frame.f_globals is not g:
            tb = tb.tb_next
        while tb and tb.tb_frame.f_globals is g:
            tb = tb.tb_next
    except Exception as e:
        logging.exception(e)
        tb = base_tb
    if not tb:
        tb = base_tb
    return tb


def run_in_thread(func, *args, **kwargs):
    """Run function in thread, return a Thread object"""
    from threading import Thread
    thread = Thread(target=func, args=args, kwargs=kwargs)
    thread.daemon = True
    thread.start()
    return thread


def run_in_subprocess(func, *args, **kwargs):
    """Run function in subprocess, return a Process object"""
    from multiprocessing import Process
    thread = Process(target=func, args=args, kwargs=kwargs)
    thread.daemon = True
    thread.start()
    return thread


def format_date(date, gmt_offset=0, relative=True, shorter=False, full_format=False):
    """Formats the given date (which should be GMT).
    By default, we return a relative time (e.g., "2 minutes ago"). You
    can return an absolute date string with ``relative=False``.
    You can force a full format date ("July 10, 1980") with
    ``full_format=True``.
    This method is primarily intended for dates in the past.
    For dates in the future, we fall back to full format.
    From tornado
    """
    if not date:
        return '-'
    if isinstance(date, float) or isinstance(date, int):
        date = datetime.datetime.utcfromtimestamp(date)
    now = datetime.datetime.utcnow()
    if date > now:
        if relative and (date - now).seconds < 60:
            # Due to click skew, things are some things slightly
            # in the future. Round timestamps in the immediate
            # future down to now in relative mode.
            date = now
        else:
            # Otherwise, future dates always use the full format.
            full_format = True
    local_date = date - datetime.timedelta(minutes=gmt_offset)
    local_now = now - datetime.timedelta(minutes=gmt_offset)
    local_yesterday = local_now - datetime.timedelta(hours=24)
    difference = now - date
    seconds = difference.seconds
    days = difference.days

    format = None
    if not full_format:
        if relative and days == 0:
            if seconds < 50:
                return ("1 second ago" if seconds <= 1 else
                        "%(seconds)d seconds ago") % {"seconds": seconds}

            if seconds < 50 * 60:
                minutes = round(seconds / 60.0)
                return ("1 minute ago" if minutes <= 1 else
                        "%(minutes)d minutes ago") % {"minutes": minutes}

            hours = round(seconds / (60.0 * 60))
            return ("1 hour ago" if hours <= 1 else
                    "%(hours)d hours ago") % {"hours": hours}

        if days == 0:
            format = "%(time)s"
        elif days == 1 and local_date.day == local_yesterday.day and \
                relative:
            format = "yesterday" if shorter else "yesterday at %(time)s"
        elif days < 5:
            format = "%(weekday)s" if shorter else "%(weekday)s at %(time)s"
        elif days < 334:  # 11mo, since confusing for same month last year
            format = "%(month_name)s-%(day)s" if shorter else \
                "%(month_name)s-%(day)s at %(time)s"

    if format is None:
        format = "%(month_name)s %(day)s, %(year)s" if shorter else \
            "%(month_name)s %(day)s, %(year)s at %(time)s"

    str_time = "%d:%02d" % (local_date.hour, local_date.minute)

    return format % {
        "month_name": local_date.month - 1,
        "weekday": local_date.weekday(),
        "day": str(local_date.day),
        "year": str(local_date.year),
        "time": str_time
    }


class TimeoutError(Exception):
    pass


try:
    import signal

    if not hasattr(signal, 'SIGALRM'):
        raise ImportError('signal')


    class timeout:
        """
        Time limit of command
        with timeout(3):
            time.sleep(10)
        """

        def __init__(self, seconds=1, error_message='Timeout'):
            self.seconds = seconds
            self.error_message = error_message

        def handle_timeout(self, signum, frame):
            raise TimeoutError(self.error_message)

        def __enter__(self):
            if self.seconds:
                signal.signal(signal.SIGALRM, self.handle_timeout)
                signal.alarm(self.seconds)

        def __exit__(self, type, value, traceback):
            if self.seconds:
                signal.alarm(0)
except ImportError:
    class timeout:
        """
        Time limit of command (for windows)
        """

        def __init__(self, seconds=1, error_message='Timeout'):
            pass

        def __enter__(self):
            pass

        def __exit__(self, type, value, traceback):
            pass


def utf8(string):
    """
    Make sure string is utf8 encoded bytes.
    If parameter is a object, object.__str__ will been called before encode as bytes
    """
    if string is None:
        return string
    if isinstance(string, six.text_type):
        return string.encode('utf8')
    elif isinstance(string, six.binary_type):
        return string
    elif isinstance(string, bytearray):
        return six.binary_type(string)
    else:
        return unicode(string).encode('utf8')


def binary_data(string):
    """
    Make sure the result is a bytearray.
    If parameter is a object, object.__str__ will been called before encode as bytes
    """
    if string is None:
        return string
    if isinstance(string, six.text_type):
        return bytearray(string.encode('utf8'))
    elif isinstance(string, six.binary_type):
        return bytearray(string)
    elif isinstance(string, bytearray):
        return string
    else:
        return bytearray(unicode(string).encode('utf8'))


def text(string, encoding='utf8'):
    """
    Make sure string is unicode type, decode with given encoding if it's not.
    If parameter is a object, object.__str__ will been called
    """
    if string is None:
        return string
    if isinstance(string, six.text_type):
        return string
    elif isinstance(string, six.binary_type):
        return string.decode(encoding)
    elif isinstance(string, bytearray):
        return six.binary_type(string).decode(encoding)
    else:
        return six.text_type(string)


def pretty_unicode(string):
    """
    Make sure string is unicode, try to decode with utf8, or unicode escaped string if failed.
    """
    if isinstance(string, six.text_type):
        return string
    try:
        return string.decode("utf8")
    except UnicodeDecodeError:
        return string.decode('Latin-1').encode('unicode_escape')


def unicode_string(string):
    """
    Make sure string is unicode, try to default with utf8, or base64 if failed.
    can been decode by `decode_unicode_string`
    """
    if string is None:
        return string
    if isinstance(string, six.text_type):
        return string
    try:
        return string.decode("utf8")
    except UnicodeDecodeError:
        return '[BASE64-DATA]' + base64.b64encode(string) + '[/BASE64-DATA]'


def unicode_dict(_dict):
    """
    Make sure keys and values of dict is unicode.
    """
    r = {}
    for k, v in iteritems(_dict):
        r[unicode_string(k)] = unicode_obj(v)
    return r


def unicode_list(_list):
    """
    Make sure every element in list is unicode. bytes will encode in base64
    """
    return [unicode_obj(x) for x in _list]


def unicode_obj(obj):
    """
    Make sure keys and values of dict/list/tuple is unicode. bytes will encode in base64.
    Can been decode by `decode_unicode_obj`
    """
    if obj is None:
        return obj
    if isinstance(obj, dict):
        return unicode_dict(obj)
    elif isinstance(obj, (list, tuple)):
        return unicode_list(obj)
    elif isinstance(obj, six.string_types):
        return unicode_string(obj)
    elif isinstance(obj, (int, float)):
        return obj
    elif obj is None:
        return obj
    else:
        return obj


def decode_unicode_string(string):
    """
    Decode string encoded by `unicode_string`
    """
    if string.startswith('[BASE64-DATA]') and string.endswith('[/BASE64-DATA]'):
        return base64.b64decode(string[len('[BASE64-DATA]'):-len('[/BASE64-DATA]')])
    return string


def decode_unicode_obj(obj):
    """
    Decode unicoded dict/list/tuple encoded by `unicode_obj`
    """
    if isinstance(obj, dict):
        r = {}
        for k, v in iteritems(obj):
            r[decode_unicode_string(k)] = decode_unicode_obj(v)
        return r
    elif isinstance(obj, six.string_types):
        return decode_unicode_string(obj)
    elif isinstance(obj, (list, tuple)):
        return [decode_unicode_obj(x) for x in obj]
    else:
        return obj


class Get(object):
    """
    Lazy value calculate for object
    """

    def __init__(self, getter):
        self.getter = getter

    def __get__(self, instance, owner):
        return self.getter()


class ObjectDict(dict):
    """
    Object like dict, every dict[key] can visite by dict.key
    If dict[key] is `Get`, calculate it's value.
    """

    def __getattr__(self, name):
        ret = self.__getitem__(name)
        if hasattr(ret, '__get__'):
            return ret.__get__(self, ObjectDict)
        return ret


def load_object(name):
    """Load object from module"""

    if "." not in name:
        raise Exception('load object need module.object')

    module_name, object_name = name.rsplit('.', 1)
    if six.PY2:
        module = __import__(module_name, globals(), locals(), [utf8(object_name)], -1)
    else:
        module = __import__(module_name, globals(), locals(), [object_name])
    return getattr(module, object_name)


def get_python_console(namespace=None):
    """
    Return a interactive python console instance with caller's stack
    """

    if namespace is None:
        import inspect
        frame = inspect.currentframe()
        caller = frame.f_back
        if not caller:
            logging.error("can't find caller who start this console.")
            caller = frame
        namespace = dict(caller.f_globals)
        namespace.update(caller.f_locals)

    try:
        from IPython.terminal.interactiveshell import TerminalInteractiveShell
        shell = TerminalInteractiveShell(user_ns=namespace)
    except ImportError:
        try:
            import readline
            import rlcompleter
            readline.set_completer(rlcompleter.Completer(namespace).complete)
            readline.parse_and_bind("tab: complete")
        except ImportError:
            pass
        import code
        shell = code.InteractiveConsole(namespace)
        shell._quit = False

        def exit():
            shell._quit = True

        def readfunc(prompt=""):
            if shell._quit:
                raise EOFError
            return six.moves.input(prompt)

        # inject exit method
        shell.ask_exit = exit
        shell.raw_input = readfunc

    return shell


def python_console(namespace=None):
    """Start a interactive python console with caller's stack"""

    if namespace is None:
        import inspect
        frame = inspect.currentframe()
        caller = frame.f_back
        if not caller:
            logging.error("can't find caller who start this console.")
            caller = frame
        namespace = dict(caller.f_globals)
        namespace.update(caller.f_locals)

    return get_python_console(namespace=namespace).interact()


def millis_now():
    return int(time.time() * 1000)


def gen_uuid():
    return str(uuid.uuid4())[-12:]


def asciifyUrl(url, forceQuote=False):
    """
    Attempts to make a unicode URL usuable with ``urllib/urllib2``.

    More specifically, it attempts to convert the unicode object ``url``,
    which is meant to represent a IRI, to an unicode object that,
    containing only ASCII characters, is a valid URI. This involves:

        * IDNA/Puny-encoding the domain name.
        * UTF8-quoting the path and querystring parts.

    See also RFC 3987.

    Reference: http://blog.elsdoerfer.name/2008/12/12/opening-iris-in-python/

    >>> asciifyUrl(u'http://www.\u0161u\u0107uraj.com')
    u'http://www.xn--uuraj-gxa24d.com'
    """
    import urlparse
    parts = urlparse.urlsplit(url)
    if not parts.scheme or not parts.netloc:
        # apparently not an url
        return url

    # idna-encode domain
    hostname = parts.hostname.encode("idna")

    # UTF8-quote the other parts. We check each part individually if
    # if needs to be quoted - that should catch some additional user
    # errors, say for example an umlaut in the username even though
    # the path *is* already quoted.
    def quote(s, safe, forceQuote=False):
        s = s or ''
        # Triggers on non-ascii characters - another option would be:
        #     urllib.quote(s.replace('%', '')) != s.replace('%', '')
        # which would trigger on all %-characters, e.g. "&".
        if s.encode("ascii", "replace") != s or forceQuote:
            return urllib.quote(s.encode('utf8'), safe=safe)
        return s

    username = quote(parts.username, '')
    password = quote(parts.password, safe='')
    path = quote(parts.path, safe='/')
    query = quote(parts.query, safe="&=;/", forceQuote=True)

    # put everything back together
    netloc = hostname
    if username or password:
        netloc = '@' + netloc
        if password:
            netloc = ':' + password + netloc
        netloc = username + netloc

    if parts.port:
        netloc += ':' + str(parts.port)

    return urlparse.urlunsplit([parts.scheme, netloc, path, query, parts.fragment])


def simweb(url, data=None, is_json=False, timeout=5):
    """
    @url:请求URL
    @data:如果不为空使用POST并指定post数据为data
    @is_json: 如果为True,会将data的数据json.dumps
    @timeout:设置超时时间
    return: 返回格式  source,code 二元组
    """
    try:
        import urllib2
        opener = urllib2.build_opener()
        url = asciifyUrl(url)
        if data:
            if is_json:
                data = json.dumps(data)
            Req = urllib2.Request(url, data)
            Req.add_header('Content-type', "application/x-www-form-urlencoded")
        else:
            Req = urllib2.Request(url)

        Res = opener.open(Req, timeout=timeout)
        source = Res.read()
        code = Res.getcode()
        return source, code
    except Exception as e:
        logger.warn("Http Request Error:%s", e)
        return None, None


def elapsed(logger=None, verbose="elapsed time: %f s"):
    """
    Tell us a function performed how many milliseconds.
    if logger is given,it will write in the logger,otherwith
    it will use print method.
    the verbose is logger format,must have a %f falg.
    @elapsed
    def fun():
        pass
    """

    def _elapsed(func):
        @functools.wraps(func)
        def _elapsed(*args, **kw):
            start = time.time()
            func(*args, **kw)
            end = time.time()
            secs = end - start
            msecs = secs * 1000
            if logger:
                logger.debug(verbose % secs)
            else:
                print(verbose % secs)

        return _elapsed

    return _elapsed
