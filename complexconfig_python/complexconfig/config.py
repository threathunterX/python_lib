#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Config object that can help when you are using dynamic config
"""

from collections import Sequence, Mapping
import logging
import time
import six

from threathunter_common.util import run_in_thread, millis_now


LOGGER = logging.getLogger("config")


class ConfigItem(object):
    """
    A config item that has given key, and it can always give you almost latest value.
    """

    def __init__(self, config, key, caching=10, default=None, cb_load=None):
        """
        :param config: corresponding config object
        :param key: item key
        :param caching: if the value will be cached
        :param default: default value
        :param cb_load: callback once the raw value is loaded
        """

        self.config = config
        self.key = key
        self.default = default
        self.cb_load = cb_load
        self.caching = caching

        self.last_raw_value_from_config = None
        self.latest_value = None
        self.last_load = 0

    def get(self):
        """
        Get the almost latest config value
        :return: latest value than can be no older than caching seconds ago
        """

        now = millis_now()
        if self.caching > 0 and (now - self.last_load) / 1000 < self.caching:
            # have not expire yet
            return self.latest_value

        latest_raw_value = self.config.get_value(self.key)
        if latest_raw_value is None:
            self.last_raw_value_from_config = None
            self.latest_value = self.default
            return self.latest_value

        if latest_raw_value == self.last_raw_value_from_config:
            # config has not changed yet
            return self.latest_value

        latest_value = latest_raw_value
        if self.cb_load:
            latest_value = self.cb_load(latest_value)

        self.latest_value = latest_value
        self.last_raw_value_from_config = latest_raw_value
        self.last_load = now
        return self.latest_value


class Config(object):
    """
    Base class of the config object
    """

    def __init__(self, loader, parser, cb_after_load=None):
        """
        Build a config.
        :param loader: Loader that will load config content from local/remote resources
        :param parser: Parser that will parse config from specific format
        :param cb_after_load: callback called when the config is loaded
        :return:
        """

        self.loader = loader
        self.parser = parser
        self.cb_after_load = cb_after_load

        self.config_data = {}
        self.has_loaded = False

        # prevent multiple loading
        self.is_loading = False

    def get_config_data(self):
        """
        :return: the internal config data
        """

        return self.config_data

    def _load_config(self):
        """
        This method is used internally, while subclass can use load_config which is a wrapper
        :return:
        """
        try:
            # 0. parser parse
            self.config_data = self.parser.parse(self.loader.load())

            # 1. call back for further advanced functionality
            if self.cb_after_load:
                self.config_data = self.cb_after_load(self.config_data)

            self.has_loaded = True
        except Exception as err:
            LOGGER.error("config load error: %s", err)

        self.is_loading = False

    def invalidate_config(self):
        """
        invalidate the config data, so that it can load again.
        """
        self.has_loaded = False

    def load_config(self, sync=False):
        """
        load config.
        Be invoked at the first time or the config has been invalidated.
        :param sync: whether this is a synchronous invocation
        """
        if self.has_loaded:
            return

        if self.is_loading:
            # some one has already loaded
            if not sync:
                return
            else:
                attempts = 0
                while self.is_loading and attempts < 3:
                    attempts += 1
                    time.sleep(0.5)

                if attempts >= 3:
                    raise RuntimeError("fail to load config")

                # should be loaded successfully
                return

        self.is_loading = True
        if sync:
            self._load_config()
        else:
            run_in_thread(self._load_config)

    def get_value(self, key, default=None):
        """
        return config value

        :param key:
        :param default: default value if there is not one
        :return: value stored or the default value
        """

        # ensure we have loaded
        self.load_config()

        result = self.config_data.get(key)
        if result is not None:
            return result

        fields = key.split(".")
        current_node = self.config_data
        for f in fields:
            if not isinstance(current_node, Mapping) or f not in current_node:
                return default
            current_node = current_node[f]

        return current_node

    def item(self, key, caching=10, default=None, cb_load=None):
        """
        get a config item
        :param key: config item key
        :param caching: caching period, config item will get new config value from underlying config
        :param default: default value if the config value is None
        :param cb_load: callback will be executed on the config value from the underlying config
        :return: almost latest value
        """
        return ConfigItem(self, key, caching, default, cb_load)

    # #####helper methods from here######
    # item parts, return useful config item
    def int_item(self, key, caching=10, default=None):
        """
        short version for the item whose value is integer
        """

        return self.item(key, caching, default, int)

    def str_item(self, key, caching=10, default=None):
        """
        short version for the item whose value is string
        """

        return self.item(key, caching, default, str)

    def list_item(self, key, caching=10, default=None):
        """
        short version for the item whose value is a list and the raw value is separated by
        comma.
        """

        if default is None:
            default = list()

        def list_cb(raw):
            """
            change string to a list
            """

            if isinstance(raw, six.text_type) or isinstance(raw, six.binary_type):
                return raw.split(",")
            elif isinstance(raw, Sequence):
                return raw
            else:
                return []

        return self.item(key, caching, default, list_cb)

    def boolean_item(self, key, caching=10, default=None):
        """
        short version for the item whose value is boolean
        """

        def boolean_cb(raw):
            """
            change string to a boolean value
            """

            if isinstance(raw, bool):
                return raw
            if raw.lower() in ("yes", "true", "y", "t", "true"):
                return True

            if raw.lower() in ("no", "false", "n", "f", "false"):
                return False

        return self.item(key, caching, default, boolean_cb)

    # useful functions to get value of key
    def get_dict(self, key, default=None):
        """
        get dict value of a key from the config
        :param key: config key
        :param default: default value
        """

        if default is None:
            default = dict()

        value = self.get_value(key)
        if value is None:
            return default

        return value

    def get_string(self, key, default=None):
        """
        get string value of a key from the config
        :param key: config key
        :param default: default value
        """

        value = self.get_value(key)
        if value is None:
            return default

        return str(value)

    def get_int(self, key, default=0):
        """
        get int value of a key from the config
        :param key: config key
        :param default: default value
        """

        value = self.get_value(key)
        if value is None:
            return default

        return int(value)

    def get_boolean(self, key, default=False):
        """
        get boolean value of a key from the config
        :param key: config key
        :param default: default value
        """

        value = self.get_value(key)
        if value is None:
            return default

        if isinstance(value, bool):
            return value
        if value.lower() in ("yes", "true", "y", "t", "true"):
            return True

        if value.lower() in ("no", "false", "n", "f", "false"):
            return False

        raise RuntimeError("invalid config item({}) for boolean".format(value))

    def get_list(self, key, default=None):
        """
        get list value of a key from the config
        :param key: config key
        :param default: default value
        """

        value = self.get_value(key)
        if value is None:
            return default

        if isinstance(value, six.text_type) or isinstance(value, six.binary_type):
            return value.split(",")
        elif isinstance(value, Sequence):
            return value
        else:
            return []


class PeriodicalConfig(Config):
    """
    Config that will load config periodically.
    """

    def __init__(self, config, period):
        """
        :param config: the decorated config object
        :param period: the period that the config could expire(in seconds)
        """
        parent = self
        super(PeriodicalConfig, self).__init__(None, None,
                                               cb_after_load=lambda: parent.__setattr__("last_load_ts", millis_now()))

        assert config, "config is null"
        assert isinstance(config, Config), "should be an Config instance"

        self.period = period
        self.config = config

        # the timestamp for last loading
        self.last_load_ts = 0

    def load_config(self, sync=False):
        """
        overwritten the load_config, so that the config will be updated periodically
        """
        now = millis_now()
        if (now - self.last_load_ts) / 1000 < self.period:
            return

        self.last_load_ts = now  # check other thread will not run again
        self.config.invalidate_config()
        self.config.load_config(sync)

    def get_value(self, key, default=None):
        self.load_config()
        result = self.config.get_value(key)
        if result is None:
            return default
        else:
            return result


class CascadingConfig(Config):

    def __init__(self, *configs):
        """
        Cascading a series of config instance
        :param configs: decorated configs
        :return:
        """

        super(CascadingConfig, self).__init__(None, None)
        assert configs, "at least one config"
        for config in configs:
            assert isinstance(config, Config), "not valid config"

        self.configs = configs

    def load_config(self, sync=False):
        def _load():
            for config in self.configs:
                config.load_config(sync=True)

        if sync:
            _load()
        else:
            run_in_thread(_load)

    def get_value(self, key, default=None):
        # TODO: this is an inefficient implementation

        # ensure we have loaded
        for c in self.configs[::-1]:
            result = c.get_value(key)
            if result is not None:
                return result

        return default


class PrefixConfig(Config):
    """
    automatically add a prefix of the key.
    """

    def __init__(self, config, prefix):
        """
        Using predefined prefix, so the key in the following queries will
        prepended the prefix automatically.

        :param config: decorated config
        :param prefix: predefined prefix
        :return:
        """

        super(PrefixConfig, self).__init__(None, None)
        assert config, "config is null"
        assert prefix, 'prefix should not be null'
        assert isinstance(config, Config), "should be an Config instance"

        self.config = config
        self.prefix = prefix

    def load_config(self, sync=False):
        return self.config.load_config(sync)

    def get_value(self, key, default=None):
        new_key = '{}.{}'.format(self.prefix, key)
        return self.config.get_value(new_key, default=default)


class EmptyConfig(Config):
    """
    Empty config
    """

    def __init__(self):
        super(EmptyConfig, self).__init__(None, None)

    def get_value(self, key, default=None):
        return default


class MockConfig(Config):
    """
    Dumb config for ease of test.
    """

    def __init__(self, data):
        super(MockConfig, self).__init__(None, None)
        self.data = {}
        self.data.update(data)

    def get_value(self, key, default=None):
        """
        Get the config
        """

        return self.data.get(key, default)

    def set_value(self, key, value):
        """
        update config
        """

        self.data[key] = value
