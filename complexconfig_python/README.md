complexconfig
=============

Introduction
----
complexconfig is developed after [archaius](https://github.com/Netflix/archaius), which is an excellent config lib in java. complexconfig wants to provider a similar easy api for configuration usage, which can help us build better system programs with python


build config
----

complexconfig build config from **loader** and **parser**.

loader will load text data from different data sources:

1. file_loader: load data from file
2. web_loader: load data from web
3. string_loader: load data from a string
4. pyfile_loader: load data from python script


parser will parse config as dict from the text loaded by loader:

1. dict_parser: parse source text as dict
1. json_parser: parse source text as json
1. threathunter_json_parser: parse source text as json return value by threathunter api
1. ini_parser: parse source text as ini file
1. yaml_parser: parse source text as yaml file
1. properties_parser: parse source text as properties file

config can be built from parser and loader, like:

```python
from complexconfig.config import Config
from complexconfig.parser.properties_parser import PropertiesParser
from complexconfig.loader.file_loader import FileLoader
loader = FileLoader("loader", filename)
parser = PropertiesParser("parser")
c = Config(loader, parser)
c.load_config(sync=True)
```

config decorator
----

there are also some decorators offering more advanced functionalities:

**PeriodicalConfig** loads config data from source periodically

```python
c = Config(loader, parser)
pc = PeriodicalConfig(c, 3)    #load every 3 seconds
pc.load_config(sync=True)
```

**CascadingConfig** loads config from multiple data sources(currently we are using an inefficient implementation):

```python
# build file config
file_config = Config(loader, parser)
file_config.load_config(sync=True)

# build web config
web_config = Config(another_loader, another_parser)
web_config.load_config(sync=True)

#build cascading config
cascading_config = CascadingConfig(file_config, web_config)
```

config items
----

maybe the most useful thing is **config item**, see usage
```python
from complexconfig.configcontainer import configcontainer
configcontainer.set_config("myconfig", c)
int_item = configcontainer.get_config("myconfig").int_item("keyname", caching=10, default=1) # load every 10 seconds
print int_item.get()

# use a callback once the underlying raw config data is loaded
item = configcontainer.get_config("myconfig").item("keyname", caching=10, default=1, cb_load=lambda x: int(x)) # load every 10 seconds
print item.get()

```
