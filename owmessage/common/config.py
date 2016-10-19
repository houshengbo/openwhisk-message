import ConfigParser

cfg_file = None

class ConfigOpts(ConfigParser.ConfigParser):
    def __init__(self):
        ConfigParser.ConfigParser.__init__(self)

    def __call__(self, *args, **kwargs):
        self.read(args[0])

CONF = ConfigOpts()