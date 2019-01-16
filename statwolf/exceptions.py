class StatwolfException(Exception):
    pass

class InvalidConfigException(StatwolfException):
    def __init__(self):
        super(InvalidConfigException, self).__init__("Invalid config: the following parameters are mandatory.\n* host\n* username\n* password")

