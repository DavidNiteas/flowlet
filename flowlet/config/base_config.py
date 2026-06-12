from .behavior.branch_config import BranchConfigBehavior
from .behavior.json_config import JsonConfigBehavior
from .behavior.msgpack_config import MsgpackConfigBehavior
from .behavior.toml_config import TomlConfigBehavior


class BaseConfig(JsonConfigBehavior, MsgpackConfigBehavior, TomlConfigBehavior):
    pass


class BaseBranchConfig(BranchConfigBehavior, BaseConfig):
    pass
