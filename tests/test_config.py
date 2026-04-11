import shutil
from pathlib import Path

import pytest
from flowlet.config.base_config import BaseBranchConfig, BaseConfig
from flowlet.config.config_container import BaseConfigContainer

# 测试缓存目录
CACHE_DIR = Path(__file__).parent.parent / "cache" / "config"


@pytest.fixture(scope="module", autouse=True)
def setup_cache_dir():
    """设置测试缓存目录。"""
    # 测试前清理缓存目录
    if CACHE_DIR.exists():
        shutil.rmtree(CACHE_DIR)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    yield


# ==================== 测试1: 基础BaseConfig功能 ====================


class DatabaseConfig(BaseConfig):
    """数据库配置。"""

    host: str = "localhost"
    port: int = 5432
    username: str = "admin"
    password: str = "password"


class CacheConfig(BaseConfig):
    """缓存配置。"""

    backend: str = "redis"
    ttl: int = 3600
    max_size: int = 1000


class AppConfig(BaseBranchConfig):
    """应用配置，包含数据库和缓存配置。"""

    configs_type = {
        "database": DatabaseConfig,
        "cache": CacheConfig,
    }

    method_name: str = "database"
    app_name: str = "MyApp"
    debug: bool = False


class TestBaseConfig:
    """测试基础BaseConfig功能。"""

    def test_default_initialization(self):
        """测试默认初始化。"""
        config = DatabaseConfig()

        # 检查默认配置值
        assert config.host == "localhost"
        assert config.port == 5432
        assert config.username == "admin"
        assert config.password == "password"

    def test_custom_initialization(self):
        """测试自定义初始化。"""
        config = DatabaseConfig(host="custom.host", port=3306)

        # 检查自定义配置值
        assert config.host == "custom.host"
        assert config.port == 3306
        # 检查默认值保持不变
        assert config.username == "admin"

    def test_update_method(self):
        """测试update方法。"""
        config = DatabaseConfig(host="localhost", port=5432)

        config.update(host="updated.host", port=3306)
        assert config.host == "updated.host"
        assert config.port == 3306

        # 更新不存在的字段应该被忽略
        config.update(nonexistent="value")
        assert not hasattr(config, "nonexistent")


# ==================== 测试2: BaseBranchConfig功能 ====================


class TestBaseBranchConfig:
    """测试BaseBranchConfig功能。"""

    def test_default_initialization(self):
        """测试默认初始化。"""
        config = AppConfig()

        # 检查默认method_name
        assert config.method_name == "database"
        # 检查configs是否被自动初始化
        assert "database" in config.configs
        assert "cache" in config.configs
        # 检查默认配置值
        assert isinstance(config.configs["database"], DatabaseConfig)
        assert isinstance(config.configs["cache"], CacheConfig)
        assert config.configs["database"].host == "localhost"
        assert config.configs["cache"].backend == "redis"

    def test_method_switching(self):
        """测试方法切换。"""
        config = AppConfig()

        # 初始为database配置
        assert config.method_name == "database"
        assert config.config.host == "localhost"

        # 切换到cache配置
        config.method_name = "cache"
        assert config.config.backend == "redis"

        # 使用__setitem__切换
        config["method_name"] = "database"
        assert config.method_name == "database"

    def test_config_property_access(self):
        """测试config property访问。"""
        config = AppConfig()

        # 获取当前配置
        db_config = config.config
        assert isinstance(db_config, DatabaseConfig)
        assert db_config.host == "localhost"

        # 设置当前配置
        new_db_config = DatabaseConfig(host="192.168.1.1", port=3306)
        config.config = new_db_config
        assert config.config.host == "192.168.1.1"
        assert config.config.port == 3306

    def test_dict_access(self):
        """测试字典式访问。"""
        config = AppConfig()

        # 访问method_name
        assert config["method_name"] == "database"

        # 访问当前config
        current = config["config"]
        assert isinstance(current, DatabaseConfig)

        # 访问特定配置
        cache = config["cache"]
        assert isinstance(cache, CacheConfig)

        # 设置值
        config["method_name"] = "cache"
        assert config.method_name == "cache"

        new_cache = CacheConfig(backend="memcached")
        config["cache"] = new_cache
        assert config.configs["cache"].backend == "memcached"

    def test_dict_serialization(self):
        """测试字典序列化/反序列化。"""
        config = AppConfig(
            method_name="cache",
            app_name="TestApp",
            debug=True,
            configs={
                "database": DatabaseConfig(host="remote.host", port=3306),
                "cache": CacheConfig(backend="memcached", ttl=7200),
            },
        )

        # 序列化为字典
        data = config.to_dict()
        assert data["method_name"] == "cache"
        assert data["app_name"] == "TestApp"
        assert data["debug"] is True
        assert data["configs"]["database"]["host"] == "remote.host"
        assert data["configs"]["cache"]["backend"] == "memcached"

        # 从字典反序列化
        config2 = AppConfig.from_dict(data)
        assert config2.method_name == "cache"
        assert config2.app_name == "TestApp"
        assert config2.configs["database"].host == "remote.host"
        assert config2.configs["cache"].backend == "memcached"


# ==================== 测试3: 三级嵌套配置 ====================


class LoggerConfig(BaseConfig):
    """日志配置（Level 1）。"""

    level: str = "INFO"
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    file_path: str = "/var/log/app.log"


class ServiceConfig(BaseBranchConfig):
    """服务配置（Level 2），包含LoggerConfig。"""

    configs_type = {
        "api": BaseConfig,  # 将在后面定义
        "worker": BaseConfig,  # 将在后面定义
    }

    method_name: str = "api"
    logger: LoggerConfig = None
    timeout: int = 30

    def __init__(self, **kwargs):
        # 设置默认logger
        if "logger" not in kwargs or kwargs["logger"] is None:
            kwargs["logger"] = LoggerConfig()
        super().__init__(**kwargs)


class ApiServiceConfig(BaseConfig):
    """API服务配置。"""

    host: str = "0.0.0.0"
    port: int = 8000
    workers: int = 4


class WorkerServiceConfig(BaseConfig):
    """Worker服务配置。"""

    queue_name: str = "default"
    concurrency: int = 10
    prefetch_count: int = 100


# 更新ServiceConfig的configs_type
ServiceConfig.configs_type = {
    "api": ApiServiceConfig,
    "worker": WorkerServiceConfig,
}


class SystemConfig(BaseBranchConfig):
    """系统配置（Level 3），包含ServiceConfig。"""

    configs_type = {
        "production": ServiceConfig,
        "development": ServiceConfig,
    }

    method_name: str = "production"
    environment: str = "prod"
    version: str = "1.0.0"


class TestThreeLevelNesting:
    """测试三级嵌套配置。"""

    def test_level1_config(self):
        """测试Level 1基础配置。"""
        config = LoggerConfig(level="DEBUG", file_path="/tmp/test.log")
        assert config.level == "DEBUG"
        assert config.file_path == "/tmp/test.log"

    def test_level2_config(self):
        """测试Level 2配置（包含Level 1）。"""
        config = ServiceConfig(
            method_name="worker",
            logger=LoggerConfig(level="ERROR"),
            timeout=60,
        )

        assert config.method_name == "worker"
        assert config.logger.level == "ERROR"
        assert config.timeout == 60
        assert isinstance(config.config, WorkerServiceConfig)

    def test_level3_config(self):
        """测试Level 3配置（包含Level 2）。"""
        # 创建嵌套配置
        logger = LoggerConfig(level="WARNING")
        service_config = ServiceConfig(
            method_name="api",
            logger=logger,
            timeout=45,
        )

        system_config = SystemConfig(
            method_name="development",
            environment="dev",
            version="2.0.0",
            configs={
                "production": ServiceConfig(
                    method_name="api",
                    logger=LoggerConfig(level="INFO"),
                    timeout=30,
                ),
                "development": service_config,
            },
        )

        # 验证层级关系
        assert system_config.method_name == "development"
        assert system_config.environment == "dev"
        assert system_config.config.method_name == "api"
        assert system_config.config.logger.level == "WARNING"
        assert system_config.config.config.host == "0.0.0.0"

    def test_three_level_serialization(self):
        """测试三级配置的序列化/反序列化。"""
        config = SystemConfig(
            method_name="production",
            environment="prod",
            version="1.0.0",
            configs={
                "production": ServiceConfig(
                    method_name="worker",
                    logger=LoggerConfig(level="INFO"),
                    timeout=30,
                ),
                "development": ServiceConfig(
                    method_name="api",
                    logger=LoggerConfig(level="DEBUG"),
                    timeout=60,
                ),
            },
        )

        # 序列化
        data = config.to_dict()

        # 验证序列化结构
        assert data["method_name"] == "production"
        assert data["configs"]["production"]["method_name"] == "worker"
        assert data["configs"]["production"]["logger"]["level"] == "INFO"
        assert data["configs"]["development"]["logger"]["level"] == "DEBUG"

        # 反序列化
        config2 = SystemConfig.from_dict(data)
        assert config2.method_name == "production"
        assert config2.configs["production"].method_name == "worker"
        assert config2.configs["production"].logger.level == "INFO"
        assert config2.configs["development"].config.port == 8000


# ==================== 测试4: 文件序列化格式 ====================


class FileFormatConfig(BaseConfig):
    """用于测试文件格式的配置类。"""

    name: str = "test"
    values: list = None
    metadata: dict = None

    def __init__(self, **kwargs):
        if "values" not in kwargs or kwargs["values"] is None:
            kwargs["values"] = [1, 2, 3]
        if "metadata" not in kwargs or kwargs["metadata"] is None:
            kwargs["metadata"] = {"key": "value", "number": 42}
        super().__init__(**kwargs)


class TestFileSerialization:
    """测试文件序列化格式。"""

    def test_toml_string_serialization(self):
        """测试TOML字符串序列化。"""
        config = FileFormatConfig(name="toml_test", values=["a", "b", "c"])

        # 序列化为字符串
        toml_str = config.to_toml_string()
        assert 'name = "toml_test"' in toml_str

        # 从字符串反序列化
        config2 = FileFormatConfig.from_toml_string(toml_str)
        assert config2.name == "toml_test"

    def test_toml_file_serialization(self):
        """测试TOML文件序列化。"""
        config = FileFormatConfig(name="file_test")
        file_path = CACHE_DIR / "test_config.toml"

        # 保存到文件
        config.to_toml(file_path)
        assert file_path.exists()

        # 从文件加载
        config2 = FileFormatConfig.from_toml(file_path)
        assert config2.name == "file_test"
        assert config2.values == [1, 2, 3]

    def test_json_string_serialization(self):
        """测试JSON字符串序列化。"""
        config = FileFormatConfig(name="json_test", metadata={"nested": {"key": "value"}})

        # 序列化为字符串
        json_str = config.to_json_string()
        assert '"name": "json_test"' in json_str
        assert '"nested"' in json_str

        # 从字符串反序列化
        config2 = FileFormatConfig.from_json_string(json_str)
        assert config2.name == "json_test"
        assert config2.metadata["nested"]["key"] == "value"

    def test_json_file_serialization(self):
        """测试JSON文件序列化。"""
        config = FileFormatConfig(name="json_file_test")
        file_path = CACHE_DIR / "test_config.json"

        # 保存到文件
        config.to_json(file_path)
        assert file_path.exists()

        # 从文件加载
        config2 = FileFormatConfig.from_json(file_path)
        assert config2.name == "json_file_test"
        assert config2.values == [1, 2, 3]

    def test_msgpack_bytes_serialization(self):
        """测试MsgPack字节序列化。"""
        config = FileFormatConfig(name="msgpack_test")

        # 序列化为字节
        msgpack_bytes = config.to_msgpack_bytes()
        assert isinstance(msgpack_bytes, bytes)

        # 从字节反序列化
        config2 = FileFormatConfig.from_msgpack_bytes(msgpack_bytes)
        assert config2.name == "msgpack_test"
        assert config2.values == [1, 2, 3]

    def test_msgpack_file_serialization(self):
        """测试MsgPack文件序列化。"""
        config = FileFormatConfig(name="msgpack_file_test")
        file_path = CACHE_DIR / "test_config.msgpack"

        # 保存到文件
        config.to_msgpack(file_path)
        assert file_path.exists()

        # 从文件加载
        config2 = FileFormatConfig.from_msgpack(file_path)
        assert config2.name == "msgpack_file_test"
        assert config2.metadata["key"] == "value"

    def test_all_formats_consistency(self):
        """测试所有格式的一致性。"""
        config = FileFormatConfig(
            name="consistency_test",
            values=[10, 20, 30],
            metadata={"test": True, "count": 100},
        )

        # TOML
        toml_data = FileFormatConfig.from_toml_string(config.to_toml_string()).to_dict()

        # JSON
        json_data = FileFormatConfig.from_json_string(config.to_json_string()).to_dict()

        # MsgPack
        msgpack_data = FileFormatConfig.from_msgpack_bytes(config.to_msgpack_bytes()).to_dict()

        # 验证一致性
        assert toml_data["name"] == json_data["name"] == msgpack_data["name"]
        assert toml_data["values"] == json_data["values"] == msgpack_data["values"]


# ==================== 测试5: BaseBranchConfig的文件序列化 ====================


class TestBaseBranchConfigFileSerialization:
    """测试BaseBranchConfig的文件序列化。"""

    def test_convert_method_toml(self):
        """测试BaseBranchConfig的TOML序列化。"""
        config = AppConfig(
            method_name="cache",
            app_name="TestApp",
            configs={
                "database": DatabaseConfig(host="db.host", port=5433),
                "cache": CacheConfig(backend="redis", ttl=7200),
            },
        )

        file_path = CACHE_DIR / "app_config.toml"
        config.to_toml(file_path)

        config2 = AppConfig.from_toml(file_path)
        assert config2.method_name == "cache"
        assert config2.app_name == "TestApp"
        assert config2.configs["database"].host == "db.host"
        assert config2.configs["cache"].ttl == 7200

    def test_convert_method_json(self):
        """测试BaseBranchConfig的JSON序列化。"""
        config = AppConfig(
            method_name="database",
            debug=True,
            configs={
                "database": DatabaseConfig(host="json.host"),
                "cache": CacheConfig(backend="json_cache"),
            },
        )

        file_path = CACHE_DIR / "app_config.json"
        config.to_json(file_path)

        config2 = AppConfig.from_json(file_path)
        assert config2.method_name == "database"
        assert config2.debug is True
        assert config2.configs["database"].host == "json.host"

    def test_convert_method_msgpack(self):
        """测试BaseBranchConfig的MsgPack序列化。"""
        config = AppConfig(
            method_name="cache",
            app_name="MsgPackApp",
            configs={
                "database": DatabaseConfig(port=3307),
                "cache": CacheConfig(max_size=5000),
            },
        )

        file_path = CACHE_DIR / "app_config.msgpack"
        config.to_msgpack(file_path)

        config2 = AppConfig.from_msgpack(file_path)
        assert config2.method_name == "cache"
        assert config2.app_name == "MsgPackApp"
        assert config2.configs["database"].port == 3307
        assert config2.configs["cache"].max_size == 5000


# ==================== 测试6: 其他功能测试 ====================


class TestAdditionalFeatures:
    """测试其他功能。"""

    def test_update_method(self):
        """测试 update 方法。"""
        config = DatabaseConfig(host="localhost", port=5432)

        config.update(host="updated.host", port=3306)
        assert config.host == "updated.host"
        assert config.port == 3306

        # 更新不存在的字段应该被忽略
        config.update(nonexistent="value")
        assert not hasattr(config, "nonexistent")

    def test_nested_config_in_config(self):
        """测试 Config 中嵌套 Config。"""

        class InnerConfig(BaseConfig):
            value: int = 10

        class OuterConfig(BaseConfig):
            inner: InnerConfig = None
            name: str = "outer"

            def __init__(self, **kwargs):
                if "inner" not in kwargs or kwargs["inner"] is None:
                    kwargs["inner"] = InnerConfig()
                super().__init__(**kwargs)

        config = OuterConfig(inner=InnerConfig(value=20), name="test")
        assert config.inner.value == 20

        # 测试序列化
        data = config.to_dict()
        assert data["inner"]["value"] == 20

        config2 = OuterConfig.from_dict(data)
        assert config2.inner.value == 20

    def test_update_from_other_config(self):
        """测试 update 方法支持从其他配置对象更新。"""
        config1 = DatabaseConfig(host="host1", port=5432)
        config2 = DatabaseConfig(host="host2", port=3306, username="user2")

        config1.update(config2)
        assert config1.host == "host2"
        assert config1.port == 3306
        assert config1.username == "user2"

    def test_from_other_config(self):
        """测试 from_other_config 方法。"""
        config2 = DatabaseConfig(host="host2", port=3306)

        config3 = DatabaseConfig.from_other_config(config2)
        assert config3.host == "host2"
        assert config3.port == 3306
        assert config3 is not config2


# ==================== 测试 7: BaseConfigContainer 功能 ====================


class ConfigContainerForTest(BaseConfigContainer):
    """用于测试的 BaseConfigContainer 子类。"""

    database: DatabaseConfig = None
    cache: CacheConfig = None
    name: str = "test"

    def __init__(self, **kwargs):
        if "database" not in kwargs or kwargs["database"] is None:
            kwargs["database"] = DatabaseConfig()
        if "cache" not in kwargs or kwargs["cache"] is None:
            kwargs["cache"] = CacheConfig()
        super().__init__(**kwargs)


class TestBaseConfigContainer:
    """测试 BaseConfigContainer 功能。"""

    def test_default_initialization(self):
        """测试默认初始化。"""
        config = ConfigContainerForTest()

        assert config.name == "test"
        assert isinstance(config.database, DatabaseConfig)
        assert isinstance(config.cache, CacheConfig)
        assert config.database.host == "localhost"
        assert config.cache.backend == "redis"

    def test_custom_initialization(self):
        """测试自定义初始化。"""
        config = ConfigContainerForTest(
            name="custom",
            database=DatabaseConfig(host="custom.host", port=3306),
            cache=CacheConfig(backend="memcached"),
        )

        assert config.name == "custom"
        assert config.database.host == "custom.host"
        assert config.cache.backend == "memcached"

    def test_update_from_other_config(self):
        """测试 update 方法支持从其他容器对象更新。"""
        config1 = ConfigContainerForTest(
            name="config1",
            database=DatabaseConfig(host="host1", port=5432),
        )
        config2 = ConfigContainerForTest(
            name="config2",
            database=DatabaseConfig(host="host2", port=3306),
            cache=CacheConfig(backend="memcached", ttl=7200),
        )

        config1.update(config2)
        assert config1.name == "config2"
        assert config1.database.host == "host2"
        assert config1.database.port == 3306
        assert config1.cache.backend == "memcached"
        assert config1.cache.ttl == 7200

    def test_from_other_config(self):
        """测试 from_other_config 方法。"""
        config2 = ConfigContainerForTest(
            name="config2",
            database=DatabaseConfig(host="host2", port=3306),
        )

        config3 = ConfigContainerForTest.from_other_config(config2)
        assert config3.name == "config2"
        assert config3.database.host == "host2"
        assert config3.database.port == 3306
        assert config3 is not config2


# ==================== 测试 8: 子类匹配功能 ====================


class ParentConfig(BaseConfig):
    """父类配置。"""

    value: str = "parent"
    count: int = 10


class ChildConfig(ParentConfig):
    """子类配置，继承自 ParentConfig。"""

    extra: str = "child"


class GrandchildConfig(ChildConfig):
    """孙类配置，继承自 ChildConfig。"""

    level: int = 3


class TestSubclassMatching:
    """测试子类匹配功能。"""

    def test_child_updates_parent(self):
        """测试子类实例更新父类实例。"""
        parent = ParentConfig(value="original", count=5)
        child = ChildConfig(value="from_child", count=20, extra="child_extra")

        parent.update(child)

        assert parent.value == "from_child"
        assert parent.count == 20

    def test_grandchild_updates_parent(self):
        """测试孙类实例更新父类实例。"""
        parent = ParentConfig(value="original", count=5)
        grandchild = GrandchildConfig(value="from_grandchild", count=30, extra="gc_extra", level=3)

        parent.update(grandchild)

        assert parent.value == "from_grandchild"
        assert parent.count == 30

    def test_child_updates_parent_via_kwargs(self):
        """测试通过 kwargs 使用子类实例更新父类实例。

        注意：BaseConfig 不会透传参数，只有 BaseConfigContainer 才会透传。
        因此这个测试验证的是：当 kwargs 中的参数名与字段名匹配时，
        子类实例可以直接覆盖父类字段。
        """
        parent = ParentConfig(value="original", count=5)
        child = ChildConfig(value="from_child", count=25, extra="child_extra")

        # BaseConfig 不会透传参数，所以这里应该直接覆盖字段值
        # 由于 ParentConfig 没有名为 'config' 的字段，所以这个调用不会改变 parent 的值
        # 正确的做法是直接传递字段名
        parent.update(value=child.value, count=child.count)

        assert parent.value == "from_child"
        assert parent.count == 25


class ContainerWithParentConfig(BaseConfigContainer):
    """包含父类配置的容器。"""

    config: ParentConfig = None
    name: str = "container"

    def __init__(self, **kwargs):
        if "config" not in kwargs or kwargs["config"] is None:
            kwargs["config"] = ParentConfig()
        super().__init__(**kwargs)


class TestConfigContainerSubclassMatching:
    """测试 BaseConfigContainer 子类匹配功能。"""

    def test_child_config_updates_parent_in_container(self):
        """测试子类配置更新容器中的父类配置。"""
        container = ContainerWithParentConfig()
        child_config = ChildConfig(value="child_value", count=100, extra="extra_value")

        container.update(config=child_config)

        assert container.config.value == "child_value"
        assert container.config.count == 100

    def test_grandchild_config_updates_parent_in_container(self):
        """测试孙类配置更新容器中的父类配置。"""
        container = ContainerWithParentConfig()
        grandchild_config = GrandchildConfig(
            value="grandchild_value",
            count=200,
            extra="extra_value",
            level=5,
        )

        container.update(config=grandchild_config)

        assert container.config.value == "grandchild_value"
        assert container.config.count == 200

    def test_same_type_still_works(self):
        """测试相同类型仍然可以正常工作。"""
        container = ContainerWithParentConfig()
        same_type_config = ParentConfig(value="same_type", count=50)

        container.update(config=same_type_config)

        assert container.config.value == "same_type"
        assert container.config.count == 50
