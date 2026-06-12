"""测试可执行单元的异步执行功能"""

import pickle
import time
import unittest

from flowlet.config import BaseConfig, BaseConfigContainer
from flowlet.executable_unit import ExecutableUnit


class DummyConfig(BaseConfig):
    """测试用配置类"""

    value: int = 10
    name: str = "default"


class DummyConfigContainer(BaseConfigContainer):
    """测试用配置容器"""

    dummy: DummyConfig = None
    count: int = 5

    def __init__(self, **kwargs):
        if "dummy" not in kwargs or kwargs["dummy"] is None:
            kwargs["dummy"] = DummyConfig()
        super().__init__(**kwargs)


class DummyExecutableUnit(ExecutableUnit):
    """测试用的可执行单元实现"""

    config = BaseConfigContainer()
    """配置对象"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def __call__(self, *args, **kwargs):
        """实现__call__方法，返回输入参数的和"""
        time.sleep(0.1)  # 模拟耗时操作
        return sum(args) if args else 0


class ConfigurableExecutableUnit(ExecutableUnit):
    """带配置的可执行单元实现"""

    config = DummyConfigContainer()
    """配置对象"""

    def __call__(self, *args, **kwargs):
        """实现__call__方法，返回配置中的count和输入参数的和"""
        return self.config.count + sum(args)


class TestExecutableUnitAsync(unittest.TestCase):
    """测试可执行单元的异步执行功能"""

    def test_execute_async(self):
        """测试基于线程的异步执行"""
        unit = DummyExecutableUnit()
        unit = unit.bind_input(1, 2, 3)

        # 异步执行
        unit.execute_async()

        # 等待执行完成
        while unit._thread is not None:
            time.sleep(0.01)

        # 验证执行结果
        self.assertEqual(unit.result, 6)

    def test_serialization(self):
        """测试序列化和反序列化功能"""
        unit = DummyExecutableUnit()
        unit = unit.bind_input(1, 2, 3)

        # 序列化
        serialized = pickle.dumps(unit)

        # 反序列化
        deserialized = pickle.loads(serialized)

        # 验证反序列化后的对象可以正常执行
        result = deserialized.execute()
        self.assertEqual(result, 6)

    def test_running_check(self):
        """测试运行状态检查"""
        unit = DummyExecutableUnit()
        unit = unit.bind_input(1, 2, 3)

        # 开始异步执行
        unit.execute_async()

        # 尝试再次执行，应该抛出异常
        with self.assertRaises(RuntimeError):
            unit.execute_async()

        # 等待执行完成
        while unit._thread is not None:
            time.sleep(0.01)

        # 再次执行，应该成功
        unit.execute_async()


class TestBuildConfig(unittest.TestCase):
    """测试 build_config 类方法"""

    def test_build_config_default(self):
        """测试默认配置构造"""
        config = ConfigurableExecutableUnit.build_config()

        assert config.count == 5
        assert config.dummy.value == 10

    def test_build_config_with_kwargs(self):
        """测试带参数的配置构造"""
        config = ConfigurableExecutableUnit.build_config(count=100)

        assert config.count == 100
        assert config.dummy.value == 10

    def test_build_config_with_nested_config(self):
        """测试带嵌套配置的构造"""
        new_dummy = DummyConfig(value=50, name="custom")
        config = ConfigurableExecutableUnit.build_config(dummy=new_dummy)

        assert config.dummy.value == 50
        assert config.dummy.name == "custom"


class TestRunMethod(unittest.TestCase):
    """测试 run 类方法"""

    def test_run_basic(self):
        """测试基本即时执行"""
        result = ConfigurableExecutableUnit.run(1, 2, 3)

        assert result == 11  # 5 (count) + 1 + 2 + 3

    def test_run_with_config_kwargs(self):
        """测试带配置参数的即时执行"""
        result = ConfigurableExecutableUnit.run(
            10,
            count=100,
        )

        assert result == 110  # 100 (count) + 10

    def test_run_with_config_args(self):
        """测试带配置位置参数的即时执行"""
        # 注意：run 方法现在只支持通过 kwargs 传递配置参数
        # 位置参数形式已废弃，应使用以下方式
        new_config = DummyConfigContainer(count=50)
        # 这里我们直接使用配置对象作为参数
        result = ConfigurableExecutableUnit.run(
            5,
            dummy=new_config.dummy,
            count=50,
        )

        assert result == 55  # 50 (count) + 5

    def test_run_no_args(self):
        """测试无参数的即时执行"""
        result = ConfigurableExecutableUnit.run()

        assert result == 5  # 只有 count 的默认值


if __name__ == "__main__":
    unittest.main()
