"""Dispatcher 类的单元测试。"""

import pytest
from flowlet.config.base_config import BaseConfig
from flowlet.config.config_container import BaseConfigContainer
from flowlet.dispatcher import Dispatcher
from flowlet.executable_unit import ExecutableUnit


class DispatcherTestConfig(BaseConfig):
    """测试用配置类。"""

    test_param: str = "default"
    test_value: int = 42


class DispatcherTestContainer(BaseConfigContainer):
    """测试用配置容器。"""

    test: DispatcherTestConfig = None

    def __init__(self, **kwargs):
        if "test" not in kwargs or kwargs["test"] is None:
            kwargs["test"] = DispatcherTestConfig()
        super().__init__(**kwargs)


class DispatcherTest(Dispatcher):
    """测试用 Dispatcher 子类。"""

    config = DispatcherTestContainer()


class SampleUnit1(ExecutableUnit):
    """测试执行单元 1。"""

    config = DispatcherTestConfig()

    def __call__(self, *args, **kwargs):
        return f"SampleUnit1 result: {args}, {kwargs}"


class SampleUnit2(ExecutableUnit):
    """测试执行单元 2。"""

    config = DispatcherTestConfig(test_param="unit2", test_value=100)

    def __call__(self, *args, **kwargs):
        return f"SampleUnit2 result: {args}, {kwargs}"


class SampleUnit3(ExecutableUnit):
    """测试执行单元 3。"""

    config = DispatcherTestConfig(test_param="unit3", test_value=200)

    def __call__(self, *args, **kwargs):
        return f"SampleUnit3 result: {args}, {kwargs}"


def condition_unit1(*args, **kwargs) -> bool:
    """TestUnit1 的判断逻辑函数，当第一个参数为"unit1"时返回 True。"""
    return len(args) > 0 and args[0] == "unit1"


def condition_unit2(*args, **kwargs) -> bool:
    """TestUnit2 的判断逻辑函数，当第一个参数为"unit2"时返回 True。"""
    return len(args) > 0 and args[0] == "unit2"


def condition_default(*args, **kwargs) -> bool:
    """默认的判断逻辑函数，始终返回 True。"""
    return True


@pytest.fixture(scope="function", autouse=True)
def setup_dispatcher():
    """确保每个测试前都重置注册表。"""
    DispatcherTest.registered_units.clear()
    DispatcherTest.register(priority=10, condition=condition_unit1)(SampleUnit1)
    DispatcherTest.register(priority=5, condition=condition_unit2)(SampleUnit2)
    DispatcherTest.register(priority=0, condition=condition_default)(SampleUnit3)
    yield
    DispatcherTest.registered_units.clear()


class TestDispatcherBasic:
    """Dispatcher 类的基础测试。"""

    def test_dispatcher_initialization(self):
        """测试 Dispatcher 的初始化。"""
        config = DispatcherTestContainer()
        dispatcher = DispatcherTest(config)
        assert isinstance(dispatcher, DispatcherTest)
        assert isinstance(dispatcher, Dispatcher)

    def test_register_decorator(self):
        """测试注册装饰器。"""
        assert len(DispatcherTest.registered_units) == 3

    def test_config_inheritance(self):
        """测试配置继承机制。"""
        config = DispatcherTestContainer()
        dispatcher = DispatcherTest(config)
        assert hasattr(dispatcher.config, "update")
        assert hasattr(dispatcher.config, "test")

    def test_execute(self):
        """测试执行逻辑。"""
        config = DispatcherTestContainer()
        dispatcher = DispatcherTest(config)
        result = dispatcher.bind_input("unit1", key="value").execute()
        assert "SampleUnit1 result" in result
        result = dispatcher.bind_input("unit2", key="value").execute()
        assert "SampleUnit2 result" in result
        result = dispatcher.bind_input("other", key="value").execute()
        assert "SampleUnit3 result" in result

    def test_call_method(self):
        """测试__call__方法。"""
        config = DispatcherTestContainer()
        dispatcher = DispatcherTest(config)
        result = dispatcher("unit1", key="value")
        assert "SampleUnit1 result" in result
        result = dispatcher("unit2", key="value")
        assert "SampleUnit2 result" in result
        result = dispatcher("other", key="value")
        assert "SampleUnit3 result" in result


class TestDispatcherPriority:
    """Dispatcher 类的优先级测试。"""

    def test_priority_ordering(self):
        """测试优先级机制。"""
        config = DispatcherTestContainer()

        def high_priority_condition(*args, **kwargs) -> bool:
            return True

        def low_priority_condition(*args, **kwargs) -> bool:
            return True

        class HighPriorityUnit(ExecutableUnit):
            config = DispatcherTestConfig()

            def __call__(self, *args, **kwargs):
                return "HighPriorityUnit result"

        class LowPriorityUnit(ExecutableUnit):
            config = DispatcherTestConfig()

            def __call__(self, *args, **kwargs):
                return "LowPriorityUnit result"

        DispatcherTest.register(priority=20, condition=high_priority_condition)(HighPriorityUnit)
        DispatcherTest.register(priority=0, condition=low_priority_condition)(LowPriorityUnit)

        dispatcher = DispatcherTest(config)
        result = dispatcher("other", key="value")
        assert "HighPriorityUnit result" in result

    def test_priority_sorting(self):
        """测试优先级排序。"""
        priorities = [priority for priority, _, _, _ in DispatcherTest.registered_units]
        assert priorities == sorted(priorities, reverse=True)


class TestDispatcherErrorHandling:
    """Dispatcher 类的错误处理测试。"""

    def test_no_matching_unit(self):
        """测试没有满足条件的分支。"""
        config = DispatcherTestContainer()

        DispatcherTest.registered_units.clear()

        def never_match_condition(*args, **kwargs) -> bool:
            return False

        class NoMatchUnit(ExecutableUnit):
            config = DispatcherTestConfig()

            def __call__(self, *args, **kwargs):
                return "Should not reach here"

        DispatcherTest.register(priority=0, condition=never_match_condition)(NoMatchUnit)

        dispatcher = DispatcherTest(config)
        with pytest.raises(ValueError):
            dispatcher("test")


class TestDispatcherBuildConfig:
    """Dispatcher 类的 build_config 方法测试。"""

    def test_build_config_default(self):
        """测试默认配置构造。"""
        config = DispatcherTest.build_config()
        assert isinstance(config, DispatcherTestContainer)
        assert isinstance(config.test, DispatcherTestConfig)

    def test_build_config_with_kwargs(self):
        """测试带参数的配置构造。"""
        new_test = DispatcherTestConfig(test_param="custom", test_value=999)
        config = DispatcherTest.build_config(test=new_test)
        assert config.test.test_param == "custom"
        assert config.test.test_value == 999


class TestDispatcherRunMethod:
    """Dispatcher 类的 run 方法测试。"""

    def test_run_basic(self):
        """测试基本即时执行。"""
        result = SampleUnit1.run("test", key="value")
        assert "SampleUnit1 result" in result

    def test_run_with_config(self):
        """测试带配置的即时执行。"""
        config = DispatcherTestConfig(test_param="custom", test_value=500)
        result = SampleUnit1.run("test", config=config, key="value")
        assert "SampleUnit1 result" in result


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
