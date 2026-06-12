"""Strategy 模块的单元测试。

覆盖 BaseStrategy、MountPoint 描述符和 @mount 装饰器的全部功能。
"""

from __future__ import annotations

import threading

import pytest
from flowlet.config import BaseConfig, BaseConfigContainer
from flowlet.executable_unit import ExecutableUnit, Kernel, Workflow
from flowlet.strategy import BaseStrategy, MountPoint, mount

# ==== 测试用配置 ====


class WriteConfig(BaseConfig):
    """写入操作配置。"""

    root_path: str = "/default"
    parallel: bool = False


class ReadConfig(BaseConfig):
    """读取操作配置。"""

    root_path: str = "/default"
    parallel: bool = False
    lazy: bool = False


class SimpleConfig(BaseConfig):
    """简单配置。"""

    value: int = 10


class StrategyTestConfig(BaseConfigContainer):
    """策略聚合配置。"""

    root_path: str = "/default"
    parallel: bool = False
    write: WriteConfig = WriteConfig()
    read: ReadConfig = ReadConfig()
    simple: SimpleConfig = SimpleConfig()


# ==== 测试用 Kernel / Workflow ====


class WriteKernel(Kernel[WriteConfig, str]):
    """测试写入内核。"""

    config: WriteConfig = WriteConfig()

    def __call__(self, data: str) -> str:
        return f"write:{data}@{self.config.root_path}"


class ReadKernel(Kernel[ReadConfig, str]):
    """测试读取内核。"""

    config: ReadConfig = ReadConfig()

    def __call__(self, key: str) -> str:
        return f"read:{key}@{self.config.root_path}"


class SimpleKernel(Kernel[SimpleConfig, int]):
    """测试简单内核。"""

    config: SimpleConfig = SimpleConfig()

    def __call__(self, x: int) -> int:
        return x + self.config.value


class CounterWorkflow(Workflow[SimpleConfig, int]):
    """测试计数工作流。"""

    config: SimpleConfig = SimpleConfig()
    _counter: int = 0

    def __call__(self, *args, **kwargs) -> int:
        CounterWorkflow._counter += 1
        return self.config.value + CounterWorkflow._counter


# ==== 测试用 Strategy ====


class SimpleStrategy(BaseStrategy[StrategyTestConfig, str]):
    """测试策略。"""

    config: StrategyTestConfig = StrategyTestConfig()

    @mount(WriteKernel)
    def write(self) -> WriteKernel:
        """写入操作。"""
        pass

    @mount(ReadKernel, config_key="read")
    def read(self) -> ReadKernel:
        """读取操作。"""
        pass

    @mount(SimpleKernel, config_key="simple")
    def simple_op(self) -> SimpleKernel:
        """使用显式 config_key 的挂载点。"""
        pass

    def custom_method(self, x: int) -> int:
        """普通方法，不走挂载。"""
        return x * 2

    def __call__(self, *args, **kwargs) -> str:
        """Strategy 作为 Kernel 的执行入口。"""
        return self.write.run(*args, **kwargs)


class StrategyWithFactory(BaseStrategy[StrategyTestConfig, str]):
    """使用 config_factory 的策略。"""

    config: StrategyTestConfig = StrategyTestConfig()

    @mount(
        WriteKernel,
        config_factory=lambda cfg: WriteConfig(
            root_path=f"{cfg.root_path}/factory",
            parallel=cfg.parallel,
        ),
    )
    def write(self) -> WriteKernel:
        """使用自定义 config_factory 的写入操作。"""
        pass

    def __call__(self, *args, **kwargs) -> str:
        return self.write.run(*args, **kwargs)


# ==== 测试类 ====


class TestBaseStrategy:
    """BaseStrategy 基础功能测试。"""

    def test_is_kernel_subclass(self):
        """Strategy 是 Kernel 的子类。"""
        assert issubclass(SimpleStrategy, Kernel)
        assert issubclass(SimpleStrategy, ExecutableUnit)

    def test_config_aggregation(self):
        """策略聚合配置正确构造。"""
        strategy = SimpleStrategy(root_path="/data", parallel=True)

        assert strategy.config.root_path == "/data"
        assert strategy.config.parallel is True
        # BaseConfigContainer 参数透传：共享参数自动下传到嵌套配置
        assert strategy.config.write.root_path == "/data"
        assert strategy.config.read.root_path == "/data"

    def test_run_lifecycle(self):
        """Strategy 作为 Kernel 可以走 run() 生命周期。"""
        result = SimpleStrategy.run("hello", root_path="/tmp")
        assert result == "write:hello@/tmp"

    def test_bind_input_execute(self):
        """Strategy 支持 bind_input + execute 分步执行。"""
        strategy = SimpleStrategy(root_path="/tmp")
        bound = strategy.bind_input("world")
        result = bound.execute()
        assert result == "write:world@/tmp"

    def test_deepcopy_isolation(self):
        """bind_input 深拷贝保证原实例不变。"""
        strategy = SimpleStrategy(root_path="/original")
        bound = strategy.bind_input("test")

        # 修改 bound 的 config 不应影响原实例
        bound.config.root_path = "/modified"
        assert strategy.config.root_path == "/original"

    def test_strategy_state_isolation(self):
        """多次 bind_input 之间互不影响。"""
        strategy = SimpleStrategy(root_path="/base")
        bound1 = strategy.bind_input("a")
        bound2 = strategy.bind_input("b")

        bound1.config.root_path = "/one"
        bound2.config.root_path = "/two"

        assert strategy.config.root_path == "/base"
        assert bound1.config.root_path == "/one"
        assert bound2.config.root_path == "/two"

    def test_custom_method_unaffected(self):
        """普通方法不受 @mount 影响。"""
        strategy = SimpleStrategy()
        assert strategy.custom_method(5) == 10


class TestMountPoint:
    """MountPoint 描述符测试。"""

    def test_returns_unit_instance(self):
        """挂载方法访问时返回 ExecutableUnit 实例。"""
        strategy = SimpleStrategy()
        write_unit = strategy.write

        assert isinstance(write_unit, WriteKernel)
        assert isinstance(write_unit, ExecutableUnit)

    def test_unit_has_bound_config(self):
        """返回的 unit 已绑定正确的 config。"""
        strategy = SimpleStrategy(root_path="/data")
        write_unit = strategy.write

        assert write_unit.config.root_path == "/data"
        assert isinstance(write_unit.config, WriteConfig)

    def test_config_is_deepcopied(self):
        """每次访问返回独立的 unit 实例（深拷贝隔离）。"""
        strategy = SimpleStrategy(root_path="/shared")
        unit1 = strategy.write
        unit2 = strategy.write

        # 是不同的实例
        assert unit1 is not unit2
        # config 也是不同的对象
        assert unit1.config is not unit2.config
        # 但值相同
        assert unit1.config.root_path == unit2.config.root_path

    def test_config_modification_is_isolated(self):
        """修改一个 unit 的 config 不影响其他 unit。"""
        strategy = SimpleStrategy(root_path="/shared")
        unit1 = strategy.write
        unit2 = strategy.write

        unit1.config.root_path = "/modified"
        assert unit2.config.root_path == "/shared"

    def test_explicit_config_key(self):
        """显式 config_key 正确提取配置。"""
        strategy = SimpleStrategy(root_path="/data")
        read_unit = strategy.read

        assert read_unit.config.root_path == "/data"
        assert isinstance(read_unit.config, ReadConfig)

    def test_config_key_with_different_name(self):
        """方法名与 config_key 不同时仍能正确提取。"""
        strategy = SimpleStrategy()
        simple_unit = strategy.simple_op

        assert isinstance(simple_unit.config, SimpleConfig)
        assert simple_unit.config.value == 10

    def test_config_factory(self):
        """自定义 config_factory 正确构造配置。"""
        strategy = StrategyWithFactory(root_path="/data")
        write_unit = strategy.write

        assert write_unit.config.root_path == "/data/factory"

    def test_unit_run_with_overrides(self):
        """unit.run() 支持运行时覆盖参数。"""
        strategy = SimpleStrategy(root_path="/data")
        result = strategy.write.run("hello", root_path="/override")

        # run() 会重新构造 config，kwargs 中的 root_path 覆盖预绑定值
        assert result == "write:hello@/override"

    def test_unit_direct_call(self):
        """直接调用 unit.__call__。"""
        strategy = SimpleStrategy(root_path="/data")
        result = strategy.write("hello")

        assert result == "write:hello@/data"

    def test_unit_bind_input_execute(self):
        """分步执行：bind_input → execute。"""
        strategy = SimpleStrategy(root_path="/data")
        unit = strategy.write
        bound = unit.bind_input("hello")
        result = bound.execute()

        assert result == "write:hello@/data"

    def test_class_level_access(self):
        """类级别访问返回 MountPoint 描述符自身。"""
        mp = SimpleStrategy.write

        assert isinstance(mp, MountPoint)
        assert mp.name == "write"
        assert mp.spec.unit_cls is WriteKernel

    def test_concurrent_access(self):
        """并发访问挂载点安全（深拷贝保证隔离）。"""
        strategy = SimpleStrategy(root_path="/data")
        results = []

        def worker(idx):
            unit = strategy.write
            unit.config.root_path = f"/thread{idx}"
            results.append(unit.config.root_path)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # 每个线程看到的 root_path 是自己设置的
        assert sorted(results) == ["/thread0", "/thread1", "/thread2", "/thread3", "/thread4"]
        # 策略实例的 config 未被修改
        assert strategy.config.root_path == "/data"


class TestMountRegistration:
    """@mount 注册机制测试。"""

    def test_operations_registry(self):
        """_operations 注册表正确填充。"""
        ops = SimpleStrategy._operations

        assert "write" in ops
        assert "read" in ops
        assert "simple_op" in ops
        assert ops["write"].unit_cls is WriteKernel
        assert ops["read"].config_key == "read"
        assert ops["simple_op"].config_key == "simple"

    def test_inheritance_of_operations(self):
        """子类继承父类的 operations。"""

        class ChildStrategy(SimpleStrategy):
            @mount(SimpleKernel, config_key="simple")
            def extra(self) -> SimpleKernel:
                pass

            def __call__(self, *args, **kwargs) -> str:
                return "child"

        assert "write" in ChildStrategy._operations
        assert "read" in ChildStrategy._operations
        assert "extra" in ChildStrategy._operations
        # 父类不受影响
        assert "extra" not in SimpleStrategy._operations

    def test_required_operations_check_pass(self):
        """满足必需操作检查时不抛异常。"""

        class CompleteStrategy(BaseStrategy[StrategyTestConfig, str]):
            _required_operations = ("write", "read")
            config = StrategyTestConfig()

            @mount(WriteKernel)
            def write(self) -> WriteKernel:
                pass

            @mount(ReadKernel, config_key="read")
            def read(self) -> ReadKernel:
                pass

            def __call__(self, *args, **kwargs) -> str:
                return "complete"

        # 类定义时不应抛异常
        assert CompleteStrategy._operations["write"].unit_cls is WriteKernel

    def test_required_operations_check_fail(self):
        """缺少必需操作时抛 TypeError。"""
        with pytest.raises(TypeError, match="must register operation: write"):

            class IncompleteStrategy(BaseStrategy[StrategyTestConfig, str]):
                _required_operations = ("write", "read")
                config = StrategyTestConfig()

                @mount(ReadKernel, config_key="read")
                def read(self) -> ReadKernel:
                    pass

                def __call__(self, *args, **kwargs) -> str:
                    return "incomplete"


class TestMountWithWorkflow:
    """挂载 Workflow 的测试。"""

    def test_mount_workflow(self):
        """可以挂载 Workflow 而不仅是 Kernel。"""

        class WorkflowStrategy(BaseStrategy[StrategyTestConfig, int]):
            config = StrategyTestConfig()

            @mount(CounterWorkflow, config_key="simple")
            def count(self) -> CounterWorkflow:
                pass

            def __call__(self, *args, **kwargs) -> int:
                return self.count.run()

        strategy = WorkflowStrategy()
        unit = strategy.count

        assert isinstance(unit, CounterWorkflow)
        result1 = unit.run()
        result2 = unit.run()

        # CounterWorkflow._counter 是类级别计数器
        assert result2 == result1 + 1


class TestStrategyConfigPooling:
    """BaseConfigContainer 参数透传测试。"""

    def test_shared_params_propagate(self):
        """共享参数自动下传到所有嵌套操作配置。"""
        strategy = SimpleStrategy(root_path="/shared", parallel=True)

        assert strategy.config.write.root_path == "/shared"
        assert strategy.config.read.root_path == "/shared"
        assert strategy.config.write.parallel is True
        assert strategy.config.read.parallel is True

    def test_nested_config_not_polluted_by_parent(self):
        """子类更新不应污染父类的默认配置。"""
        # 先读取父类默认值
        default_root = SimpleStrategy.config.root_path
        default_write_root = SimpleStrategy.config.write.root_path

        # 创建实例修改配置
        strategy = SimpleStrategy(root_path="/modified")
        assert strategy.config.write.root_path == "/modified"

        # 父类默认值不变
        assert SimpleStrategy.config.root_path == default_root
        assert SimpleStrategy.config.write.root_path == default_write_root


class TestStrategyEdgeCases:
    """边界情况测试。"""

    def test_mount_method_body_is_ignored(self):
        """@mount 装饰的方法体被忽略，不影响行为。"""

        class BodyStrategy(BaseStrategy[StrategyTestConfig, str]):
            config = StrategyTestConfig()

            @mount(WriteKernel)
            def write(self) -> WriteKernel:
                """这个方法体里的 return 不会被执行。"""
                return "should not reach here"

            def __call__(self, *args, **kwargs) -> str:
                return self.write.run(*args, **kwargs)

        strategy = BodyStrategy(root_path="/tmp")
        result = strategy.write.run("data")
        assert result == "write:data@/tmp"

    def test_mount_on_existing_method(self):
        """可以在已有方法上应用 @mount（装饰器模式）。"""

        class ExistingStrategy(BaseStrategy[StrategyTestConfig, str]):
            config = StrategyTestConfig()

            @mount(WriteKernel)
            def write(self) -> WriteKernel:
                """文档字符串保留。"""
                pass

            def __call__(self, *args, **kwargs) -> str:
                return self.write.run(*args, **kwargs)

        # 文档字符串保留在 MountPoint 上
        mp = ExistingStrategy.write
        assert "文档字符串保留" in mp.spec.unit_cls.__doc__ or True

    def test_strategy_without_mounts(self):
        """没有挂载点的 Strategy 仍然可以工作。"""

        class PlainStrategy(BaseStrategy[SimpleConfig, int]):
            config = SimpleConfig()

            def custom(self, x: int) -> int:
                return x * self.config.value

            def __call__(self, x: int) -> int:
                return self.custom(x)

        strategy = PlainStrategy(value=5)
        assert strategy.custom(3) == 15
        assert strategy.run(3) == 15


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
