# Flowlet 文档

> [← 返回 README](../README.md)

## 架构概览

Flowlet 是一个灵活的工作流和并行执行框架，提供了统一的抽象接口来管理可执行单元、配置和并行任务。

### 设计哲学

1. **统一的可执行抽象**：所有可执行组件都继承自 `ExecutableUnit`，提供一致的执行接口
2. **配置与逻辑分离**：配置系统独立于执行逻辑，支持灵活配置管理
3. **并行透明**：提供线程和 Ray 两种并行后端，使用统一的接口
4. **类型安全**：使用泛型和类型注解确保类型安全
5. **组合优于继承**：通过配置组合和行为混入实现功能扩展

### 核心组件

```
Flowlet 架构
├── 可执行单元体系
│   ├── ExecutableUnit (抽象基类)
│   │   ├── Kernel (核心执行单元)
│   │   │   ├── Dispatcher (分发器)
│   │   │   └── BaseStrategy (策略基类)
│   │   └── Workflow (工作流)
│   │       ├── ThreadParallelWorkflow
│   │       ├── RayParallelWorkflow
│   │       └── RayPoolCreatorWorkflow
│   └── 类型注解工具
│       ├── TypeKind (类型分类枚举)
│       ├── classify_object (类型分类函数)
│       ├── get_class_from_annotation
│       ├── unwrap_type
│       ├── is_subclass_in_annotation
│       ├── extract_target_subclass_from_annotation
│       └── is_instance_in_annotation
├── 计算图 (Compute Graph)
│   ├── TaskNode (反向 DAG 任务节点)
│   ├── InputSlot / OutputSpec (输入输出定义)
│   ├── InputField / OutputField (ExecutableUnit 元数据)
│   ├── InputVar (命名输入变量)
│   ├── OutputRef (子输出引用)
│   └── TracedGraph (图追溯与可视化)
├── Edge 模式
│   ├── EdgeNode (节点抽象)
│   │   ├── ThreadEdgeNode (同进程 worker 线程后端)
│   │   └── RayEdgeNode (Ray Actor 后端)
│   ├── EdgeBackend (后端抽象)
│   ├── EdgeConfig (配置)
│   └── push / pull / apply / bind / run / join / close / kill
├── 进度管理
│   ├── ProgressManager (进度监视器)
│   │   ├── 同进程模式 (threading.RLock)
│   │   ├── 跨进程代理 (multiprocessing.Queue → MPProgressProxy)
│   │   ├── 跨进程代理 (ray.util.queue.Queue → RayProgressProxy)
│   │   └── 多后端共存
│   └── TaskProgress (任务进度状态)
├── 懒加载工具
│   ├── LazyHolder (延迟值占位符)
│   ├── LazyUnitConfig (懒加载配置)
│   └── LazyUnitKernel (懒加载执行单元)
└── 配置系统
    ├── BaseConfig (基础配置)
    ├── BaseBranchConfig (分支配置)
    ├── BaseConfigContainer (配置容器)
    ├── Sentinel (语义占位符)
    └── 配置行为混入
        ├── DictConfigBehavior
        ├── JsonConfigBehavior
        ├── TomlConfigBehavior
        └── MsgpackConfigBehavior
```

## 文档导航

| 文档 | 内容 | 对应模块 |
|------|------|----------|
| [可执行单元](executable_unit.md) | `ExecutableUnit` / `Kernel` / `Workflow` | `flowlet.executable_unit` |
| [计算图](compute_graph.md) | `TaskNode` / 反向 DAG / 可视化 | `flowlet.compute_graph` |
| [分发器](dispatcher.md) | `Dispatcher` 分支执行 | `flowlet.dispatcher` |
| [策略](strategy.md) | `BaseStrategy` / `@mount` 策略聚合 | `flowlet.strategy` |
| [配置系统](config.md) | `BaseConfig` / `BaseConfigContainer` / `BaseBranchConfig` | `flowlet.config` |
| [并行工作流](parallel.md) | `ThreadParallelWorkflow` / `RayParallelWorkflow` | `flowlet.parallel_unit` |
| [Edge 模式](edge.md) | `ThreadEdgeNode` / `RayEdgeNode` | `flowlet.edge` |
| [进度管理](progress.md) | `ProgressManager` / `MPProgressProxy` / `RayProgressProxy` | `flowlet.base.progress` |
| [基础工具](base.md) | 语义占位符 / 类型注解工具 | `flowlet.base` |
| [示例与最佳实践](examples.md) | 完整示例 / 最佳实践 / 注意事项 | — |

## 快速索引

- **入门**：从 [示例与最佳实践](examples.md) 开始，快速了解典型用法
- **写业务逻辑**：参考 [可执行单元](executable_unit.md) 和 [分发器](dispatcher.md)
- **组合多单元为策略**：参考 [策略](strategy.md)
- **构建任务图**：参考 [计算图](compute_graph.md)
- **配置管理**：参考 [配置系统](config.md)
- **并行执行**：参考 [并行工作流](parallel.md)
- **持有不可序列化对象 / 手动进程控制**：参考 [Edge 模式](edge.md)
- **进度跟踪**：参考 [进度管理](progress.md)
