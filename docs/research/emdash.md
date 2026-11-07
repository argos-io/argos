# EmDash：运行时、插件与交付维护参考

调研日期：2026-11-06。依据当日上游 `main` 分支文档和 Cloudflare 官方文档；链接会随上游变化，正式选型前需固定版本并验证。此次为文档调研，未运行 EmDash 或做部署测试。

## 已核实的上游设计

| 主题 | 上游描述 | 直接来源 |
| --- | --- | --- |
| 产品定位 | 基于 Astro 的 TypeScript CMS，提供内容管理、管理后台、认证及插件机制 | [README](https://github.com/emdash-cms/emdash/blob/main/README.md) |
| 平台适配 | Cloudflare 与 Node.js 均有部署路径；数据库和存储可按平台配置 | [README](https://github.com/emdash-cms/emdash/blob/main/README.md)、[Node.js 部署](https://github.com/emdash-cms/emdash/blob/main/docs/src/content/docs/deployment/nodejs.mdx) |
| 原生插件 | 与站点同进程运行，通过项目配置安装，变更需要部署；可扩展管理界面和页面渲染 | [插件概览](https://github.com/emdash-cms/emdash/blob/main/docs/src/content/docs/plugins/overview.mdx) |
| 沙箱插件 | 通过配置的 runner 执行，使用受约束的扩展 API；支持注册表安装 | [插件概览](https://github.com/emdash-cms/emdash/blob/main/docs/src/content/docs/plugins/overview.mdx) |
| 沙箱运行环境 | Cloudflare 使用 Dynamic Workers；Node.js 路径需额外启动 workerd 子进程 | [沙箱部署](https://github.com/emdash-cms/emdash/blob/main/docs/src/content/docs/deployment/plugin-sandbox.mdx) |
| 隔离底层能力 | Dynamic Workers 支持控制 bindings、网络访问和资源限制 | [Cloudflare 官方文档](https://developers.cloudflare.com/dynamic-workers/) |
| 升级与恢复 | 插件存储布局变更可能要求核心与 runner 协同升级、停写和迁移；仅降级包不能保证正确恢复数据 | [插件升级指南](https://github.com/emdash-cms/emdash/blob/main/docs/src/content/docs/plugins/upgrading-sites.mdx) |

注意：EmDash 的 Node.js 沙箱路径包含额外二进制与进程依赖，不能等同于仅依赖 Node.js 的单进程方案。其文档还列出 Cloudflare 与 Node.js runner 的资源限制差异。本项目已确定由注册操作人评估插件安全，不要求这一沙箱路径；若基于 EmDash 扩展，需要评估其原生插件路径及交付依赖。

## 对本项目的启发（推论，尚未采纳）

1. 生成工具、管理平台和站点运行时可以分别演进；需要明确生成后的站点对平台的持续依赖。
2. 插件的代码组织方式、安装时机与执行隔离是不同维度，应分别决定。
3. 本项目已确定构建时安装、运行时启停，注册操作人负责插件安全性评估。无需照搬 EmDash 的在线安装与沙箱插件路径。
4. 声明式插件清单可表达版本、运行平台、依赖、所需能力与数据迁移，支持生成和升级前检查。
5. 支持两种平台不代表能力完全一致；需要插件级别的兼容声明与可重复验证。
6. 插件升级属于交付后的维护流程，必须同时考虑代码、配置、数据和外部服务。

## 选型前仍需验证

- EmDash 的内容模型和自带管理后台是否适合首版站点类型，是否符合“小核心”的要求。
- 直接基于 EmDash 扩展、只复用部分能力、自建运行时三种方案的维护成本。
- 本项目是否需要 EmDash 插件兼容性，还是仅借鉴其扩展边界。
- 若采用 EmDash，是否可基于原生插件满足运行时启停需求，并避免不必要的沙箱运行依赖。
- 候选方案在两种运行目标上的资源配置、成本和故障恢复是否满足要求。
- 具体版本下的插件停用、卸载、依赖管理与数据迁移能力；本次未对其作完整性结论。

本调研不足以直接得出“应采用 EmDash”或“应自建”的结论；先以需求文档中的交付、功能与插件边界作筛选依据。
