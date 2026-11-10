# Argos 设计与需求

Argos 的 demo 范围及技术方向已确认，实施前审查的需求澄清和技术契约已补齐，首批任务已修订，用户已授权连续推进 T0–T5。T0、T1、T3、T4 已通过；T5 真实公告源码的独立运行、本地四向恢复及真实云端双向数据恢复已验证。演示站已部署到获授权测试域名，[业务后台](https://argos-demo.infra.us.to/admin) 已提供限时一键编辑与只读体验；真实邮件验收按用户决定暂不进行，T2 及完整 demo 的该项门槛保留待验。需求分为永久仅供内部团队使用的管理端，以及交给客户的站点交付物两套，以可维护的源码交付和插件、主题组装为核心。管理端不向客户交付或开放使用。内部工作台与站点业务后台统一以 TailAdmin 为样式基础，前台继续由主题插件控制。操作人只组装插件、创建或编辑插件和主题，核心代码由独立维护流程管理。

本仓库 `argos` 保留需求与设计文档，与核心仓库 `core`、插件及主题仓库 `plugins` 并列放置。仓库组织、职责和本地迁移说明见 [仓库结构](docs/repositories.md)。

后续日常演示和站点预览默认使用本地 Wrangler 及本地持久化 D1/R2；当前工作区在 `core` 执行 `npm run demo:local` 可启动完整公告演示并一键登录，首次准备及启动方式见 [Core 本地演示说明](../core/README.md#本地演示与预览)。远端部署仅按明确的云端验收或发布需求执行；上述远端站点保留为既有测试版本。

当前优先验证系统可行性并完成 demo，电商移出第一期，停止细化其业务规则。此前完整产品需求保留为后续参考；当前 demo 范围、完成标准和剩余开工条件见 [需求第 8 节](docs/requirements.md#8-编码前的澄清进度与就绪条件) 和 [分期决策](docs/adr/0012-feasibility-demo-first.md)。

- [总体需求与澄清记录](docs/requirements.md)
- [Demo 最小技术方案](docs/demo-design.md)：已确认 Astro 站点宿主、React 后台和 Hono API；统一插件契约、双平台数据路径与实现基线。
- [Demo 设计审查](docs/demo-review.md)：Q73–Q75 已确认，技术补充及验证门槛已记录；本地与真实云端业务及数据恢复已通过，真实邮件按用户决定暂不验收。
- [Demo 首批实现任务](docs/demo-tasks.md)：T0–T5 的实现归属、技术门槛、验收证据及外部条件，持续记录本地证据、真实验收和剩余条件。
- [管理端需求](docs/management-system.md)：Web 工作台与 agent 执行生成、工程管理、构建、交付与托管；部署不限定 Cloudflare。
- [站点交付物需求](docs/site-deliverable.md)：双平台运行时、首版交付/验收提案、客户账户代管及完整数据恢复。
- [插件生命周期](docs/plugins.md)：构建时安装、运行时启停、操作人负责插件安全。
- [站点业务后台](docs/business-admin.md)：随源码交付，插件注册对应管理功能。
- [RBAC 插件设计 v1](docs/rbac.md)：必需基础插件与纯角色授权；已补齐管理规则、接口、事务撤权、升级恢复及验收，细化提案待整体评审。
- [主题与操作边界](docs/themes.md)：页面定制位于主题，操作人及生成任务不修改核心代码。
- [仓库结构](docs/repositories.md)：设计、核心、插件三个并列的独立仓库。
- [电商需求基线](docs/commerce.md)
- [市场与物流](docs/shipping.md)：市场配置、主流物流接口适配及履约能力。
- [用户管理与 CRM](docs/users-crm.md)
- [领域术语](CONTEXT.md)
- [EmDash 调研](docs/research/emdash.md)
- [Codex 集成调研](docs/research/codex-integration.md)

架构决策位于 [docs/adr](docs/adr/)，通过状态区分已接受的决策和候选方案。各文档明确区分已确认需求和待确认的设计建议。
