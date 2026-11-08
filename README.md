# Argos 设计与需求

Argos 处于需求与设计阶段。需求分为永久仅供内部团队使用的管理端，以及交给客户的站点交付物两套，以可维护的源码交付和插件、主题组装为核心。管理端不向客户交付或开放使用。操作人只组装插件、创建或编辑插件和主题，核心代码由独立维护流程管理。

本仓库 `argos` 保留需求与设计文档，与核心仓库 `core`、插件及主题仓库 `plugins` 并列放置。仓库组织、职责和本地迁移说明见 [仓库结构](docs/repositories.md)。

- [总体需求与澄清记录](docs/requirements.md)
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
