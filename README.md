# Argos

Argos 是内部团队使用的站点生成与管理系统：Web 工作台调用编码 agent 组装插件、创建或编辑插件和主题，交付可在 Cloudflare 或标准 Node.js 上运行的站点源码及独立业务后台。

本仓库承载核心系统，包括内部管理端基础、站点最小运行时、通用业务后台宿主，以及插件、主题和平台适配的公共契约。管理端永久仅供内部使用，交付运行时与管理端分别打包。

## 独立仓库

三个仓库并列放置在 `/root/src/github.com/argos-io/`，各自维护 Git 历史、提交与远端。

| 仓库 | 职责 |
| --- | --- |
| `argos`（本仓库） | 核心系统、内部管理端基础、运行时、后台宿主与扩展契约 |
| [argos-design](../argos-design/README.md) | 需求、领域术语、设计、ADR 与调研 |
| [argos-plugins](../argos-plugins/README.md) | 业务插件、供应商适配器、主题及其后台管理功能 |

操作人只组装插件、创建或编辑插件与主题，站点生成任务不调整核心代码。核心维护走独立开发和发布流程，详见 [操作边界](../argos-design/docs/themes.md) 与 [仓库组织](../argos-design/docs/repositories.md)。

## 当前状态

目前已落地需求设计与独立仓库骨架，运行时与管理端代码尚未实现，技术栈和具体包结构待设计。

本次只调整本地仓库。`argos` 复用已有远端配置；`argos-design` 与 `argos-plugins` 已配置目标远端，尚未在 GitHub 创建或推送。

历史备份分支 `paas` 保留在 `argos-design` 中。迁移前快照保存在本仓库的 Git 元数据中；原核心骨架提交通过 `migration/core-initial` 标签保留。
