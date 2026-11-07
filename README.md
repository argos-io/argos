# Argos

Argos 是内部团队使用的站点生成与管理系统：Web 工作台调用编码 agent 组装插件、创建或编辑插件和主题，交付可在 Cloudflare 或标准 Node.js 上运行的站点源码及独立业务后台。

## 仓库结构

| 子模块 | 独立仓库 | 职责 |
| --- | --- | --- |
| [design](design/README.md) | `argos-design` | 需求、领域术语、设计、ADR 与调研 |
| [core](core/README.md) | `argos-core` | 内部管理端、站点运行时、后台宿主与扩展契约 |
| [plugins](plugins/README.md) | `argos-plugins` | 业务插件、供应商适配器、主题及其后台管理功能 |

操作人只组装插件、创建或编辑插件与主题，站点生成任务不调整核心代码。核心维护走独立开发和发布流程，详见 [操作边界](design/docs/themes.md) 与 [仓库组织](design/docs/repositories.md)。

## 当前状态

目前已落地需求设计、仓库拆分和初始化说明，运行时与管理端代码尚未实现。总入口记录三个子模块的具体提交，每个子模块维护独立 Git 历史。

本次完成的是本地迁移。总入口复用已有 `argos-io/argos` 仓库，子模块配置为 `argos-io/argos-design`、`argos-io/argos-core`、`argos-io/argos-plugins`，这些子仓库尚未在 GitHub 创建或推送。远端具备相应仓库和提交后，才可从 GitHub 递归克隆完整项目。

## 子模块协作

```bash
# 已发布远端的工作区初始化
git submodule update --init --recursive

# 查看各仓库及总入口状态
git submodule status
git status --short
git -C design status --short
git -C core status --short
git -C plugins status --short
```

子模块变更先在各自仓库提交，再在总入口提交更新后的子模块引用。发布时先确保子模块提交在相应远端可获取，再推送总入口引用。

项目入口为 `/root/src/github.com/argos-io/argos`。原设计仓库的 `paas` 历史分支保留在 `design` 中，历史项目配置保存在本地迁移快照中，不作为 Argos 当前产品实现。
