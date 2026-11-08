---
status: accepted
date: 2026-11-08
---

# 交付站点采用薄 Astro 宿主，保留业务插件契约

用户在比较 Astro 与 React/Vite＋Hono、明确其对核心及插件的影响后，同意以下边界：

- 核心提供薄 Astro 宿主，承担页面注册、请求上下文及目标平台构建接入。
- 交付站点主题采用 Astro 页面与布局，按需使用 React；独立业务后台采用 React，业务 API 保留 Hono。
- 依赖解析、RBAC、存储、导出恢复及启停规则继续由 Argos 提供；业务服务通过核心契约访问，不直接依赖 Astro 上下文或平台 binding。
- 生成任务只修改插件、主题及组装配置，核心根据声明自动注册，不因站点需求改写固定核心。
- 路由在构建时注册，运行时按插件有效状态控制请求、页面和后台入口；停用不等于卸载编译后的路由。需要随内容修改或停用立即生效的页面使用按需渲染，不能只隐藏菜单或保留可绕过检查的静态页面。
- 第一阶段只实现这一套主题渲染入口，不建设通用多框架插件系统。内部工作台继续采用 React/Vite＋Node.js/Hono，与交付运行时分别打包。

同一份交付源码分别使用 Node.js 与 Cloudflare adapter 构建，平台特定资源由薄适配连接；不承诺同一个构建产物直接跨平台运行。该选择不改变客户源码交接、独立后台、构建时安装与运行时启停等已确认产品边界。

依据为 Astro 官方 [integration 页面注册](https://docs.astro.build/en/reference/integrations-reference/#injectroute-option)、[Hono 集成](https://docs.astro.build/en/reference/modules/astro-hono/)及[按需渲染](https://docs.astro.build/en/guides/on-demand-rendering/)能力；具体职责分配为本项目架构决策，不表示已有运行验证。

Demo 实现基线见 [最小技术方案](../demo-design.md)，执行与证据见 [首批实现任务](../demo-tasks.md)。完整 RBAC v1、后续业务和生产运维提案不会因本次框架确认而整体获批；尚未开始功能编码或真实部署。
