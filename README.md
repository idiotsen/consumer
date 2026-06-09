# consumer-pages

公开静态站发布仓库本地副本。

本目录保存 `../macro_dashboard` 构建后同步过来的 Pages 静态产物，用于公开展示宏观、地产高频、服务消费和市场跟踪看板。

## 主要内容

- `index.html`：站点首页。
- `macro/`：宏观看板页面。
- `real-estate-high-frequency/`：地产高频页面。
- `service-consumption/`：服务消费页面。
- `market-tracking/`：市场跟踪页面。
- `_astro/`：Astro 构建产物。
- `data/`：公开看板 JSON 数据。
- `sitemap*.xml`、`robots.txt`、`.nojekyll`：Pages 发布辅助文件。

## 使用口径

默认不要手工编辑本目录的发布产物。应从 `../macro_dashboard` 执行构建和发布脚本后同步到这里。
