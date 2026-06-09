# consumer-pages Runbook

## 日常使用

本目录通常不直接运行开发命令。日常发布入口在：

```bash
cd /home/jujusen/workspace/macro_dashboard
npm run publish:all
```

## 排查发布产物

1. 确认 `../macro_dashboard` 构建成功。
2. 检查本目录是否包含最新 `index.html`、页面目录、`_astro/` 和 `data/*.json`。
3. 如需本地预览，可在本目录启动简单静态服务：

```bash
python3 -m http.server 8000
```

## 注意事项

- 不要把源码、`node_modules/`、原始 Excel 或 ETL 缓存放入本目录。
- 发布流程和自动化调度记忆保存在 `../macro_dashboard`。
