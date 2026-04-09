# 宏观看板

这是一个基于 `Astro + Python ETL` 的静态看板项目。当前公开站点只保留 3 个板块：

- `宏观`
- `地产高频`
- `市场跟踪`

当前发布方式已调整为：

- **本地**运行 ETL，生成 `public/data/*.json`
- **GitHub Pages** 只基于现成 JSON 构建并发布静态站

这样做的目的有两个：

- 不把 `input_data/*.xlsx` 上传到公开仓库
- 不把 `TUSHARE_TOKEN` 放到 GitHub

## 技术栈

- `Astro`：静态站点生成
- `Tailwind CSS`：页面样式
- `Chart.js`：图表渲染
- `Python ETL`：本地生成标准化 JSON
- `GitHub Actions + GitHub Pages`：静态站发布

## 当前公开页面

- `/`：跳转到 `/macro`
- `/macro`
- `/real-estate-high-frequency`
- `/market-tracking`

## 本地准备

先安装依赖：

```powershell
npm install
pip install -r requirements.txt
```

设置本地 `TUSHARE_TOKEN`：

```powershell
$env:TUSHARE_TOKEN="你的 token"
```

## 本地更新数据

所有原始输入文件都只保留在本地 `input_data/` 中，不上传到 GitHub。当前本地输入包括：

- `input_data/bingshan_index.xlsx`
- `input_data/new_house.xlsx`
- `input_data/employee_history.xlsx`
- `input_data/consumer_confidence.xlsx`
- `input_data/household_asset.xlsx`
- `input_data/inflation.xlsx`

每次刷新数据时，固定按这个顺序：

```powershell
npm run etl
python scripts/validate_data.py
npm run build
```

说明：

- `npm run etl`：本地读取 `input_data/`、调用抓数逻辑，并生成 `public/data/*.json`
- `python scripts/validate_data.py`：校验公开 JSON
- `npm run build`：只使用现成的 `public/data/*.json` 构建静态站，不会再自动跑 ETL

## 本地预览

在已经生成好 `public/data/*.json` 后，再启动开发或预览：

```powershell
npm run dev
```

或：

```powershell
npm run preview
```

## GitHub Pages 发布

GitHub 侧不再在线抓数，只负责：

- 安装依赖
- 校验 `public/data/*.json`
- 构建静态站
- 发布 `dist/`

### GitHub 仓库里需要保留的内容

必须上传：

- `src/`
- `public/data/*.json`
- `scripts/`
- `schemas/`
- `.github/workflows/deploy.yml`
- `package.json`
- `package-lock.json`
- `requirements.txt`
- `astro.config.mjs`

不要上传：

- `input_data/`
- `data_sources/cache/`
- `dist/`
- `.astro/`
- `node_modules/`

### GitHub Actions 配置

仓库 `Settings -> Secrets and variables -> Actions` 中只需要配置：

- `SITE_URL = https://<你的 GitHub 用户名>.github.io`
- `BASE_PATH = /<仓库名>`

不再需要配置：

- `TUSHARE_TOKEN`

### Pages 设置

在 `Settings -> Pages` 中：

- `Source` 选择 `GitHub Actions`

推送到 `main` 后会自动发布。

## 后续刷新流程

以后每次线上更新都按同一套流程：

1. 在本地更新 `input_data/`
2. 运行：

```powershell
npm run etl
python scripts/validate_data.py
npm run build
```

3. 提交更新后的 `public/data/*.json`
4. push 到 GitHub
5. GitHub Pages 自动重新部署

## 常用命令

```powershell
npm run etl
python scripts/validate_data.py
npm run build
npm run dev
npm run preview
```
