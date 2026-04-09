export const siteMeta = {
  name: "Macro Sector Dashboard",
  defaultTitle: "宏观、地产高频与市场跟踪看板",
  defaultDescription:
    "基于 Astro + Tushare + Excel + Python ETL 的多板块看板，覆盖宏观图表、地产高频与市场跟踪。",
};

export const boardPages = [
  {
    boardId: "macro",
    kind: "macro",
    href: "/macro",
    label: "宏观",
    teaser: "通胀、收入、居民资负表与消费意愿等图表集中展示在宏观看板中，便于单独对外呈现。",
  },
  {
    boardId: "real-estate-high-frequency",
    kind: "real-estate-high-frequency",
    href: "/real-estate-high-frequency",
    label: "地产高频",
    teaser: "地产高频板块集中展示二手房价格与活跃度，以及新房价格与销售变化。",
  },
  {
    boardId: "market-tracking",
    kind: "market",
    href: "/market-tracking",
    label: "市场跟踪",
    teaser: "市场跟踪用于补充消费行业 PB-ROE、拥挤度和券商金股中消费个股占比的横向观察。",
  },
] as const;

export const navigationItems = boardPages.map(({ href, label }) => ({ href, label }));

export function getBoardHref(boardId: string): string {
  return boardPages.find((item) => item.boardId === boardId)?.href ?? "/";
}

export function withBasePath(path: string): string {
  const base = import.meta.env.BASE_URL;
  const normalizedBase = base.endsWith("/") ? base.slice(0, -1) : base;

  if (path.startsWith("#")) {
    return `${base}${path.slice(1) ? `#${path.slice(1)}` : ""}`;
  }

  if (path.includes("#")) {
    const [pathname, hash] = path.split("#", 2);
    const resolvedPath = withBasePath(pathname || "/");
    return hash ? `${resolvedPath}#${hash}` : resolvedPath;
  }

  if (path === "/") {
    return base;
  }

  return `${normalizedBase}${path}`;
}
