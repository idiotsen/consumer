import type {
  BriefingPayload,
  DashboardSummary,
  GeoEventsPayload,
  MacroBoardPayload,
  MarketTrackingBoardPayload,
  NewsFeed,
  RealEstateHighFrequencyBoardPayload,
  SectorMarketPanelsPayload,
  SectorMarketSeriesPayload,
  TimeSeriesPayload,
} from "@/lib/types";

const fallbackTimestamp = "2026-03-30T00:00:00+08:00";

export function createEmptyDashboardSummary(): DashboardSummary {
  return {
    updated_at: fallbackTimestamp,
    source: "fallback",
    stale_after_minutes: 180,
    status: "error",
    status_message: "dashboard-summary.json 尚未生成。",
    topic: {
      slug: "fallback",
      title: "专题数据暂不可用",
      subtitle: "等待 ETL 生成看板摘要",
      description: "运行 `npm run etl` 后，这里会显示专题概览、指标卡与数据源状态。",
    },
    alerts: [],
    metrics: [],
    source_status: [],
    highlights: [],
  };
}

export function createEmptyTimeseries(): TimeSeriesPayload {
  return {
    updated_at: fallbackTimestamp,
    source: "fallback",
    stale_after_minutes: 180,
    status: "error",
    status_message: "timeseries.json 尚未生成。",
    series: [],
  };
}

export function createEmptyNewsFeed(): NewsFeed {
  return {
    updated_at: fallbackTimestamp,
    source: "fallback",
    stale_after_minutes: 60,
    status: "error",
    status_message: "news-feed.json 尚未生成。",
    counts: {
      approved: 0,
      pending: 0,
    },
    items: [],
  };
}

export function createEmptyBriefing(): BriefingPayload {
  return {
    updated_at: fallbackTimestamp,
    source: "fallback",
    stale_after_minutes: 180,
    status: "error",
    status_message: "briefing.json 尚未生成。",
    briefing: {
      id: "briefing-fallback",
      date: "",
      title: "暂无简报",
      headline: "请先生成或发布一篇已审核简报。",
      latest_updates: [],
      timeline: [],
      analysis_sections: [],
      market_impacts: [],
      watchlist: [],
      review_status: "approved",
    },
  };
}

export function createEmptyGeoEvents(): GeoEventsPayload {
  return {
    updated_at: fallbackTimestamp,
    source: "fallback",
    stale_after_minutes: 360,
    status: "error",
    status_message: "geo-events.json 尚未生成。",
    center: {
      lat: 26.7,
      lng: 54.5,
      zoom: 5,
    },
    counts: {},
    events: [],
  };
}

export function createEmptySectorMarketPanels(): SectorMarketPanelsPayload {
  return {
    updated_at: fallbackTimestamp,
    source: "fallback",
    stale_after_minutes: 1440,
    status: "error",
    status_message: "sector-market-panels.json 尚未生成。",
    boards: [],
  };
}

export function createEmptySectorMarketSeries(): SectorMarketSeriesPayload {
  return {
    updated_at: fallbackTimestamp,
    source: "fallback",
    stale_after_minutes: 1440,
    status: "error",
    status_message: "sector-market-series.json 尚未生成。",
    series: [],
  };
}

export function createEmptyMacroBoard(): MacroBoardPayload {
  return {
    updated_at: fallbackTimestamp,
    source: "fallback",
    stale_after_minutes: 1440,
    status: "error",
    status_message: "macro-board.json 尚未生成。",
    board_id: "macro",
    title: "宏观",
    description: "宏观看板数据尚未生成。",
    sections: [],
  };
}

export function createEmptyMarketTrackingBoard(): MarketTrackingBoardPayload {
  return {
    updated_at: fallbackTimestamp,
    source: "fallback",
    stale_after_minutes: 1440,
    status: "error",
    status_message: "market-tracking-board.json 尚未生成。",
    board_id: "market-tracking",
    title: "市场跟踪",
    description: "市场跟踪看板数据尚未生成。",
    sections: [],
  };
}

export function createEmptyRealEstateHighFrequencyBoard(): RealEstateHighFrequencyBoardPayload {
  return {
    updated_at: fallbackTimestamp,
    source: "fallback",
    stale_after_minutes: 1440,
    status: "error",
    status_message: "real-estate-high-frequency-board.json 尚未生成。",
    board_id: "real-estate-high-frequency",
    title: "地产高频",
    description: "地产高频看板数据尚未生成。",
    sections: [],
  };
}
