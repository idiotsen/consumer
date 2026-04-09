export interface BasePayload {
  updated_at: string;
  source: string;
  stale_after_minutes: number;
  status?: string;
  status_message?: string;
}

export interface SectorFundamentalsPlaceholder {
  title: string;
  description: string;
  planned_metrics: string[];
}

export type IndustryPanelStatus = "ready" | "partial" | "error" | "coming_soon";
export type PbRoeAlignmentStatus = "expensive_vs_roe" | "balanced" | "cheap_vs_roe" | "empty";

export interface IndustryPbPercentiles {
  three_year: number | null;
  five_year: number | null;
}

export interface IndustryCrowdedness {
  amount_pct: number | null;
  turnover_ratio_pct: number | null;
  score: number | null;
  label: string;
}

export interface IndustryPbRoeAlignment {
  pb_pct_3y: number | null;
  pb_pct_5y: number | null;
  roe_pct_20q: number | null;
  current_roe: number | null;
  current_roe_period: string | null;
  match_gap: number | null;
  status: PbRoeAlignmentStatus;
  label: string;
}

export interface SectorIndustryPanel {
  ts_code: string;
  name: string;
  market: string;
  enabled: boolean;
  status: IndustryPanelStatus;
  status_message: string;
  latest_trade_date?: string;
  latest_close?: number;
  latest_pct_change?: number;
  pb?: number;
  amount?: number;
  float_mv?: number;
  member_count?: number;
  pb_percentiles?: IndustryPbPercentiles;
  crowdedness?: IndustryCrowdedness;
  pb_roe_alignment?: IndustryPbRoeAlignment;
}

export interface SectorBoard {
  board_id: string;
  title: string;
  anchor: string;
  description: string;
  fundamentals_placeholder: SectorFundamentalsPlaceholder;
  industries: SectorIndustryPanel[];
}

export interface SectorMarketPanelsPayload extends BasePayload {
  boards: SectorBoard[];
}

export interface SectorSeriesItem {
  board_id: string;
  ts_code: string;
  label: string;
  source: string;
  benchmark_code: string;
  benchmark_label: string;
  latest_trade_date: string | null;
  dates: string[];
  close: Array<number | null>;
  normalized_close: Array<number | null>;
  benchmark_close: Array<number | null>;
  benchmark_normalized_close: Array<number | null>;
  relative_to_benchmark: Array<number | null>;
  pb: Array<number | null>;
  amount: Array<number | null>;
  turnover_value_ratio: Array<number | null>;
}

export interface SectorMarketSeriesPayload extends BasePayload {
  series: SectorSeriesItem[];
}

export type MacroSectionStatus = "ready" | "coming_soon" | "error";
export type MacroChartAxisId = "left" | "right";
export type MacroChangeDirection = "up" | "down" | "flat";

export interface MacroChartSeries {
  series_id: string;
  label: string;
  unit: string;
  y_axis_id: MacroChartAxisId;
  render_type?: "line" | "bar";
  stack?: string;
  span_gaps: boolean;
  show_point_markers?: boolean;
  values: Array<number | null>;
}

export interface MacroChartLatestChange {
  label: string;
  basis_label: string;
  direction: MacroChangeDirection;
  delta_value: number;
  unit: string;
}

export type MacroChartAnnotationTone = "latest" | "high" | "low" | "neutral";

export interface MacroChartAnnotation {
  series_id: string;
  point_index: number;
  label: string;
  tone?: MacroChartAnnotationTone;
  dx?: number;
  dy?: number;
}

export interface MacroChartConfig {
  chart_id: string;
  title: string;
  description: string;
  chart_type: "line" | "combo";
  x_axis_label: string;
  latest_label: string;
  latest_time_text: string;
  show_point_markers?: boolean;
  latest_changes?: MacroChartLatestChange[];
  annotations?: MacroChartAnnotation[];
  labels: string[];
  series: MacroChartSeries[];
}

export interface MacroSection {
  section_id: string;
  title: string;
  description: string;
  status: MacroSectionStatus;
  placeholder_message?: string;
  chart?: MacroChartConfig;
  charts?: MacroChartConfig[];
}

export interface MacroBoardPayload extends BasePayload {
  board_id: string;
  title: string;
  description: string;
  sections: MacroSection[];
}

export interface RealEstateHighFrequencyBoardPayload extends BasePayload {
  board_id: string;
  title: string;
  description: string;
  sections: MacroSection[];
}

export type MarketTrackingSectionStatus = "ready" | "coming_soon" | "error";

export interface MarketTrackingCoverageSummary {
  latest_month: string;
  months_covered: number;
  latest_broker_count: number;
  min_broker_count: number;
  max_broker_count: number;
  broker_count_consistent: boolean;
  broker_members_consistent: boolean;
  latest_total_recommendations: number;
  latest_consumer_recommendations: number;
  latest_consumer_stock_count: number;
  latest_consumer_share_pct: number;
  comparison_note: string;
}

export interface MarketTrackingRecommendedStock {
  ts_code: string;
  name: string;
  industry_name: string;
  broker_count: number;
  previous_broker_count?: number | null;
  broker_count_delta?: number | null;
  broker_names: string[];
}

export interface MarketTrackingCrowdednessIndustry {
  ts_code: string;
  industry_name: string;
  level: "L1" | "L2";
  parent_l1_name?: string | null;
  total_score: number;
  high_crowding: boolean;
  excluded_signal: boolean;
  crowded_20d_count: number;
}

export interface MarketTrackingCrowdednessGroup {
  group_id: "l1" | "l2";
  title: string;
  as_of_date: string;
  industries: MarketTrackingCrowdednessIndustry[];
}

export interface MarketTrackingScatterPoint {
  ts_code: string;
  industry_name: string;
  level: "L1" | "L2";
  parent_l1_name?: string | null;
  pb_percentile_5y: number;
  roe_percentile_5y: number;
  latest_pb: number;
  latest_pb_date: string;
  latest_roe: number;
  latest_roe_period: string;
  is_consumer_related: boolean;
}

export interface MarketTrackingScatterChartConfig {
  chart_id: string;
  title: string;
  description: string;
  chart_type: "scatter";
  x_axis_label: string;
  y_axis_label: string;
  latest_label: string;
  latest_time_text: string;
  points: MarketTrackingScatterPoint[];
}

export interface MarketTrackingSection {
  section_id: string;
  title: string;
  description: string;
  status: MarketTrackingSectionStatus;
  placeholder_message?: string;
  notice_message?: string;
  chart?: MacroChartConfig;
  charts?: MarketTrackingScatterChartConfig[];
  coverage?: MarketTrackingCoverageSummary;
  recommended_stocks?: MarketTrackingRecommendedStock[];
  crowdedness_groups?: MarketTrackingCrowdednessGroup[];
}

export interface MarketTrackingBoardPayload extends BasePayload {
  board_id: string;
  title: string;
  description: string;
  sections: MarketTrackingSection[];
}

export interface TopicMeta {
  slug: string;
  title: string;
  subtitle: string;
  description: string;
}

export interface AlertItem {
  level: "critical" | "warning" | "info" | "healthy";
  title: string;
  description: string;
  link?: string;
}

export interface MetricItem {
  id: string;
  label: string;
  value: number | string;
  unit: string;
  status: string;
  trend: string;
  delta_text: string;
  description: string;
}

export interface SourceStatusItem {
  id: string;
  name: string;
  status: string;
  last_updated: string;
  latency_minutes: number;
  detail: string;
}

export interface DashboardSummary extends BasePayload {
  topic: TopicMeta;
  alerts: AlertItem[];
  metrics: MetricItem[];
  source_status: SourceStatusItem[];
  highlights?: string[];
}

export interface TimeSeriesItem {
  series_id: string;
  label: string;
  group: string;
  unit: string;
  source: string;
  source_url?: string;
  updated_at: string;
  dates: string[];
  values: Array<number | null>;
  mom_values?: Array<number | null>;
  yoy_values?: Array<number | null>;
}

export interface TimeSeriesPayload extends BasePayload {
  series: TimeSeriesItem[];
}

export interface NewsItem {
  id: string;
  title: string;
  source: string;
  url: string;
  published_at: string;
  tags: string[];
  summary: string;
  review_status: "approved";
}

export interface NewsFeed extends BasePayload {
  counts: {
    approved: number;
    pending: number;
  };
  items: NewsItem[];
}

export interface BriefingTimelineItem {
  time: string;
  event: string;
}

export interface BriefingAnalysisSection {
  title: string;
  body: string;
}

export interface BriefingMarketImpact {
  asset: string;
  impact: string;
}

export interface BriefingItem {
  id: string;
  date: string;
  title: string;
  headline: string;
  latest_updates: string[];
  timeline: BriefingTimelineItem[];
  analysis_sections: BriefingAnalysisSection[];
  market_impacts: BriefingMarketImpact[];
  watchlist: string[];
  review_status: "approved";
}

export interface BriefingPayload extends BasePayload {
  briefing: BriefingItem;
}

export interface GeoEventItem {
  id: string;
  type: string;
  title: string;
  summary: string;
  lat: number;
  lng: number;
  status: string;
  started_at: string;
  source: string;
  link?: string;
}

export interface GeoEventsPayload extends BasePayload {
  center: {
    lat: number;
    lng: number;
    zoom: number;
  };
  counts?: Record<string, number>;
  events: GeoEventItem[];
}

export interface LoadedPayload<T extends BasePayload> {
  data: T;
  exists: boolean;
  state: "ready" | "partial" | "stale" | "missing" | "error";
  ageMinutes: number | null;
  errorMessage?: string;
}
