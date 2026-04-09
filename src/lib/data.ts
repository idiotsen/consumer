import fs from "node:fs/promises";
import path from "node:path";

import type { BasePayload, LoadedPayload, TimeSeriesItem } from "@/lib/types";

function resolveState<T extends BasePayload>(
  payload: T,
  exists: boolean,
  ageMinutes: number | null,
  errorMessage?: string,
): LoadedPayload<T>["state"] {
  if (!exists) {
    return "missing";
  }
  if (errorMessage || payload.status === "error") {
    return "error";
  }
  if (ageMinutes !== null && ageMinutes > payload.stale_after_minutes) {
    return "stale";
  }
  if (payload.status === "partial") {
    return "partial";
  }
  return "ready";
}

export async function loadPublicJson<T extends BasePayload>(
  filename: string,
  fallback: T,
): Promise<LoadedPayload<T>> {
  const filePath = path.join(process.cwd(), "public", "data", filename);
  try {
    const raw = await fs.readFile(filePath, "utf-8");
    const payload = JSON.parse(raw) as T;
    const updated = Date.parse(payload.updated_at);
    const ageMinutes = Number.isNaN(updated) ? null : Math.max(0, Math.round((Date.now() - updated) / 60000));
    return {
      data: payload,
      exists: true,
      ageMinutes,
      state: resolveState(payload, true, ageMinutes),
    };
  } catch (error) {
    return {
      data: fallback,
      exists: false,
      ageMinutes: null,
      state: resolveState(fallback, false, null, error instanceof Error ? error.message : String(error)),
      errorMessage: error instanceof Error ? error.message : String(error),
    };
  }
}

export function formatDateTime(value?: string): string {
  if (!value) {
    return "未知时间";
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return value;
  }
  return new Intl.DateTimeFormat("zh-CN", {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).format(parsed);
}

export function formatDateOnly(value?: string): string {
  if (!value) {
    return "未发布";
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return value;
  }
  return new Intl.DateTimeFormat("zh-CN", {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(parsed);
}

export function formatMetricValue(value: string | number, unit: string): string {
  if (typeof value === "number") {
    const normalized = Number.isInteger(value) ? value.toString() : value.toFixed(1);
    return `${normalized}${unit}`;
  }
  return `${value}${unit}`;
}

export function formatNullableNumber(value?: number | null, digits = 1): string {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "待补充";
  }
  return value.toLocaleString("zh-CN", {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
}

export function formatNullablePercent(value?: number | null, digits = 1): string {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "待补充";
  }
  return `${formatNullableNumber(value, digits)}%`;
}

export function formatSignedPercent(value?: number | null, digits = 1): string {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "待补充";
  }
  const sign = value > 0 ? "+" : "";
  return `${sign}${formatNullableNumber(value, digits)}%`;
}

export function formatCompactCurrency(value?: number | null): string {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "待补充";
  }
  return new Intl.NumberFormat("zh-CN", {
    notation: "compact",
    maximumFractionDigits: 1,
  }).format(value);
}

export function seriesByGroup(series: TimeSeriesItem[], group: string): TimeSeriesItem[] {
  return series.filter((item) => item.group === group);
}

export function latestSeriesValue(series: TimeSeriesItem): number | null {
  return series.values.length ? series.values[series.values.length - 1] : null;
}

export function previousSeriesValue(series: TimeSeriesItem): number | null {
  return series.values.length > 1 ? series.values[series.values.length - 2] : null;
}

export function deltaText(series: TimeSeriesItem): string {
  const latest = latestSeriesValue(series);
  const previous = previousSeriesValue(series);
  if (latest === null || previous === null) {
    return "暂无变化数据";
  }
  const delta = latest - previous;
  const direction = delta > 0 ? "较前值上升" : delta < 0 ? "较前值回落" : "较前值持平";
  return `${direction} ${Math.abs(delta).toFixed(1)}${series.unit}`;
}
