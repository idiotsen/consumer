import { Chart, registerables } from "chart.js";

export type ScatterPointPayload = {
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
};

export type ScatterChartPayload = {
  chart_id: string;
  title: string;
  description: string;
  x_axis_label: string;
  y_axis_label: string;
  latest_label: string;
  latest_time_text: string;
  points: ScatterPointPayload[];
};

type Rect = {
  x: number;
  y: number;
  width: number;
  height: number;
};

type Bounds = {
  left: number;
  right: number;
  top: number;
  bottom: number;
};

type Candidate = {
  rect: Rect;
  anchorX: number;
  anchorY: number;
  overlapCount: number;
};

type LabelMode = "none" | "full" | "short";

let chartRegistered = false;

function clamp(value: number, min: number, max: number) {
  return Math.min(Math.max(value, min), max);
}

function rectsOverlap(a: Rect, b: Rect) {
  return !(
    a.x + a.width <= b.x ||
    b.x + b.width <= a.x ||
    a.y + a.height <= b.y ||
    b.y + b.height <= a.y
  );
}

function rectOverlapsProtectedPoint(rect: Rect, point: { x: number; y: number; radius: number }) {
  const nearestX = clamp(point.x, rect.x, rect.x + rect.width);
  const nearestY = clamp(point.y, rect.y, rect.y + rect.height);
  const dx = point.x - nearestX;
  const dy = point.y - nearestY;
  return dx * dx + dy * dy < point.radius * point.radius;
}

function buildCandidateRect(
  pointX: number,
  pointY: number,
  width: number,
  height: number,
  position: "right-up" | "left-up" | "right-down" | "left-down",
  bounds: Bounds,
): Candidate {
  const offsetX = 16;
  const offsetY = 16;
  let x = pointX + offsetX;
  let y = pointY - offsetY - height;

  if (position === "left-up") {
    x = pointX - offsetX - width;
    y = pointY - offsetY - height;
  } else if (position === "right-down") {
    x = pointX + offsetX;
    y = pointY + offsetY;
  } else if (position === "left-down") {
    x = pointX - offsetX - width;
    y = pointY + offsetY;
  }

  const rect = {
    x: clamp(x, bounds.left, bounds.right - width),
    y: clamp(y, bounds.top, bounds.bottom - height),
    width,
    height,
  };
  return {
    rect,
    anchorX: clamp(pointX, rect.x, rect.x + rect.width),
    anchorY: clamp(pointY, rect.y, rect.y + rect.height),
    overlapCount: 0,
  };
}

function compactIndustryName(name: string) {
  const normalized = name
    .replace(/\s+/g, "")
    .replace(/[\uFF08(]\u7533\u4E07[\uFF09)]/g, "")
    .replace(/[\u2161\u2162]/g, "")
    .trim();
  if (normalized.length <= 6) {
    return normalized;
  }
  return normalized.slice(0, 6);
}

function labelModeForChart(chartId: string): LabelMode {
  if (chartId === "consumer-pb-roe-l1") {
    return "full";
  }
  if (chartId === "consumer-pb-roe-l2") {
    return "short";
  }
  return "none";
}

const marketScatterLabelPlugin = {
  id: "marketScatterLabels",
  afterDatasetsDraw(chart: Chart, _args: unknown, pluginOptions?: { labelMode?: LabelMode }) {
    if (!pluginOptions?.labelMode || pluginOptions.labelMode === "none") {
      return;
    }

    const consumerDatasetIndex = chart.data.datasets.findIndex(
      (dataset) => (dataset as { datasetRole?: string }).datasetRole === "consumer",
    );
    if (consumerDatasetIndex === -1) {
      return;
    }

    const consumerMeta = chart.getDatasetMeta(consumerDatasetIndex);
    const consumerDataset = chart.data.datasets[consumerDatasetIndex] as unknown as {
      data: ScatterPointPayload[];
    };
    if (!consumerMeta.data.length || !consumerDataset.data.length) {
      return;
    }

    const { ctx, chartArea } = chart;
    const useCompactLabel = pluginOptions.labelMode === "short";
    const padding = 8;
    const bounds = {
      left: Math.max(padding, chartArea.left - (useCompactLabel ? 34 : 42)),
      right: Math.min(chart.width - padding, chartArea.right + (useCompactLabel ? 34 : 42)),
      top: Math.max(padding, chartArea.top - (useCompactLabel ? 18 : 22)),
      bottom: Math.min(chart.height - padding, chartArea.bottom + (useCompactLabel ? 20 : 24)),
    };

    const protectedPoints: Array<{ x: number; y: number; radius: number }> = [];
    chart.data.datasets.forEach((_dataset, datasetIndex) => {
      const meta = chart.getDatasetMeta(datasetIndex);
      meta.data.forEach((point) => {
        const { x, y } = point.getProps(["x", "y"], true) as { x: number; y: number };
        protectedPoints.push({
          x,
          y,
          radius: datasetIndex === consumerDatasetIndex ? 15 : 11,
        });
      });
    });

    const candidatesOrder: Array<"right-up" | "left-up" | "right-down" | "left-down"> = [
      "right-up",
      "left-up",
      "right-down",
      "left-down",
    ];
    const placedRects: Rect[] = [];

    ctx.save();
    ctx.font = `${useCompactLabel ? 10.5 : 12}px "Microsoft YaHei", "PingFang SC", sans-serif`;
    ctx.textBaseline = "middle";
    ctx.lineWidth = useCompactLabel ? 0.85 : 1;
    ctx.strokeStyle = "rgba(220, 38, 38, 0.72)";
    ctx.fillStyle = "#0f172a";

    consumerDataset.data
      .map((raw, index) => ({ raw, index }))
      .sort((a, b) => {
        const aProps = consumerMeta.data[a.index]?.getProps(["x", "y"], true) as { x: number; y: number };
        const bProps = consumerMeta.data[b.index]?.getProps(["x", "y"], true) as { x: number; y: number };
        return aProps.y - bProps.y || aProps.x - bProps.x;
      })
      .forEach(({ raw, index }) => {
        const point = consumerMeta.data[index];
        if (!point) {
          return;
        }

        const pointProps = point.getProps(["x", "y"], true) as { x: number; y: number };
        const text = useCompactLabel ? compactIndustryName(raw.industry_name) : raw.industry_name;
        const textWidth = ctx.measureText(text).width;
        const boxWidth = Math.ceil(textWidth + (useCompactLabel ? 16 : 20));
        const boxHeight = useCompactLabel ? 20 : 24;

        let chosen: Candidate | null = null;
        let fallback: Candidate | null = null;

        for (const position of candidatesOrder) {
          const candidate = buildCandidateRect(pointProps.x, pointProps.y, boxWidth, boxHeight, position, bounds);
          const overlapsPoints = protectedPoints.some((protectedPoint) =>
            rectOverlapsProtectedPoint(candidate.rect, protectedPoint),
          );
          const overlapCount = placedRects.filter((placed) => rectsOverlap(candidate.rect, placed)).length;
          candidate.overlapCount = overlapCount;

          if (!overlapsPoints && overlapCount === 0) {
            chosen = candidate;
            break;
          }

          if (!overlapsPoints && fallback === null) {
            fallback = candidate;
          }
        }

        const finalCandidate =
          chosen ??
          fallback ??
          buildCandidateRect(pointProps.x, pointProps.y, boxWidth, boxHeight, "right-up", bounds);
        placedRects.push(finalCandidate.rect);

        const rect = finalCandidate.rect;
        const radius = useCompactLabel ? 7 : 8;

        ctx.beginPath();
        ctx.moveTo(pointProps.x, pointProps.y);
        ctx.lineTo(finalCandidate.anchorX, finalCandidate.anchorY);
        ctx.stroke();

        ctx.beginPath();
        ctx.moveTo(rect.x + radius, rect.y);
        ctx.lineTo(rect.x + rect.width - radius, rect.y);
        ctx.quadraticCurveTo(rect.x + rect.width, rect.y, rect.x + rect.width, rect.y + radius);
        ctx.lineTo(rect.x + rect.width, rect.y + rect.height - radius);
        ctx.quadraticCurveTo(rect.x + rect.width, rect.y + rect.height, rect.x + rect.width - radius, rect.y + rect.height);
        ctx.lineTo(rect.x + radius, rect.y + rect.height);
        ctx.quadraticCurveTo(rect.x, rect.y + rect.height, rect.x, rect.y + rect.height - radius);
        ctx.lineTo(rect.x, rect.y + radius);
        ctx.quadraticCurveTo(rect.x, rect.y, rect.x + radius, rect.y);
        ctx.closePath();
        ctx.fillStyle = "rgba(255, 255, 255, 0.96)";
        ctx.fill();
        ctx.strokeStyle = "rgba(220, 38, 38, 0.38)";
        ctx.stroke();

        ctx.fillStyle = "#0f172a";
        ctx.textAlign = "center";
        ctx.fillText(text, rect.x + rect.width / 2, rect.y + rect.height / 2 + 0.5);
      });

    ctx.restore();
  },
};

function ensureRegistered() {
  if (chartRegistered) {
    return;
  }
  Chart.register(...registerables, marketScatterLabelPlugin);
  chartRegistered = true;
}

export function renderMarketScatterChart(canvas: HTMLCanvasElement, payload: ScatterChartPayload) {
  ensureRegistered();
  Chart.getChart(canvas)?.destroy();

  const labelMode = labelModeForChart(payload.chart_id);
  const normalPoints = payload.points
    .filter((point) => !point.is_consumer_related)
    .map((point) => ({ ...point, x: point.pb_percentile_5y, y: point.roe_percentile_5y }));
  const consumerPoints = payload.points
    .filter((point) => point.is_consumer_related)
    .map((point) => ({ ...point, x: point.pb_percentile_5y, y: point.roe_percentile_5y }));

  return new Chart(canvas, {
    type: "scatter",
    data: {
      datasets: [
        {
          label: "其他行业",
          datasetRole: "normal",
          data: normalPoints,
          backgroundColor: "rgba(100, 116, 139, 0.82)",
          borderColor: "rgba(100, 116, 139, 1)",
          pointRadius: 3.5,
          pointHoverRadius: 5,
          clip: false,
        },
        {
          label: "消费相关行业",
          datasetRole: "consumer",
          data: consumerPoints,
          backgroundColor: "rgba(220, 38, 38, 0.92)",
          borderColor: "rgba(220, 38, 38, 1)",
          pointRadius: labelMode === "none" ? 4.5 : 5,
          pointHoverRadius: labelMode === "none" ? 6 : 6.5,
          clip: false,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      parsing: false,
      layout: {
        padding:
          labelMode === "full"
            ? { top: 34, right: 58, bottom: 22, left: 34 }
            : labelMode === "short"
              ? { top: 28, right: 42, bottom: 20, left: 28 }
              : { top: 18, right: 18, bottom: 18, left: 18 },
      },
      scales: {
        x: {
          type: "linear",
          min: 0,
          max: 100,
          title: {
            display: true,
            text: payload.x_axis_label,
            color: "#475569",
            font: {
              size: 12,
              weight: "600",
            },
          },
          ticks: {
            color: "#64748b",
            stepSize: 20,
            callback: (value: number | string) => `${value}%`,
          },
          grid: {
            color: "rgba(148, 163, 184, 0.18)",
          },
        },
        y: {
          min: 0,
          max: 100,
          title: {
            display: true,
            text: payload.y_axis_label,
            color: "#475569",
            font: {
              size: 12,
              weight: "600",
            },
          },
          ticks: {
            color: "#64748b",
            stepSize: 20,
            callback: (value: number | string) => `${value}%`,
          },
          grid: {
            color: "rgba(148, 163, 184, 0.18)",
          },
        },
      },
      plugins: {
        legend: {
          display: false,
        },
        marketScatterLabels: {
          labelMode,
        },
        tooltip: {
          backgroundColor: "rgba(15, 23, 42, 0.96)",
          padding: 12,
          titleFont: {
            size: 13,
            weight: "700",
          },
          bodyFont: {
            size: 12,
          },
          callbacks: {
            title(items: Array<{ raw?: ScatterPointPayload }>) {
              return items[0]?.raw?.industry_name ?? "";
            },
            label(context: { raw?: ScatterPointPayload }) {
              const point = context.raw;
              if (!point) {
                return "";
              }
              return `PB五年分位 ${point.pb_percentile_5y.toFixed(1)}% / ROE五年分位 ${point.roe_percentile_5y.toFixed(1)}%`;
            },
            afterLabel(context: { raw?: ScatterPointPayload }) {
              const point = context.raw;
              if (!point) {
                return [];
              }

              const lines = [
                point.level === "L2" && point.parent_l1_name
                  ? `所属一级：${point.parent_l1_name}`
                  : `层级：申万${point.level === "L1" ? "一级" : "二级"}行业`,
                `当前PB：${point.latest_pb.toFixed(2)} (${point.latest_pb_date})`,
                `当前ROE：${point.latest_roe.toFixed(2)} (${point.latest_roe_period})`,
              ];
              if (point.is_consumer_related) {
                lines.push("消费相关行业");
              }
              return lines;
            },
          },
        },
      },
    },
  } as any);
}

export function initMarketScatterCharts() {
  document.querySelectorAll("[data-market-scatter-chart]").forEach((root) => {
    if (!(root instanceof HTMLElement) || root.dataset.scatterBound === "true") {
      return;
    }

    const canvasId = root.dataset.canvasId;
    const payloadId = root.dataset.payloadId;
    if (!canvasId || !payloadId) {
      return;
    }

    const canvas = document.getElementById(canvasId);
    const payloadNode = document.getElementById(payloadId);
    if (!(canvas instanceof HTMLCanvasElement) || !payloadNode?.textContent) {
      return;
    }

    const payload = JSON.parse(payloadNode.textContent) as ScatterChartPayload;
    if (!payload || !Array.isArray(payload.points)) {
      return;
    }

    root.dataset.scatterBound = "true";
    renderMarketScatterChart(canvas, payload);
  });
}
