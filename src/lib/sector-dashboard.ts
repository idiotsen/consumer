import { loadPublicJson } from "@/lib/data";
import { createEmptySectorMarketPanels, createEmptySectorMarketSeries } from "@/lib/fallbacks";
import type {
  LoadedPayload,
  SectorBoard,
  SectorMarketPanelsPayload,
  SectorMarketSeriesPayload,
  SectorSeriesItem,
} from "@/lib/types";

export interface SectorDashboardData {
  panelsLoad: LoadedPayload<SectorMarketPanelsPayload>;
  seriesLoad: LoadedPayload<SectorMarketSeriesPayload>;
  boards: SectorBoard[];
  seriesByBoard: Map<string, SectorSeriesItem[]>;
}

export async function loadSectorDashboardData(): Promise<SectorDashboardData> {
  const panelsLoad = await loadPublicJson("sector-market-panels.json", createEmptySectorMarketPanels());
  const seriesLoad = await loadPublicJson("sector-market-series.json", createEmptySectorMarketSeries());
  const seriesByBoard = new Map<string, SectorSeriesItem[]>();

  for (const item of seriesLoad.data.series) {
    const existing = seriesByBoard.get(item.board_id) ?? [];
    existing.push(item);
    seriesByBoard.set(item.board_id, existing);
  }

  return {
    panelsLoad,
    seriesLoad,
    boards: panelsLoad.data.boards,
    seriesByBoard,
  };
}

export function findBoardById(boards: SectorBoard[], boardId: string): SectorBoard | undefined {
  return boards.find((board) => board.board_id === boardId);
}
