import { loadPublicJson } from "@/lib/data";
import { createEmptyMarketTrackingBoard } from "@/lib/fallbacks";
import type { LoadedPayload, MarketTrackingBoardPayload } from "@/lib/types";

export interface MarketTrackingBoardData {
  marketLoad: LoadedPayload<MarketTrackingBoardPayload>;
  board: MarketTrackingBoardPayload;
}

export async function loadMarketTrackingBoardData(): Promise<MarketTrackingBoardData> {
  const marketLoad = await loadPublicJson("market-tracking-board.json", createEmptyMarketTrackingBoard());
  return {
    marketLoad,
    board: marketLoad.data,
  };
}
