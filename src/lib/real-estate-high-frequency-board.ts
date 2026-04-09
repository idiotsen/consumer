import { loadPublicJson } from "@/lib/data";
import { createEmptyRealEstateHighFrequencyBoard } from "@/lib/fallbacks";
import type { LoadedPayload, RealEstateHighFrequencyBoardPayload } from "@/lib/types";

export interface RealEstateHighFrequencyBoardData {
  highFrequencyLoad: LoadedPayload<RealEstateHighFrequencyBoardPayload>;
  board: RealEstateHighFrequencyBoardPayload;
}

export async function loadRealEstateHighFrequencyBoardData(): Promise<RealEstateHighFrequencyBoardData> {
  const highFrequencyLoad = await loadPublicJson(
    "real-estate-high-frequency-board.json",
    createEmptyRealEstateHighFrequencyBoard(),
  );

  return {
    highFrequencyLoad,
    board: highFrequencyLoad.data,
  };
}
