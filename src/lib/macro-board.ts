import { loadPublicJson } from "@/lib/data";
import { createEmptyMacroBoard } from "@/lib/fallbacks";
import type { LoadedPayload, MacroBoardPayload } from "@/lib/types";

export interface MacroBoardData {
  macroLoad: LoadedPayload<MacroBoardPayload>;
  board: MacroBoardPayload;
}

export async function loadMacroBoardData(): Promise<MacroBoardData> {
  const macroLoad = await loadPublicJson("macro-board.json", createEmptyMacroBoard());
  return {
    macroLoad,
    board: macroLoad.data,
  };
}
