import type { JobMode } from "./api";

export type PendingTurn = {
  id: string;
  mode: JobMode;
  text: string;
  sourceJobId?: string;
};

export function shouldShowRunLoader({
  detailOpen,
  hasResult,
  hasStatus,
  submitting,
  hasPendingTurn,
}: {
  detailOpen: boolean;
  hasResult: boolean;
  hasStatus: boolean;
  submitting: boolean;
  hasPendingTurn: boolean;
}): boolean {
  if (detailOpen) {
    return false;
  }
  if (hasPendingTurn) {
    return true;
  }
  return !hasResult && (hasStatus || submitting);
}

export function buildPendingLoaderHeadline(mode: JobMode, fallback: string): string {
  if (mode === "document") {
    return "문서 초안을 준비하는 중";
  }
  if (mode === "question") {
    return "질문을 보내는 중";
  }
  return fallback;
}

export function buildPendingLoaderState(statusState: string | undefined, hasPendingTurn: boolean): string {
  const state = String(statusState || "").trim();
  if (state) {
    return state;
  }
  return hasPendingTurn ? "generating_keywords" : "";
}

export function shouldShowDocumentConfigurator({
  mode,
  documentThreadStarted,
}: {
  mode: JobMode;
  documentThreadStarted: boolean;
}): boolean {
  void mode;
  void documentThreadStarted;
  return false;
}
