import type { JobResultPayload, JobStatusPayload } from "./api";

export type SessionHistoryItem = {
  id: string;
  title: string;
  meta: string;
  prompt?: string;
  createdAt?: number;
  finishedAt?: number | null;
};

export type SessionSnapshot = {
  jobId: string;
  prompt?: string;
  mode?: "question" | "document";
  selectedRecordId?: string;
  samplePath?: string;
  documentPresetId?: string;
  useCustomSamplePath?: boolean;
  questionMessages?: Array<{ role: "user"; text: string }>;
  documentMessages?: Array<{ role: "user" | "assistant"; text: string }>;
};

export type DraftState = {
  mode?: "question" | "document";
  prompt?: string;
  submittedPrompt?: string;
  samplePath?: string;
  uploadedSampleLabel?: string;
  documentPresetId?: string;
  useCustomSamplePath?: boolean;
  questionMessages?: Array<{ role: "user"; text: string }>;
  documentMessages?: Array<{ role: "user" | "assistant"; text: string }>;
};

const TERMINAL_METAS = new Set(["중단됨", "실패"]);
const TERMINAL_JOB_STATES = new Set(["failed", "cancelled", "interrupted"]);
const SNAPSHOT_STORAGE_KEY = "memory-runtime-session-snapshots-v1";
export const VIEWED_SESSION_STORAGE_KEY = "memory-runtime-viewed-session-v1";
const STATUS_SNAPSHOT_STORAGE_KEY = "memory-runtime-status-snapshots-v1";
const RESULT_SNAPSHOT_STORAGE_KEY = "memory-runtime-result-snapshots-v1";
const BROWSER_CLIENT_ID_STORAGE_KEY = "memory-runtime-browser-client-id-v1";
const BROWSER_DB_NAME = "memory-runtime-browser-store-v1";
const BROWSER_DB_VERSION = 1;
const STATUS_STORE = "statusSnapshots";
const RESULT_STORE = "resultSnapshots";

export function upsertSessionHistory(
  current: SessionHistoryItem[],
  next: SessionHistoryItem,
  limit = 8,
): SessionHistoryItem[] {
  const existing = current.find((item) => item.id === next.id);
  const merged = existing ? { ...existing, ...next } : next;
  const rest = current.filter((item) => item.id !== next.id);
  const updated = [merged, ...rest];
  return updated.slice(0, limit);
}

export function findSessionPrompt(history: SessionHistoryItem[], jobId: string): string {
  const match = history.find((item) => item.id === jobId);
  if (!match) {
    return "";
  }
  return (match.prompt || match.title || "").trim();
}

export function filterVisibleHistory(history: SessionHistoryItem[]): SessionHistoryItem[] {
  return history.filter((item) => !TERMINAL_METAS.has((item.meta || "").trim()));
}

export function findLatestVisibleSessionId(history: SessionHistoryItem[]): string {
  return filterVisibleHistory(history)[0]?.id ?? "";
}

export function isRestorableJobState(state: string): boolean {
  return !TERMINAL_JOB_STATES.has((state || "").trim());
}

export function shouldPersistLastSession(activeJobId: string, hydrated: boolean): boolean {
  return hydrated && !!activeJobId.trim();
}

export function loadSessionSnapshots(): Record<string, SessionSnapshot> {
  if (typeof window === "undefined") {
    return {};
  }
  try {
    const raw = window.localStorage.getItem(SNAPSHOT_STORAGE_KEY);
    if (!raw) {
      return {};
    }
    const parsed = JSON.parse(raw) as Record<string, SessionSnapshot>;
    return parsed && typeof parsed === "object" ? parsed : {};
  } catch {
    return {};
  }
}

export function saveSessionSnapshot(snapshot: SessionSnapshot): void {
  if (typeof window === "undefined" || !snapshot.jobId.trim()) {
    return;
  }
  try {
    const current = loadSessionSnapshots();
    current[snapshot.jobId] = snapshot;
    window.localStorage.setItem(SNAPSHOT_STORAGE_KEY, JSON.stringify(current));
  } catch {
    // Ignore local storage quota issues.
  }
}

export function loadSessionSnapshot(jobId: string): SessionSnapshot | null {
  if (!jobId.trim()) {
    return null;
  }
  const current = loadSessionSnapshots();
  return current[jobId] ?? null;
}

export function clearSessionSnapshot(jobId: string): void {
  if (typeof window === "undefined" || !jobId.trim()) {
    return;
  }
  try {
    const current = loadSessionSnapshots();
    delete current[jobId];
    window.localStorage.setItem(SNAPSHOT_STORAGE_KEY, JSON.stringify(current));
  } catch {
    // Ignore local storage quota issues.
  }
}

export function normalizeDraftState(raw: unknown): DraftState {
  if (!raw || typeof raw !== "object") {
    return {};
  }
  const row = raw as Record<string, unknown>;
  const normalizedQuestionMessages: Array<{ role: "user"; text: string }> = [];
  if (Array.isArray(row.questionMessages)) {
    for (const item of row.questionMessages) {
      if (!item || typeof item !== "object") {
        continue;
      }
      const payload = item as Record<string, unknown>;
      const role = String(payload.role || "").trim();
      const text = String(payload.text || "").trim();
      if (role === "user" && text) {
        normalizedQuestionMessages.push({ role, text });
      }
    }
  }
  const normalizedDocumentMessages: Array<{ role: "user" | "assistant"; text: string }> = [];
  if (Array.isArray(row.documentMessages)) {
    for (const item of row.documentMessages) {
      if (!item || typeof item !== "object") {
        continue;
      }
      const payload = item as Record<string, unknown>;
      const role = String(payload.role || "").trim();
      const text = String(payload.text || "").trim();
      if ((role === "user" || role === "assistant") && text) {
        normalizedDocumentMessages.push({ role, text });
      }
    }
  }
  const mode = String(row.mode || "").trim();
  return {
    mode: mode === "document" ? "document" : mode === "question" ? "question" : undefined,
    prompt: String(row.prompt || ""),
    submittedPrompt: String(row.submittedPrompt || ""),
    samplePath: String(row.samplePath || ""),
    uploadedSampleLabel: String(row.uploadedSampleLabel || ""),
    documentPresetId: String(row.documentPresetId || ""),
    useCustomSamplePath: typeof row.useCustomSamplePath === "boolean" ? row.useCustomSamplePath : undefined,
    questionMessages: normalizedQuestionMessages,
    documentMessages: normalizedDocumentMessages,
  };
}

export function loadViewedSessionId(): string {
  if (typeof window === "undefined") {
    return "";
  }
  try {
    return (window.sessionStorage?.getItem(VIEWED_SESSION_STORAGE_KEY) || window.localStorage?.getItem(VIEWED_SESSION_STORAGE_KEY) || "").trim();
  } catch {
    return "";
  }
}

export function saveViewedSessionId(jobId: string): void {
  if (typeof window === "undefined") {
    return;
  }
  try {
    if (jobId.trim()) {
      window.sessionStorage?.setItem(VIEWED_SESSION_STORAGE_KEY, jobId.trim());
      window.localStorage?.removeItem(VIEWED_SESSION_STORAGE_KEY);
    } else {
      window.sessionStorage?.removeItem(VIEWED_SESSION_STORAGE_KEY);
      window.localStorage?.removeItem(VIEWED_SESSION_STORAGE_KEY);
    }
  } catch {
    // Ignore local storage quota issues.
  }
}

export function getBrowserClientId(): string {
  if (typeof window === "undefined") {
    return "";
  }
  try {
    const existing = window.localStorage?.getItem(BROWSER_CLIENT_ID_STORAGE_KEY)?.trim();
    if (existing) {
      return existing;
    }
    const cryptoLike = window.crypto as Crypto | undefined;
    const randomPart =
      typeof cryptoLike?.randomUUID === "function"
        ? cryptoLike.randomUUID()
        : `${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`;
    const next = `memory-runtime-${randomPart}`;
    window.localStorage?.setItem(BROWSER_CLIENT_ID_STORAGE_KEY, next);
    return next;
  } catch {
    return `memory-runtime-ephemeral-${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`;
  }
}

function loadStoredMap<T>(key: string): Record<string, T> {
  if (typeof window === "undefined") {
    return {};
  }
  try {
    const raw = window.localStorage.getItem(key);
    if (!raw) {
      return {};
    }
    const parsed = JSON.parse(raw) as Record<string, T>;
    return parsed && typeof parsed === "object" ? parsed : {};
  } catch {
    return {};
  }
}

function saveStoredMap<T>(key: string, value: Record<string, T>): void {
  if (typeof window === "undefined") {
    return;
  }
  try {
    window.localStorage.setItem(key, JSON.stringify(value));
  } catch {
    // Ignore local storage quota issues.
  }
}

let browserDbPromise: Promise<IDBDatabase | null> | null = null;

function openBrowserDb(): Promise<IDBDatabase | null> {
  if (typeof window === "undefined" || typeof window.indexedDB === "undefined") {
    return Promise.resolve(null);
  }
  if (browserDbPromise) {
    return browserDbPromise;
  }
  browserDbPromise = new Promise((resolve) => {
    try {
      const request = window.indexedDB.open(BROWSER_DB_NAME, BROWSER_DB_VERSION);
      request.onupgradeneeded = () => {
        const db = request.result;
        if (!db.objectStoreNames.contains(STATUS_STORE)) {
          db.createObjectStore(STATUS_STORE);
        }
        if (!db.objectStoreNames.contains(RESULT_STORE)) {
          db.createObjectStore(RESULT_STORE);
        }
      };
      request.onsuccess = () => resolve(request.result);
      request.onerror = () => resolve(null);
    } catch {
      resolve(null);
    }
  });
  return browserDbPromise;
}

async function browserStorePut<T>(storeName: string, key: string, value: T): Promise<void> {
  const db = await openBrowserDb();
  if (!db || !key.trim()) {
    return;
  }
  await new Promise<void>((resolve) => {
    try {
      const tx = db.transaction(storeName, "readwrite");
      tx.objectStore(storeName).put(value, key);
      tx.oncomplete = () => resolve();
      tx.onerror = () => resolve();
      tx.onabort = () => resolve();
    } catch {
      resolve();
    }
  });
}

async function browserStoreGet<T>(storeName: string, key: string): Promise<T | null> {
  const db = await openBrowserDb();
  if (!db || !key.trim()) {
    return null;
  }
  return new Promise<T | null>((resolve) => {
    try {
      const tx = db.transaction(storeName, "readonly");
      const request = tx.objectStore(storeName).get(key);
      request.onsuccess = () => resolve((request.result as T | undefined) ?? null);
      request.onerror = () => resolve(null);
      tx.onabort = () => resolve(null);
    } catch {
      resolve(null);
    }
  });
}

export function saveStatusSnapshot(jobId: string, status: JobStatusPayload): void {
  if (!jobId.trim()) {
    return;
  }
  const current = loadStoredMap<JobStatusPayload>(STATUS_SNAPSHOT_STORAGE_KEY);
  current[jobId] = status;
  saveStoredMap(STATUS_SNAPSHOT_STORAGE_KEY, current);
}

export async function saveStatusSnapshotAsync(jobId: string, status: JobStatusPayload): Promise<void> {
  saveStatusSnapshot(jobId, status);
  await browserStorePut(STATUS_STORE, jobId, status);
}

export function loadStatusSnapshot(jobId: string): JobStatusPayload | null {
  if (!jobId.trim()) {
    return null;
  }
  const current = loadStoredMap<JobStatusPayload>(STATUS_SNAPSHOT_STORAGE_KEY);
  return current[jobId] ?? null;
}

export async function loadStatusSnapshotAsync(jobId: string): Promise<JobStatusPayload | null> {
  const local = loadStatusSnapshot(jobId);
  if (local) {
    return local;
  }
  return browserStoreGet<JobStatusPayload>(STATUS_STORE, jobId);
}

function compactResult(result: JobResultPayload): JobResultPayload {
  return {
    ...result,
    selectedRecords: (result.selectedRecords || []).map((record) => ({
      ...record,
      fullText: "",
    })),
  };
}

export function saveResultSnapshot(jobId: string, result: JobResultPayload): void {
  if (!jobId.trim()) {
    return;
  }
  const current = loadStoredMap<JobResultPayload>(RESULT_SNAPSHOT_STORAGE_KEY);
  current[jobId] = compactResult(result);
  saveStoredMap(RESULT_SNAPSHOT_STORAGE_KEY, current);
}

export async function saveResultSnapshotAsync(jobId: string, result: JobResultPayload): Promise<void> {
  const compact = compactResult(result);
  const current = loadStoredMap<JobResultPayload>(RESULT_SNAPSHOT_STORAGE_KEY);
  current[jobId] = compact;
  saveStoredMap(RESULT_SNAPSHOT_STORAGE_KEY, current);
  await browserStorePut(RESULT_STORE, jobId, compact);
}

export function loadResultSnapshot(jobId: string): JobResultPayload | null {
  if (!jobId.trim()) {
    return null;
  }
  const current = loadStoredMap<JobResultPayload>(RESULT_SNAPSHOT_STORAGE_KEY);
  return current[jobId] ?? null;
}

export async function loadResultSnapshotAsync(jobId: string): Promise<JobResultPayload | null> {
  const local = loadResultSnapshot(jobId);
  if (local) {
    return local;
  }
  return browserStoreGet<JobResultPayload>(RESULT_STORE, jobId);
}
