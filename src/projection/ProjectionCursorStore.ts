export interface ProjectorCursor {
  lastSentVersion: number;
  failureCount: number;
  lastFailureAt: string | null;
  failedAtVersion: number | null;
}

const PREFIX = '__projection_cursor__';

function cursorKey(projectorId: string): string {
  return `${PREFIX}${projectorId}`;
}

export async function loadCursor(
  storage: DurableObjectStorage,
  projectorId: string
): Promise<ProjectorCursor> {
  const stored = await storage.get<ProjectorCursor>(cursorKey(projectorId));
  return stored ?? { lastSentVersion: 0, failureCount: 0, lastFailureAt: null, failedAtVersion: null };
}

export async function saveCursor(
  storage: DurableObjectStorage,
  projectorId: string,
  cursor: ProjectorCursor
): Promise<void> {
  await storage.put(cursorKey(projectorId), cursor);
}
