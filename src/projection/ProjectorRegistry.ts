import type { IEventProjector } from './IEventProjector';

const PROJECTORS: IEventProjector[] = [];

export function registerProjector(projector: IEventProjector): void {
  if (PROJECTORS.some((p) => p.id === projector.id)) {
    throw new Error(`Projector "${projector.id}" is already registered`);
  }
  PROJECTORS.push(projector);
}

export function getProjectors(): readonly IEventProjector[] {
  return PROJECTORS;
}

export function clearProjectors(): void {
  PROJECTORS.length = 0;
}
