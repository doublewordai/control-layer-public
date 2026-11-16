
const STORAGE_KEY = "demo-mode-state";

export interface StoredComponent {
  componentModelId: string;
  weight: number;
  enabled: boolean;
  sort_order: number;
}

interface DemoState {
  modelsGroups: Record<string, string[]>;
  userGroups: Record<string, string[]>;
  currentUserRoles?: string[];
  modelComponents?: Record<string, StoredComponent[]>;
}

export function loadDemoState(
  initialModelsGroups: Record<string, string[]>,
  initialUserGroups: Record<string, string[]>,
): DemoState {
  const stored = localStorage.getItem(STORAGE_KEY);

  if (stored) {
    try {
      return JSON.parse(stored);
    } catch {
      console.warn("Failed to parse demo state, using initial data");
    }
  }

  return {
    modelsGroups: { ...initialModelsGroups },
    userGroups: { ...initialUserGroups },
  };
}

export function saveDemoState(state: DemoState): void {
  localStorage.setItem(STORAGE_KEY, JSON.stringify(state));
}

export function resetDemoState(): void {
  localStorage.removeItem(STORAGE_KEY);
}

export function addModelToGroup(
  state: DemoState,
  modelId: string,
  groupId: string,
): DemoState {
  const newState = {
    ...state,
    modelsGroups: { ...state.modelsGroups },
  };

  if (!newState.modelsGroups[modelId]) {
    newState.modelsGroups[modelId] = [];
  }

  if (!newState.modelsGroups[modelId].includes(groupId)) {
    newState.modelsGroups[modelId] = [
      ...newState.modelsGroups[modelId],
      groupId,
    ];
  }

  saveDemoState(newState);
  return newState;
}

export function removeModelFromGroup(
  state: DemoState,
  modelId: string,
  groupId: string,
): DemoState {
  const newState = {
    ...state,
    modelsGroups: { ...state.modelsGroups },
  };

  if (newState.modelsGroups[modelId]) {
    newState.modelsGroups[modelId] = newState.modelsGroups[modelId].filter(
      (id) => id !== groupId,
    );
  }

  saveDemoState(newState);
  return newState;
}

export function setCurrentUserRoles(
  state: DemoState,
  roles: string[],
): DemoState {
  const newState = {
    ...state,
    currentUserRoles: roles,
  };
  saveDemoState(newState);
  return newState;
}

export function getCurrentUserRoles(state: DemoState): string[] | undefined {
  return state.currentUserRoles;
}

export function addUserToGroup(
  state: DemoState,
  userId: string,
  groupId: string,
): DemoState {
  const newState = {
    ...state,
    userGroups: { ...state.userGroups },
  };

  if (!newState.userGroups[userId]) {
    newState.userGroups[userId] = [];
  }

  if (!newState.userGroups[userId].includes(groupId)) {
    newState.userGroups[userId] = [...newState.userGroups[userId], groupId];
  }

  saveDemoState(newState);
  return newState;
}

export function removeUserFromGroup(
  state: DemoState,
  userId: string,
  groupId: string,
): DemoState {
  const newState = {
    ...state,
    userGroups: { ...state.userGroups },
  };

  if (newState.userGroups[userId]) {
    newState.userGroups[userId] = newState.userGroups[userId].filter(
      (id) => id !== groupId,
    );
  }

  saveDemoState(newState);
  return newState;
}

export function getModelComponents(
  state: DemoState,
  modelId: string,
  initialComponents: Record<string, StoredComponent[]>,
): StoredComponent[] {
  const components = state.modelComponents ?? initialComponents;
  return components[modelId] ?? [];
}

export function addModelComponent(
  state: DemoState,
  modelId: string,
  component: StoredComponent,
  initialComponents: Record<string, StoredComponent[]>,
): DemoState {
  const currentComponents = state.modelComponents ?? { ...initialComponents };
  const existing = currentComponents[modelId] ?? [];

  if (existing.some((c) => c.componentModelId === component.componentModelId)) {
    return state;
  }

  const newState = {
    ...state,
    modelComponents: {
      ...currentComponents,
      [modelId]: [...existing, component],
    },
  };
  saveDemoState(newState);
  return newState;
}

export function updateModelComponent(
  state: DemoState,
  modelId: string,
  componentModelId: string,
  updates: Partial<Pick<StoredComponent, "weight" | "enabled" | "sort_order">>,
  initialComponents: Record<string, StoredComponent[]>,
): DemoState {
  const currentComponents = state.modelComponents ?? { ...initialComponents };
  const existing = currentComponents[modelId] ?? [];

  const newState = {
    ...state,
    modelComponents: {
      ...currentComponents,
      [modelId]: existing.map((c) =>
        c.componentModelId === componentModelId ? { ...c, ...updates } : c,
      ),
    },
  };
  saveDemoState(newState);
  return newState;
}

export function removeModelComponent(
  state: DemoState,
  modelId: string,
  componentModelId: string,
  initialComponents: Record<string, StoredComponent[]>,
): DemoState {
  const currentComponents = state.modelComponents ?? { ...initialComponents };
  const existing = currentComponents[modelId] ?? [];

  const newState = {
    ...state,
    modelComponents: {
      ...currentComponents,
      [modelId]: existing.filter(
        (c) => c.componentModelId !== componentModelId,
      ),
    },
  };
  saveDemoState(newState);
  return newState;
}
