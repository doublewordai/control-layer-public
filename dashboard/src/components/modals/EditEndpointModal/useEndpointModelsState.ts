import { useCallback, useMemo, useState } from "react";

export interface ImportedDeployment {
  modelName: string;
  alias: string;
  isNew: boolean;
}

export interface EndpointModelsState {
  deployments: ImportedDeployment[];
  removedModelNames: string[];
  addedModelNames: string[];
  addedDeployments: ImportedDeployment[];
  aliasEdits: Record<string, string>;
  changeCount: number;
  hasChanges: boolean;

  addModel: (modelName: string, defaultAlias?: string) => void;
  removeModel: (modelName: string) => () => void;
  setAlias: (modelName: string, alias: string) => void;
  reset: () => void;
}

interface InitialDeployment {
  modelName: string;
  alias: string;
}

interface StagedState {
  aliases: Record<string, string>;
  added: ImportedDeployment[];
  removed: Set<string>;
}

const EMPTY_STAGED: StagedState = {
  aliases: {},
  added: [],
  removed: new Set(),
};

export function useEndpointModelsState(
  initial: InitialDeployment[],
): EndpointModelsState {
  const [staged, setStaged] = useState<StagedState>(EMPTY_STAGED);

  const serverByName = useMemo(() => {
    const m = new Map<string, InitialDeployment>();
    for (const d of initial) m.set(d.modelName, d);
    return m;
  }, [initial]);

  const addModel = useCallback(
    (modelName: string, defaultAlias?: string) => {
      setStaged((prev) => {
        if (prev.removed.has(modelName)) {
          const next = new Set(prev.removed);
          next.delete(modelName);
          return { ...prev, removed: next };
        }
        if (serverByName.has(modelName)) return prev;
        if (prev.added.some((d) => d.modelName === modelName)) return prev;
        return {
          ...prev,
          added: [
            ...prev.added,
            { modelName, alias: defaultAlias ?? modelName, isNew: true },
          ],
        };
      });
    },
    [serverByName],
  );

  const removeModel = useCallback(
    (modelName: string): (() => void) => {
      let capturedAdded: ImportedDeployment | null = null;
      let capturedAliasEdit: string | undefined = undefined;

      setStaged((prev) => {
        const inAdded = prev.added.find((d) => d.modelName === modelName);
        if (inAdded) {
          capturedAdded = { ...inAdded };
          return {
            ...prev,
            added: prev.added.filter((d) => d.modelName !== modelName),
          };
        }
        capturedAliasEdit = prev.aliases[modelName];
        const nextRemoved = new Set(prev.removed);
        nextRemoved.add(modelName);
        const nextAliases = { ...prev.aliases };
        delete nextAliases[modelName];
        return { ...prev, removed: nextRemoved, aliases: nextAliases };
      });

      return () => {
        setStaged((prev) => {
          if (capturedAdded) {
            if (prev.added.some((d) => d.modelName === modelName)) return prev;
            return { ...prev, added: [...prev.added, capturedAdded!] };
          }
          if (!prev.removed.has(modelName)) return prev;
          const next = new Set(prev.removed);
          next.delete(modelName);
          const aliases =
            capturedAliasEdit !== undefined
              ? { ...prev.aliases, [modelName]: capturedAliasEdit }
              : prev.aliases;
          return { ...prev, removed: next, aliases };
        });
      };
    },
    [],
  );

  const setAlias = useCallback(
    (modelName: string, alias: string) => {
      setStaged((prev) => {
        if (prev.added.some((d) => d.modelName === modelName)) {
          return {
            ...prev,
            added: prev.added.map((d) =>
              d.modelName === modelName ? { ...d, alias } : d,
            ),
          };
        }
        const original = serverByName.get(modelName)?.alias;
        if (original !== undefined && alias === original) {
          const next = { ...prev.aliases };
          delete next[modelName];
          return { ...prev, aliases: next };
        }
        return { ...prev, aliases: { ...prev.aliases, [modelName]: alias } };
      });
    },
    [serverByName],
  );

  const reset = useCallback(() => {
    setStaged(EMPTY_STAGED);
  }, []);

  const deployments = useMemo<ImportedDeployment[]>(() => {
    const surviving: ImportedDeployment[] = [];
    for (const d of initial) {
      if (staged.removed.has(d.modelName)) continue;
      surviving.push({
        modelName: d.modelName,
        alias: staged.aliases[d.modelName] ?? d.alias,
        isNew: false,
      });
    }
    const safeAdded = staged.added.filter((d) => !serverByName.has(d.modelName));
    return [...surviving, ...safeAdded];
  }, [initial, staged, serverByName]);

  const removedModelNames = useMemo(
    () => Array.from(staged.removed),
    [staged.removed],
  );

  const addedModelNames = useMemo(
    () => staged.added.map((d) => d.modelName),
    [staged.added],
  );

  const aliasEditsCount = useMemo(() => {
    let n = 0;
    for (const name of Object.keys(staged.aliases)) {
      if (staged.removed.has(name)) continue;
      n++;
    }
    return n;
  }, [staged.aliases, staged.removed]);

  const changeCount =
    addedModelNames.length + removedModelNames.length + aliasEditsCount;
  const hasChanges = changeCount > 0;

  return {
    deployments,
    removedModelNames,
    addedModelNames,
    addedDeployments: staged.added,
    aliasEdits: staged.aliases,
    changeCount,
    hasChanges,
    addModel,
    removeModel,
    setAlias,
    reset,
  };
}
