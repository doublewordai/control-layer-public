import type {
  User as BackendUser,
  Group as BackendGroup,
} from "../api/control-layer";

export type DisplayUser = BackendUser & {
  name: string;
  avatar: string;
  isAdmin: boolean;
  groupNames: string[];
};

export type DisplayGroup = BackendGroup & {
  memberCount: number;
  memberIds: string[];
};
