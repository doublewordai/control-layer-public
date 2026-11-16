import type {
  FileObject,
  Batch,
  BatchRequest,
  FileRequest,
  BatchStatus,
  BatchAnalytics,
} from "../../../api/control-layer/types";

export type {
  FileObject,
  Batch,
  BatchRequest,
  FileRequest,
  BatchStatus,
  BatchAnalytics,
};

export interface BatchWithProgress extends Batch {
  progress: number;
  estimatedTimeRemaining?: number;
}
