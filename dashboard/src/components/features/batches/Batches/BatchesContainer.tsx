import { useState, useCallback, useRef } from "react";
import { Batches } from "./Batches";
import { UploadFileModal } from "../../../modals/CreateFileModal";
import { CreateBatchModal } from "../../../modals/CreateBatchModal";
import { DownloadFileModal } from "../../../modals/DownloadFileModal";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogFooter,
} from "../../../ui/dialog";
import { Button } from "../../../ui/button";
import { Trash2, Loader2 } from "lucide-react";
import {
  useDeleteFile,
  useCancelBatch,
  useDeleteBatch,
} from "../../../../api/control-layer/hooks";
import { toast } from "sonner";
import type { FileObject, Batch } from "../types";

export function BatchesContainer() {
  const [uploadModalOpen, setUploadModalOpen] = useState(false);
  const [createBatchModalOpen, setCreateBatchModalOpen] = useState(false);
  const [downloadFileModalOpen, setDownloadFileModalOpen] = useState(false);

  const [downloadResource, setDownloadResource] = useState<{
    type: "file" | "batch-results";
    id: string;
    filename?: string;
    isPartial?: boolean;
  } | null>(null);
  const [preselectedFile, setPreselectedFile] = useState<
    FileObject | undefined
  >();
  const [preselectedFileToUpload, setPreselectedFileToUpload] = useState<
    File | undefined
  >();

  const [fileToDelete, setFileToDelete] = useState<FileObject | null>(null);
  const [batchToCancel, setBatchToCancel] = useState<Batch | null>(null);
  const [batchToDelete, setBatchToDelete] = useState<Batch | null>(null);

  const [droppedFile, setDroppedFile] = useState<File | undefined>();

  const batchCreatedCallbackRef = useRef<(() => void) | undefined>(undefined);

  const deleteMutation = useDeleteFile();
  const cancelMutation = useCancelBatch();
  const deleteBatchMutation = useDeleteBatch();

  const registerBatchCreatedCallback = useCallback((callback: () => void) => {
    batchCreatedCallbackRef.current = callback;
  }, []);

  const handleOpenUploadModal = (file?: File) => {
    setDroppedFile(file);
    setUploadModalOpen(true);
  };

  const handleOpenCreateBatchModal = (file?: File | FileObject) => {
    if (file) {
      if (file instanceof File) {
        setPreselectedFileToUpload(file);
      } else {
        setPreselectedFile(file);
      }
    }
    setCreateBatchModalOpen(true);
  };

  const handleOpenDownloadModal = (resource: {
    type: "file" | "batch-results";
    id: string;
    filename?: string;
    isPartial?: boolean;
  }) => {
    setDownloadResource(resource);
    setDownloadFileModalOpen(true);
  };

  const handleOpenDeleteDialog = (file: FileObject) => {
    setFileToDelete(file);
  };

  const handleOpenCancelDialog = (batch: Batch) => {
    setBatchToCancel(batch);
  };

  const handleOpenDeleteBatchDialog = (batch: Batch) => {
    setBatchToDelete(batch);
  };

  const confirmDeleteFile = async () => {
    if (!fileToDelete) return;

    try {
      await deleteMutation.mutateAsync(fileToDelete.id);
      toast.success(`File "${fileToDelete.filename}" deleted successfully`);
      setFileToDelete(null);
    } catch (error) {
      console.error("Failed to delete file:", error);
      toast.error(
        error instanceof Error
          ? error.message
          : "Failed to delete file. Please try again.",
      );
    }
  };

  const confirmCancelBatch = async () => {
    if (!batchToCancel) return;

    try {
      await cancelMutation.mutateAsync(batchToCancel.id);
      toast.success(`Batch "${batchToCancel.id}" is being cancelled`);
      setBatchToCancel(null);
    } catch (error) {
      console.error("Failed to cancel batch:", error);
      toast.error(
        error instanceof Error
          ? error.message
          : "Failed to cancel batch. Please try again.",
      );
    }
  };

  const confirmDeleteBatch = async () => {
    if (!batchToDelete) return;

    try {
      await deleteBatchMutation.mutateAsync(batchToDelete.id);
      toast.success(`Batch "${batchToDelete.id}" deleted successfully`);
      setBatchToDelete(null);
    } catch (error) {
      console.error("Failed to delete batch:", error);
      toast.error(
        error instanceof Error
          ? error.message
          : "Failed to delete batch. Please try again.",
      );
    }
  };

  return (
    <>
      { }
      <Batches
        onOpenUploadModal={handleOpenUploadModal}
        onOpenCreateBatchModal={handleOpenCreateBatchModal}
        onOpenDownloadModal={handleOpenDownloadModal}
        onOpenDeleteDialog={handleOpenDeleteDialog}
        onOpenDeleteBatchDialog={handleOpenDeleteBatchDialog}
        onOpenCancelDialog={handleOpenCancelDialog}
        onBatchCreatedCallback={registerBatchCreatedCallback}
      />

      { }
      <UploadFileModal
        isOpen={uploadModalOpen}
        onClose={() => {
          setUploadModalOpen(false);
          setDroppedFile(undefined);
        }}
        onSuccess={() => {
          setUploadModalOpen(false);
          setDroppedFile(undefined);
        }}
        preselectedFile={droppedFile}
      />

      <CreateBatchModal
        isOpen={createBatchModalOpen}
        onClose={() => {
          setCreateBatchModalOpen(false);
          setPreselectedFile(undefined);
        }}
        onSuccess={() => {
          setCreateBatchModalOpen(false);
          setPreselectedFile(undefined);
          if (batchCreatedCallbackRef.current) {
            batchCreatedCallbackRef.current();
          }
        }}
        preselectedFile={preselectedFile}
        preselectedFileToUpload={preselectedFileToUpload}
      />

      <DownloadFileModal
        isOpen={downloadFileModalOpen}
        onClose={() => {
          setDownloadFileModalOpen(false);
          setDownloadResource(null);
        }}
        title={
          downloadResource?.type === "file"
            ? "Download File"
            : "Download Batch Results"
        }
        description={
          downloadResource?.type === "file"
            ? "Use the code below to download this file via the API"
            : "Use the code below to download batch results via the API"
        }
        resourceType={downloadResource?.type || "file"}
        resourceId={downloadResource?.id || ""}
        filename={downloadResource?.filename}
        isPartial={downloadResource?.isPartial}
      />

      { }
      <Dialog open={!!fileToDelete} onOpenChange={() => setFileToDelete(null)}>
        <DialogContent className="sm:max-w-md">
          <DialogHeader>
            <div className="flex items-center gap-3">
              <div className="w-10 h-10 bg-red-100 rounded-full flex items-center justify-center">
                <Trash2 className="w-5 h-5 text-red-600" />
              </div>
              <div>
                <DialogTitle>Delete File</DialogTitle>
                <p className="text-sm text-gray-600">
                  This action cannot be undone
                </p>
              </div>
            </div>
          </DialogHeader>

          <div className="py-4">
            <p className="text-sm text-gray-700">
              Are you sure you want to delete the file{" "}
              <strong>"{fileToDelete?.filename}"</strong>? This action cannot be
              undone.
            </p>
          </div>

          <DialogFooter>
            <Button
              type="button"
              variant="outline"
              onClick={() => setFileToDelete(null)}
            >
              Cancel
            </Button>
            <Button
              onClick={confirmDeleteFile}
              disabled={deleteMutation.isPending}
              variant="destructive"
            >
              {deleteMutation.isPending && (
                <Loader2 className="w-4 h-4 mr-2 animate-spin" />
              )}
              Delete File
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      { }
      <Dialog
        open={!!batchToCancel}
        onOpenChange={() => setBatchToCancel(null)}
      >
        <DialogContent className="sm:max-w-md">
          <DialogHeader>
            <div className="flex items-center gap-3">
              <div className="w-10 h-10 bg-red-100 rounded-full flex items-center justify-center">
                <Trash2 className="w-5 h-5 text-red-600" />
              </div>
              <div>
                <DialogTitle>Cancel Batch</DialogTitle>
                <p className="text-sm text-gray-600">
                  This will stop processing
                </p>
              </div>
            </div>
          </DialogHeader>

          <div className="py-4">
            <p className="text-sm text-gray-700">
              Are you sure you want to cancel batch{" "}
              <strong className="font-mono">"{batchToCancel?.id}"</strong>? This
              will stop processing and may result in partial results.
            </p>
          </div>

          <DialogFooter>
            <Button
              type="button"
              variant="outline"
              onClick={() => setBatchToCancel(null)}
            >
              Keep Running
            </Button>
            <Button
              onClick={confirmCancelBatch}
              disabled={cancelMutation.isPending}
              variant="destructive"
            >
              {cancelMutation.isPending && (
                <Loader2 className="w-4 h-4 mr-2 animate-spin" />
              )}
              Cancel Batch
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      { }
      <Dialog
        open={!!batchToDelete}
        onOpenChange={() => setBatchToDelete(null)}
      >
        <DialogContent className="sm:max-w-md">
          <DialogHeader>
            <div className="flex items-center gap-3">
              <div className="w-10 h-10 bg-red-100 rounded-full flex items-center justify-center">
                <Trash2 className="w-5 h-5 text-red-600" />
              </div>
              <div>
                <DialogTitle>Delete Batch</DialogTitle>
                <p className="text-sm text-gray-600">
                  This action cannot be undone
                </p>
              </div>
            </div>
          </DialogHeader>

          <div className="py-4">
            <p className="text-sm text-gray-700">
              Are you sure you want to delete batch{" "}
              <strong className="font-mono">"{batchToDelete?.id}"</strong>? This
              will permanently delete the batch and all its associated requests.
            </p>
          </div>

          <DialogFooter>
            <Button
              type="button"
              variant="outline"
              onClick={() => setBatchToDelete(null)}
            >
              Cancel
            </Button>
            <Button
              onClick={confirmDeleteBatch}
              disabled={deleteBatchMutation.isPending}
              variant="destructive"
            >
              {deleteBatchMutation.isPending && (
                <Loader2 className="w-4 h-4 mr-2 animate-spin" />
              )}
              Delete Batch
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </>
  );
}
