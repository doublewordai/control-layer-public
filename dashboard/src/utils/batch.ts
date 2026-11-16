
export type BatchFileType = "input" | "output" | "error";

export function getBatchDownloadFilename(
  batchId: string,
  fileType: BatchFileType,
): string {
  return `batch_${batchId}_${fileType}.jsonl`;
}

export async function downloadFile(
  endpoint: string,
  filename: string,
): Promise<void> {
  const token = localStorage.getItem("auth_token");
  const response = await fetch(endpoint, {
    credentials: "same-origin",
    headers: token ? { Authorization: `Bearer ${token}` } : {},
  });

  if (!response.ok) {
    throw new Error(`Download failed: ${response.status}`);
  }

  const blob = await response.blob();
  const url = window.URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  window.URL.revokeObjectURL(url);
  document.body.removeChild(a);
}
