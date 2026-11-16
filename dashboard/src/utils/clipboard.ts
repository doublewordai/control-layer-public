import { toast } from "sonner";

export async function copyToClipboard(
  text: string,
  options: { successMessage?: string; errorMessage?: string } = {},
): Promise<boolean> {
  const errorMessage = options.errorMessage ?? "Failed to copy to clipboard";

  if (typeof navigator === "undefined" || !navigator.clipboard) {
    toast.error(errorMessage);
    return false;
  }

  try {
    await navigator.clipboard.writeText(text);
    if (options.successMessage) {
      toast.success(options.successMessage);
    }
    return true;
  } catch (error) {
    console.error("Failed to copy to clipboard:", error);
    toast.error(errorMessage);
    return false;
  }
}
