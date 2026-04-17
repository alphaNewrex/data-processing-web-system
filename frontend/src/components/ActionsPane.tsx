import { useState, useRef, useCallback } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { uploadDataset } from "@/lib/api";
import { UploadIcon, FileJsonIcon, XIcon, CheckCircleIcon, AlertCircleIcon, LoaderIcon } from "lucide-react";

interface ActionsPaneProps {
  onUploadSuccess: () => void;
}

type FileEntry = {
  file: File;
  status: "pending" | "uploading" | "success" | "error";
  message?: string;
  progress: number;
  fading?: boolean;
};

export function ActionsPane({ onUploadSuccess }: ActionsPaneProps) {
  const [files, setFiles] = useState<FileEntry[]>([]);
  const [dragActive, setDragActive] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);

  const addFiles = useCallback((newFiles: FileList | File[]) => {
    const entries: FileEntry[] = Array.from(newFiles)
      .filter((f) => f.name.endsWith(".json"))
      .map((file) => ({ file, status: "pending" as const, progress: 0 }));

    if (entries.length === 0) return;
    setFiles((prev) => [...prev, ...entries]);

    // Auto-upload each file
    entries.forEach((entry) => {
      uploadFile(entry.file);
    });
  }, []);

  const uploadFile = async (file: File) => {
    // Set uploading
    setFiles((prev) =>
      prev.map((f) =>
        f.file === file ? { ...f, status: "uploading" as const, progress: 30 } : f
      )
    );

    // Simulate progress ticks
    const progressInterval = setInterval(() => {
      setFiles((prev) =>
        prev.map((f) =>
          f.file === file && f.status === "uploading"
            ? { ...f, progress: Math.min(f.progress + 10, 90) }
            : f
        )
      );
    }, 200);

    try {
      const res = await uploadDataset(file);
      clearInterval(progressInterval);
      setFiles((prev) =>
        prev.map((f) =>
          f.file === file
            ? { ...f, status: "success" as const, progress: 100, message: `Dataset "${res.dataset_id}" queued` }
            : f
        )
      );
      onUploadSuccess();
      // Fade out then remove after 2s
      setTimeout(() => {
        setFiles((prev) =>
          prev.map((f) => (f.file === file ? { ...f, fading: true } : f))
        );
        // Remove from DOM after fade animation completes
        setTimeout(() => {
          setFiles((prev) => prev.filter((f) => f.file !== file));
        }, 500);
      }, 1500);
    } catch (e) {
      clearInterval(progressInterval);
      setFiles((prev) =>
        prev.map((f) =>
          f.file === file
            ? { ...f, status: "error" as const, progress: 100, message: e instanceof Error ? e.message : "Upload failed" }
            : f
        )
      );
    }
  };

  const removeFile = (file: File) => {
    setFiles((prev) => prev.filter((f) => f.file !== file));
  };

  const retryFile = (file: File) => {
    setFiles((prev) =>
      prev.map((f) =>
        f.file === file ? { ...f, status: "pending" as const, progress: 0, message: undefined } : f
      )
    );
    uploadFile(file);
  };

  const handleDrop = useCallback(
    (e: React.DragEvent) => {
      e.preventDefault();
      setDragActive(false);
      if (e.dataTransfer.files.length) {
        addFiles(e.dataTransfer.files);
      }
    },
    [addFiles]
  );

  const handleDragOver = (e: React.DragEvent) => {
    e.preventDefault();
    setDragActive(true);
  };

  const handleDragLeave = (e: React.DragEvent) => {
    e.preventDefault();
    setDragActive(false);
  };

  return (
    <Card className="border-b rounded-none shadow-none">
      <CardHeader className="pb-2">
        <CardTitle className="text-lg">Upload Dataset</CardTitle>
      </CardHeader>
      <CardContent className="space-y-3">
        {/* Dropzone */}
        <div
          onDrop={handleDrop}
          onDragOver={handleDragOver}
          onDragLeave={handleDragLeave}
          onClick={() => inputRef.current?.click()}
          className={`relative flex flex-col items-center justify-center gap-3 rounded-xl border-2 border-dashed p-8 cursor-pointer transition-colors ${
            dragActive
              ? "border-primary bg-primary/5"
              : "border-muted-foreground/25 hover:border-primary/50 hover:bg-muted/50"
          }`}
        >
          <div className={`rounded-full p-3 ${dragActive ? "bg-primary/10" : "bg-muted"}`}>
            <UploadIcon className={`size-6 ${dragActive ? "text-primary" : "text-muted-foreground"}`} />
          </div>
          <div className="text-center">
            <p className="text-sm">
              <span className="font-semibold text-primary">Click here</span>{" "}
              to upload your file or drag and drop.
            </p>
            <p className="text-xs text-muted-foreground mt-1">
              Supported format: JSON
            </p>
          </div>
          <input
            ref={inputRef}
            type="file"
            accept=".json"
            multiple
            className="hidden"
            onChange={(e) => {
              if (e.target.files?.length) addFiles(e.target.files);
              e.target.value = "";
            }}
          />
        </div>

        {/* File list */}
        {files.length > 0 && (
          <div className="space-y-2">
            {files.map((entry, i) => (
              <FileCard
                key={`${entry.file.name}-${i}`}
                entry={entry}
                onRemove={() => removeFile(entry.file)}
                onRetry={() => retryFile(entry.file)}
              />
            ))}
          </div>
        )}
      </CardContent>
    </Card>
  );
}

function FileCard({
  entry,
  onRemove,
  onRetry,
}: {
  entry: FileEntry;
  onRemove: () => void;
  onRetry: () => void;
}) {
  const progressColor =
    entry.status === "success"
      ? "bg-green-500"
      : entry.status === "error"
        ? "bg-red-500"
        : "bg-blue-500";

  const bgColor =
    entry.status === "error"
      ? "border-red-200 bg-red-50"
      : entry.status === "success"
        ? "border-green-200 bg-green-50"
        : "border-border bg-card";

  return (
    <div className={`flex items-center gap-3 rounded-lg border p-3 transition-all duration-500 ${bgColor} ${entry.fading ? "opacity-0 max-h-0 py-0 my-0 overflow-hidden" : "opacity-100 max-h-40"}`}>
      {/* Icon */}
      <div
        className={`flex-shrink-0 rounded-full p-2 ${
          entry.status === "error"
            ? "bg-red-100 text-red-600"
            : entry.status === "success"
              ? "bg-green-100 text-green-600"
              : "bg-blue-100 text-blue-600"
        }`}
      >
        <FileJsonIcon className="size-4" />
      </div>

      {/* Content */}
      <div className="flex-1 min-w-0">
        <div className="flex items-center justify-between">
          <p className="text-sm font-medium truncate">{entry.file.name}</p>
          {entry.status === "success" && (
            <CheckCircleIcon className="size-4 text-green-600 flex-shrink-0" />
          )}
          {entry.status === "error" && (
            <AlertCircleIcon className="size-4 text-red-600 flex-shrink-0" />
          )}
          {entry.status === "uploading" && (
            <LoaderIcon className="size-4 text-blue-600 animate-spin flex-shrink-0" />
          )}
        </div>

        {/* Progress bar */}
        <div className="mt-1.5 h-1.5 w-full rounded-full bg-muted overflow-hidden">
          <div
            className={`h-full rounded-full transition-all duration-300 ${progressColor}`}
            style={{ width: `${entry.progress}%` }}
          />
        </div>

        <div className="flex items-center justify-between mt-1">
          <p className="text-xs text-muted-foreground">
            {entry.status === "uploading" && "Uploading…"}
            {entry.status === "success" && (entry.message || "Upload Successful!")}
            {entry.status === "error" && (entry.message || "Upload failed! Please try again.")}
            {entry.status === "pending" && "Waiting…"}
          </p>
          <div className="flex items-center gap-2">
            {entry.status === "uploading" && (
              <span className="text-xs text-muted-foreground">{entry.progress}%</span>
            )}
            {entry.status === "error" && (
              <button
                onClick={onRetry}
                className="text-xs font-semibold text-blue-600 hover:text-blue-800 flex items-center gap-1"
              >
                Try Again ↻
              </button>
            )}
          </div>
        </div>
      </div>

      {/* Remove button */}
      {(entry.status === "error" || entry.status === "success") && (
        <button
          onClick={onRemove}
          className="flex-shrink-0 p-1 rounded hover:bg-muted transition-colors"
        >
          <XIcon className="size-4 text-muted-foreground" />
        </button>
      )}
    </div>
  );
}
