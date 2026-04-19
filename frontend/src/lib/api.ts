import type { DatasetStatus, UploadResponse } from "@/types";

// Relative by default. In Docker, nginx proxies /api → api:8000.
// In `vite dev`, vite.config.ts adds a /api proxy to http://localhost:8000.
// Override with VITE_API_BASE at build time if needed.
const API_BASE = import.meta.env.VITE_API_BASE ?? "/api";

export async function uploadDataset(file: File): Promise<UploadResponse> {
  const formData = new FormData();
  formData.append("file", file);

  const res = await fetch(`${API_BASE}/dataset`, {
    method: "POST",
    body: formData,
  });

  if (!res.ok) {
    const err = await res.json();
    throw new Error(err.detail || "Upload failed");
  }

  return res.json();
}

export async function fetchDatasets(): Promise<DatasetStatus[]> {
  const res = await fetch(`${API_BASE}/datasets`);
  if (!res.ok) {
    throw new Error("Failed to fetch datasets");
  }
  return res.json();
}

export async function fetchDataset(datasetId: string): Promise<DatasetStatus> {
  const res = await fetch(`${API_BASE}/dataset/${datasetId}`);
  if (!res.ok) {
    throw new Error("Failed to fetch dataset");
  }
  return res.json();
}

export async function deleteDataset(datasetId: string): Promise<void> {
  const res = await fetch(`${API_BASE}/dataset/${datasetId}`, {
    method: "DELETE",
  });
  if (!res.ok) {
    const err = await res.json();
    throw new Error(err.detail || "Delete failed");
  }
}

export async function deleteAllDatasets(): Promise<void> {
  const res = await fetch(`${API_BASE}/datasets`, {
    method: "DELETE",
  });
  if (!res.ok) {
    const err = await res.json();
    throw new Error(err.detail || "Delete all failed");
  }
}
