import { useEffect, useState, useCallback } from "react";
import type { DatasetStatus } from "@/types";
import { fetchDatasets } from "@/lib/api";

export function useTasks(pollIntervalMs = 2000) {
  const [tasks, setTasks] = useState<DatasetStatus[]>([]);
  const [error, setError] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    try {
      const data = await fetchDatasets();
      setTasks(data);
      setError(null);
    } catch (e) {
      // Silently ignore network errors (backend not ready yet)
      // Only surface non-network errors
      const msg = e instanceof Error ? e.message : "Unknown error";
      if (!msg.toLowerCase().includes("network") && !msg.toLowerCase().includes("fetch")) {
        setError(msg);
      }
    }
  }, []);

  useEffect(() => {
    refresh();
    const interval = setInterval(refresh, pollIntervalMs);
    return () => clearInterval(interval);
  }, [refresh, pollIntervalMs]);

  // Check if any task is still in-progress (needs polling)
  const hasActiveTasks = tasks.some(
    (t) => t.status !== "COMPLETED" && t.status !== "FAILED"
  );

  return { tasks, error, refresh, hasActiveTasks };
}
