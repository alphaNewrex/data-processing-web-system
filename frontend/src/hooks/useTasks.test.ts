import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { renderHook, waitFor, act } from "@testing-library/react";
import type { DatasetStatus } from "@/types";

vi.mock("@/lib/api", () => ({
  fetchDatasets: vi.fn(),
}));

import { useTasks } from "./useTasks";
import { fetchDatasets } from "@/lib/api";

const mockedFetch = vi.mocked(fetchDatasets);

const makeTask = (overrides: Partial<DatasetStatus> = {}): DatasetStatus => ({
  dataset_id: "ds_1",
  filename: "ds_1.json",
  status: "QUEUED",
  result: null,
  error: null,
  created_at: "2026-01-01T00:00:00Z",
  updated_at: "2026-01-01T00:00:00Z",
  ...overrides,
});

describe("useTasks", () => {
  beforeEach(() => {
    mockedFetch.mockReset();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("loads tasks on mount", async () => {
    const tasks = [makeTask({ dataset_id: "a" }), makeTask({ dataset_id: "b" })];
    mockedFetch.mockResolvedValueOnce(tasks);

    const { result } = renderHook(() => useTasks(2000));

    await waitFor(() => expect(result.current.tasks).toHaveLength(2));
    expect(result.current.error).toBeNull();
    expect(result.current.tasks.map((t) => t.dataset_id)).toEqual(["a", "b"]);
  });

  it("polls on the given interval", async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    mockedFetch.mockResolvedValue([]);
    renderHook(() => useTasks(1000));

    await waitFor(() => expect(mockedFetch).toHaveBeenCalledTimes(1));

    await act(async () => {
      await vi.advanceTimersByTimeAsync(1000);
    });
    expect(mockedFetch).toHaveBeenCalledTimes(2);

    await act(async () => {
      await vi.advanceTimersByTimeAsync(1000);
    });
    expect(mockedFetch).toHaveBeenCalledTimes(3);
  });

  it("swallows network errors silently", async () => {
    mockedFetch.mockRejectedValueOnce(new Error("Failed to fetch datasets"));

    const { result } = renderHook(() => useTasks(2000));
    await waitFor(() => expect(mockedFetch).toHaveBeenCalled());
    // 'fetch' in the message -> treated as network, error stays null
    expect(result.current.error).toBeNull();
  });

  it("surfaces non-network errors", async () => {
    mockedFetch.mockRejectedValueOnce(new Error("boom!"));

    const { result } = renderHook(() => useTasks(2000));
    await waitFor(() => expect(result.current.error).toBe("boom!"));
  });

  it("reports hasActiveTasks correctly", async () => {
    mockedFetch.mockResolvedValueOnce([
      makeTask({ dataset_id: "a", status: "COMPLETED" }),
      makeTask({ dataset_id: "b", status: "COMPUTING" }),
    ]);
    const { result } = renderHook(() => useTasks(2000));
    await waitFor(() => expect(result.current.tasks).toHaveLength(2));
    expect(result.current.hasActiveTasks).toBe(true);
  });

  it("hasActiveTasks is false when all tasks are terminal", async () => {
    mockedFetch.mockResolvedValueOnce([
      makeTask({ dataset_id: "a", status: "COMPLETED" }),
      makeTask({ dataset_id: "b", status: "FAILED" }),
    ]);
    const { result } = renderHook(() => useTasks(2000));
    await waitFor(() => expect(result.current.tasks).toHaveLength(2));
    expect(result.current.hasActiveTasks).toBe(false);
  });

  it("refresh() manually re-fetches", async () => {
    mockedFetch.mockResolvedValueOnce([]);
    const { result } = renderHook(() => useTasks(60_000));
    await waitFor(() => expect(mockedFetch).toHaveBeenCalledTimes(1));

    mockedFetch.mockResolvedValueOnce([makeTask()]);
    await act(async () => {
      await result.current.refresh();
    });
    expect(result.current.tasks).toHaveLength(1);
  });
});
