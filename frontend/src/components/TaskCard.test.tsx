import { describe, it, expect, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { TaskCard } from "./TaskCard";
import type { DatasetStatus } from "@/types";

const base: DatasetStatus = {
  dataset_id: "ds_001",
  filename: "ds_001.json",
  status: "QUEUED",
  result: null,
  error: null,
  created_at: "2026-01-01T00:00:00Z",
  updated_at: "2026-01-01T00:00:00Z",
};

describe("TaskCard", () => {
  it("renders dataset id, filename, and status badge", () => {
    render(<TaskCard dataset={base} />);
    expect(screen.getByText("ds_001")).toBeInTheDocument();
    expect(screen.getByText("ds_001.json")).toBeInTheDocument();
    expect(screen.getByText("Queued")).toBeInTheDocument();
  });

  it("renders computed result for COMPLETED datasets", () => {
    const ds: DatasetStatus = {
      ...base,
      status: "COMPLETED",
      result: {
        dataset_id: "ds_001",
        record_count: 9,
        invalid_records: 2,
        average_value: 17.5,
        category_summary: { A: 4, B: 3 },
      },
    };
    render(<TaskCard dataset={ds} />);
    expect(screen.getByText("Completed")).toBeInTheDocument();
    expect(screen.getByText("9")).toBeInTheDocument();       // record_count
    expect(screen.getByText("2")).toBeInTheDocument();       // invalid_records
    expect(screen.getByText("17.5")).toBeInTheDocument();    // average_value
    expect(screen.getByText("A: 4")).toBeInTheDocument();
    expect(screen.getByText("B: 3")).toBeInTheDocument();
  });

  it("renders the error message for FAILED datasets", () => {
    const ds: DatasetStatus = { ...base, status: "FAILED", error: "worker crashed" };
    render(<TaskCard dataset={ds} />);
    expect(screen.getByText("Failed")).toBeInTheDocument();
    expect(screen.getByText("worker crashed")).toBeInTheDocument();
  });

  it("does not render delete button without onDelete prop", () => {
    render(<TaskCard dataset={base} />);
    expect(screen.queryByRole("button")).not.toBeInTheDocument();
  });

  it("invokes onDelete with dataset_id when trash button is clicked", async () => {
    const onDelete = vi.fn();
    render(<TaskCard dataset={base} onDelete={onDelete} />);
    const btn = screen.getByRole("button");
    await userEvent.click(btn);
    expect(onDelete).toHaveBeenCalledWith("ds_001");
  });

  it("falls back to QUEUED styling when status is unknown", () => {
    // @ts-expect-error intentionally passing an unknown status
    render(<TaskCard dataset={{ ...base, status: "MYSTERY" }} />);
    expect(screen.getByText("Queued")).toBeInTheDocument();
  });
});
