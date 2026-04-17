import { describe, it, expect, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import { StatusPane } from "./StatusPane";
import type { DatasetStatus } from "@/types";

// TaskCard uses shadcn/ui + lucide icons that are noisy in jsdom;
// shallow-mock it to keep these tests focused on StatusPane logic.
vi.mock("@/components/TaskCard", () => ({
  TaskCard: ({ dataset }: { dataset: DatasetStatus }) => (
    <div data-testid="task-card">{dataset.dataset_id}</div>
  ),
}));

const makeTask = (id: string, status: DatasetStatus["status"] = "QUEUED"): DatasetStatus => ({
  dataset_id: id,
  filename: `${id}.json`,
  status,
  result: null,
  error: null,
  created_at: null,
  updated_at: null,
});

describe("StatusPane", () => {
  it("shows empty state when no tasks and no error", () => {
    render(<StatusPane tasks={[]} error={null} />);
    expect(screen.getByText(/no datasets submitted yet/i)).toBeInTheDocument();
  });

  it("renders error banner when error is present", () => {
    render(<StatusPane tasks={[]} error="Server unreachable" />);
    expect(screen.getByText(/error fetching tasks/i)).toHaveTextContent("Server unreachable");
  });

  it("does not show empty-state text when there is an error", () => {
    render(<StatusPane tasks={[]} error="bad" />);
    expect(screen.queryByText(/no datasets submitted yet/i)).not.toBeInTheDocument();
  });

  it("renders one TaskCard per task", () => {
    const tasks = [makeTask("ds_a"), makeTask("ds_b", "COMPLETED"), makeTask("ds_c", "FAILED")];
    render(<StatusPane tasks={tasks} error={null} />);
    const cards = screen.getAllByTestId("task-card");
    expect(cards).toHaveLength(3);
    expect(cards.map((c) => c.textContent)).toEqual(["ds_a", "ds_b", "ds_c"]);
  });
});
