import { TaskCard } from "@/components/TaskCard";
import type { DatasetStatus } from "@/types";

interface StatusPaneProps {
  tasks: DatasetStatus[];
  error: string | null;
}

export function StatusPane({ tasks, error }: StatusPaneProps) {
  return (
    <div className="flex-1 overflow-auto p-4">
      <h2 className="text-lg font-semibold mb-4">Processing Tasks</h2>

      {error && (
        <p className="text-sm text-red-600 mb-4">Error fetching tasks: {error}</p>
      )}

      {tasks.length === 0 && !error && (
        <p className="text-sm text-muted-foreground">
          No datasets submitted yet. Upload a JSON file above to get started.
        </p>
      )}

      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
        {tasks.map((task) => (
          <TaskCard key={task.dataset_id} dataset={task} />
        ))}
      </div>
    </div>
  );
}
