import { ActionsPane } from "@/components/ActionsPane";
import { Separator } from "@/components/ui/separator";
import { Button } from "@/components/ui/button";
import { useTasks } from "@/hooks/useTasks";
import { TaskCard } from "@/components/TaskCard";
import { deleteDataset, deleteAllDatasets } from "@/lib/api";
import { Trash2Icon } from "lucide-react";

function App() {
  const { tasks, refresh } = useTasks(2000);

  const handleDelete = async (datasetId: string) => {
    try {
      await deleteDataset(datasetId);
      refresh();
    } catch (e) {
      console.error("Failed to delete dataset", e);
    }
  };

  const handleDeleteAll = async () => {
    try {
      await deleteAllDatasets();
      refresh();
    } catch (e) {
      console.error("Failed to delete all datasets", e);
    }
  };

  return (
    <div className="min-h-screen flex flex-col bg-background">
      {/* Header */}
      <header className="border-b px-6 py-3">
        <h1 className="text-xl font-bold">Data Processing System</h1>
      </header>

      {/* Top pane: Actions */}
      <ActionsPane onUploadSuccess={refresh} />

      <Separator />

      {/* Bottom pane: Dataset results */}
      <div className="flex-1 overflow-auto p-4">
        {tasks.length > 0 && (
          <>
            <div className="flex items-center justify-between mb-4">
              <h2 className="text-lg font-semibold">Datasets</h2>
              <Button variant="outline" size="sm" onClick={handleDeleteAll} className="text-red-600 hover:text-red-700 hover:bg-red-50">
                <Trash2Icon className="size-3.5 mr-1.5" />
                Clear All
              </Button>
            </div>
            <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
              {tasks.map((task) => (
                <TaskCard key={task.dataset_id} dataset={task} onDelete={handleDelete} />
              ))}
            </div>
          </>
        )}
      </div>
    </div>
  );
}

export default App;
