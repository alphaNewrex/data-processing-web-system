import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Separator } from "@/components/ui/separator";
import { Trash2Icon } from "lucide-react";
import type { DatasetStatus } from "@/types";

const STATUS_CONFIG: Record<string, { label: string; variant: "default" | "secondary" | "destructive" | "outline" }> = {
  QUEUED: { label: "Queued", variant: "outline" },
  PREPROCESSING: { label: "Preprocessing", variant: "secondary" },
  COMPUTING: { label: "Computing", variant: "default" },
  SUMMARISING: { label: "Summarising", variant: "secondary" },
  COMPLETED: { label: "Completed", variant: "default" },
  FAILED: { label: "Failed", variant: "destructive" },
};

const STEPS = ["QUEUED", "PREPROCESSING", "COMPUTING", "SUMMARISING", "COMPLETED"];

function StepProgress({ status }: { status: string }) {
  const currentIdx = STEPS.indexOf(status);
  return (
    <div className="flex items-center gap-1 mt-2">
      {STEPS.map((step, i) => (
        <div
          key={step}
          className={`h-1.5 flex-1 rounded-full ${
            i <= currentIdx
              ? status === "COMPLETED"
                ? "bg-green-500"
                : "bg-blue-500"
              : "bg-muted"
          }`}
        />
      ))}
    </div>
  );
}

interface TaskCardProps {
  dataset: DatasetStatus;
  onDelete?: (datasetId: string) => void;
}

export function TaskCard({ dataset, onDelete }: TaskCardProps) {
  const config = STATUS_CONFIG[dataset.status] || STATUS_CONFIG.QUEUED;

  return (
    <Card>
      <CardHeader className="pb-2">
        <div className="flex items-center justify-between">
          <CardTitle className="text-sm font-mono">{dataset.dataset_id}</CardTitle>
          <div className="flex items-center gap-2">
            <Badge variant={config.variant}>{config.label}</Badge>
            {onDelete && (
              <Button
                variant="ghost"
                size="icon"
                className="h-6 w-6 text-muted-foreground hover:text-red-600"
                onClick={() => onDelete(dataset.dataset_id)}
              >
                <Trash2Icon className="size-3.5" />
              </Button>
            )}
          </div>
        </div>
        <p className="text-xs text-muted-foreground">{dataset.filename}</p>
        {dataset.status !== "FAILED" && <StepProgress status={dataset.status} />}
      </CardHeader>

      {dataset.status === "COMPLETED" && dataset.result && (
        <CardContent className="pt-0">
          <Separator className="mb-3" />
          <div className="grid grid-cols-2 gap-2 text-sm">
            <div>
              <span className="text-muted-foreground">Records:</span>{" "}
              <span className="font-medium">{dataset.result.record_count}</span>
            </div>
            <div>
              <span className="text-muted-foreground">Invalid:</span>{" "}
              <span className="font-medium text-red-600">{dataset.result.invalid_records}</span>
            </div>
            <div className="col-span-2">
              <span className="text-muted-foreground">Average Value:</span>{" "}
              <span className="font-medium">{dataset.result.average_value}</span>
            </div>
          </div>
          {dataset.result.category_summary && (
            <div className="mt-2">
              <p className="text-xs text-muted-foreground mb-1">Category Summary</p>
              <div className="flex flex-wrap gap-2">
                {Object.entries(dataset.result.category_summary).map(([cat, count]) => (
                  <Badge key={cat} variant="outline" className="text-xs">
                    {cat}: {count}
                  </Badge>
                ))}
              </div>
            </div>
          )}
        </CardContent>
      )}

      {dataset.status === "FAILED" && dataset.error && (
        <CardContent className="pt-0">
          <Separator className="mb-3" />
          <p className="text-sm text-red-600">{dataset.error}</p>
        </CardContent>
      )}

      {dataset.created_at && (
        <CardContent className="pt-0">
          <p className="text-xs text-muted-foreground">
            Submitted: {new Date(dataset.created_at).toLocaleString()}
          </p>
        </CardContent>
      )}
    </Card>
  );
}
