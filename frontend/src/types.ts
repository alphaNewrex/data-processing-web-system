export interface DatasetResult {
  dataset_id: string;
  record_count: number;
  category_summary: Record<string, number>;
  average_value: number;
  invalid_records: number;
}

export interface DatasetStatus {
  dataset_id: string;
  filename: string;
  status: "QUEUED" | "PREPROCESSING" | "COMPUTING" | "SUMMARISING" | "COMPLETED" | "FAILED";
  result: DatasetResult | null;
  error: string | null;
  created_at: string | null;
  updated_at: string | null;
}

export interface UploadResponse {
  dataset_id: string;
  status: string;
}
