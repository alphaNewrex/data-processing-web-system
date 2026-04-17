import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

vi.mock("@/lib/api", () => ({
  uploadDataset: vi.fn(),
}));

import { ActionsPane } from "./ActionsPane";
import { uploadDataset } from "@/lib/api";

const mockedUpload = vi.mocked(uploadDataset);

function makeJsonFile(name = "ds.json", content = '{"dataset_id":"ds"}') {
  return new File([content], name, { type: "application/json" });
}

describe("ActionsPane", () => {
  beforeEach(() => {
    mockedUpload.mockReset();
  });

  it("renders the dropzone", () => {
    render(<ActionsPane onUploadSuccess={vi.fn()} />);
    expect(screen.getByText(/click here/i)).toBeInTheDocument();
    expect(screen.getByText(/supported format: json/i)).toBeInTheDocument();
  });

  it("uploads a chosen JSON file and calls onUploadSuccess", async () => {
    mockedUpload.mockResolvedValueOnce({ dataset_id: "ds", status: "QUEUED" });
    const onUploadSuccess = vi.fn();
    const { container } = render(<ActionsPane onUploadSuccess={onUploadSuccess} />);

    const input = container.querySelector('input[type="file"]') as HTMLInputElement;
    const file = makeJsonFile();

    await userEvent.upload(input, file);

    await waitFor(() => expect(mockedUpload).toHaveBeenCalledWith(file));
    await waitFor(() => expect(onUploadSuccess).toHaveBeenCalled());
  });

  it("ignores non-JSON files", async () => {
    render(<ActionsPane onUploadSuccess={vi.fn()} />);
    const input = document.querySelector('input[type="file"]') as HTMLInputElement;
    const bad = new File(["hi"], "notes.txt", { type: "text/plain" });

    await userEvent.upload(input, bad);

    expect(mockedUpload).not.toHaveBeenCalled();
  });

  it("shows an error message when upload fails", async () => {
    mockedUpload.mockRejectedValueOnce(new Error("Dataset already exists"));
    render(<ActionsPane onUploadSuccess={vi.fn()} />);

    const input = document.querySelector('input[type="file"]') as HTMLInputElement;
    await userEvent.upload(input, makeJsonFile());

    await waitFor(() =>
      expect(screen.getByText(/dataset already exists/i)).toBeInTheDocument(),
    );
  });
});
