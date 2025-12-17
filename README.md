# SiteTracker File Metadata Exporter

Export Salesforce File metadata linked from SiteTracker attachment exports without downloading binaries.

## What this tool does
- Reads a SiteTracker CSV export with `ContentDocumentId` values.
- Authenticates via Salesforce CLI and queries **latest** `ContentVersion` rows (metadata only).
- Filters the query results to the IDs present in the CSV.
- Writes two UTF-8 CSVs:
  - **Merged**: SiteTracker rows enriched with file size + metadata.
  - **Files only**: Deduped file list with size + metadata and clickable Lightning URLs.

## Quick start
```bash
python sitetracker_file_exporter.py \
  --alias myorg \
  --sitetracker-csv sitetracker_attachments.csv \
  --docid-col sitetracker__ContentDocumentRecord__c
```
Outputs in the working directory:
- `merged_sitetracker_files.csv`
- `sitetracker_files_only.csv`

## Prerequisites
1. **Salesforce CLI** available in `PATH` (`sf --version`).
2. **Salesforce permissions**: org-wide file visibility recommended (`Query All Files` + admin-level access) or you will only see files shared with you.
3. **Python 3.10+** with dependencies: `pip install pandas requests`.
4. **Salesforce login** once per machine: `sf org login web --alias myorg` (use `--instance-url https://test.salesforce.com` for sandboxes). Confirm with `sf org list`.

## Arguments
Required:
- `--alias` – Salesforce CLI org alias.
- `--sitetracker-csv` – Path to the SiteTracker CSV export.
- `--docid-col` – Column containing `ContentDocumentId` values (e.g., `sitetracker__ContentDocumentRecord__c`).

Optional:
- `--out` – Merged CSV path. Default: `merged_sitetracker_files.csv`.
- `--out-files-only` – Files-only CSV path. Default: `sitetracker_files_only.csv`.
- `--api-version` – Salesforce API version. Default: `60.0`.
- `--login-url` – Use for sandboxes/custom domains (`https://login.salesforce.com` or `https://test.salesforce.com`).
- `--sitetracker-columns` – Comma-separated SiteTracker columns to keep (DocId column is always retained).
- `--bulk-max-records` – Bulk API 2.0 page size (`maxRecords`). Default: `50000`.
- `--encoding` – CSV encoding for input (default `utf-8-sig` handles Excel BOM).

## Output columns (high level)
**Files only** (`sitetracker_files_only.csv`):
- `ContentDocumentId`, `Title`, `FileType`, `FileExtension`, `ContentSize`, `ContentSizeBytes`, `ContentSizeMB`, `CreatedDate`, `LastModifiedDate`, `FileUrl`.

**Merged** (`merged_sitetracker_files.csv`):
- All selected SiteTracker columns plus the fields above and `SiteTrackerAttachmentUrl` (when the input contains `Id`).

## Notes & troubleshooting
- If you only see files you own, request org-wide file visibility (e.g., `Query All Files` or admin-level access).
- Rows missing file size typically have blank/invalid IDs or are inaccessible due to sharing/permissions.
- Large datasets: use `--sitetracker-columns` to reduce width for Excel; consider Power BI/Power Pivot for >100k rows.
