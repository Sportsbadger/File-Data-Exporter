SiteTracker → Salesforce Files Storage Export (ContentDocumentId join)

This script helps identify Salesforce File Storage usage driven by SiteTracker attachments.

It takes a CSV export from sitetracker__Attachment__c that contains ContentDocument IDs, downloads File metadata (not file contents) for all Salesforce Files, then joins the two datasets locally to produce storage-focused outputs.

What it does

Reads a SiteTracker export CSV containing sitetracker__ContentDocumentRecord__c (ContentDocumentId).

Queries Salesforce ContentVersion for latest file versions only (IsLatest = true) to get:

Title, type/extension, size (ContentSize), dates, owner/creator metadata.

Filters those file rows to only the ContentDocumentIds present in the SiteTracker CSV.

Outputs:

Merged CSV: original SiteTracker rows + file size + file metadata

Files-only CSV: deduped list of matched files with size + metadata (one row per file)

✅ Metadata only: It does not download any file binaries (no PDFs/images/etc).

Prerequisites
1) Salesforce CLI

You must have Salesforce CLI installed and available in PATH:

sf --version

2) Salesforce permissions

To see all Files in the org, the running user needs org-wide file visibility. One working setup is:

View All Data (and/or your org’s admin access), plus

Query All Files (recommended for org-wide file visibility)

If you don’t have sufficient access, the script will only return files you can see (owned by/shared with you).

3) Python + packages

Python 3.9+ recommended.

Install dependencies:

pip install pandas requests

Salesforce login (once)

Login with Salesforce CLI and set an alias:

sf org login web --alias myorg


Confirm the alias exists:

sf org list

Required input
SiteTracker CSV export

A CSV file exported from sitetracker__Attachment__c (or Excel export saved as CSV) containing a column with ContentDocument IDs.

Required column (default SiteTracker field name):

sitetracker__ContentDocumentRecord__c

The CSV may contain many other fields; the script keeps them and merges file details onto each row.

Usage
Minimal command (recommended starting point)
python merge_sitetracker_docids_to_files.py `
  --alias myorg `
  --sitetracker-csv sitetracker_attachments.csv `
  --docid-col sitetracker__ContentDocumentRecord__c


This produces two files in the current directory:

merged_sitetracker_files.csv

sitetracker_files_only.csv

Arguments
Required

--alias
Salesforce CLI org alias (e.g. myorg)

--sitetracker-csv
Path to the SiteTracker export CSV

--docid-col
Column name in the CSV that holds the ContentDocumentId values
Example: sitetracker__ContentDocumentRecord__c

Optional

--out
Output path for merged CSV
Default: merged_sitetracker_files.csv

--out-files-only
Output path for deduped files-only CSV
Default: sitetracker_files_only.csv

--api-version
Salesforce API version
Default: 60.0

--login-url
Use when logging into a sandbox or a specific instance
Examples:

https://login.salesforce.com

https://test.salesforce.com

--sitetracker-columns
Comma-separated list of SiteTracker columns to keep (reduces output size and speeds Excel usage).
The --docid-col will always be included even if not listed.

Example:

--sitetracker-columns "Id,Name,sitetracker__ContentDocumentRecord__c,sitetracker__Parent_Name__c,sitetracker__Proj__c"


--bulk-max-records
Bulk API 2.0 result page size (maxRecords)
Default: 50000

Output columns (high level)
Files-only output (sitetracker_files_only.csv)

Typical columns include:

ContentDocumentId

Title

FileType

FileExtension

ContentSize (raw, bytes from Salesforce)

ContentSizeBytes (numeric)

ContentSizeMB

CreatedDate, LastModifiedDate

FileUrl (clickable Lightning URL)

Merged output (merged_sitetracker_files.csv)

Contains all selected SiteTracker columns plus the file columns above.

Notes & troubleshooting
“I only get files owned by me / admins”

That means you do not have org-wide file visibility. Ask an admin to grant:

Query All Files (preferred), and/or

appropriate admin permissions (View All Data / Modify All Data), depending on org policy.

“Some SiteTracker rows have no matched file size”

Common causes:

ContentDocumentId field is blank/invalid in those rows

File exists but your user cannot see it (permissions/sharing)

File was deleted or moved, but SiteTracker row still references it

Large datasets

With ~180k ContentDocumentIds, output CSVs can be large. Use --sitetracker-columns to keep only what you need, and open in Power Pivot / Power BI if Excel struggles.

What this script does NOT do

Does not download file binaries (VersionData)

Does not delete or modify Salesforce data

Does not attempt to resolve record names for polymorphic links
