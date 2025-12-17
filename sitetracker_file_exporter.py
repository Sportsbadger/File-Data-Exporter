#!/usr/bin/env python3
"""SiteTracker to Salesforce File metadata exporter."""
from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import time
from io import StringIO
from pathlib import Path
from typing import Any, Optional, Sequence, Set

import pandas as pd
import requests


SALESFORCE_LOGIN_URLS: tuple[str, ...] = (
    "https://login.salesforce.com",
    "https://test.salesforce.com",
)


def pick_cli() -> str:
    """Return the first available Salesforce CLI command in PATH.

    Returns:
        str: CLI executable name (sf/sfdx) found on PATH.

    Raises:
        FileNotFoundError: If no supported CLI executable is available.
    """
    for cmd in ("sf.cmd", "sf", "sfdx.cmd", "sfdx"):
        if shutil.which(cmd):
            return cmd
    raise FileNotFoundError(
        "Salesforce CLI not found on PATH. Verify `sf --version` works in this terminal."
    )


def run_cmd(cmd: Sequence[str]) -> str:
    """Run a subprocess command and return stdout.

    Args:
        cmd: Command and arguments to execute.

    Returns:
        str: Captured stdout from the process.

    Raises:
        RuntimeError: If the process exits with a non-zero status.
    """
    completed = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        shell=False,
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            "Command failed:\n  {cmd}\n\nSTDERR:\n{stderr}".format(
                cmd=" ".join(cmd), stderr=completed.stderr.strip()
            )
        )
    return completed.stdout


def sf_org_auth(alias: str, login_url: Optional[str] = None) -> dict[str, str]:
    """Return access token and instance URL for a Salesforce org alias.

    Args:
        alias: Salesforce CLI alias to authenticate against.
        login_url: Optional login URL for sandbox or custom domains.

    Returns:
        dict[str, str]: Mapping containing ``accessToken`` and ``instanceUrl``.

    Raises:
        RuntimeError: If authentication succeeds but the response is missing
            required fields.
        FileNotFoundError: If no supported CLI executable is available.
    """
    cli = pick_cli()

    def org_display() -> dict[str, Any]:
        if cli.startswith("sf"):
            output = run_cmd([cli, "org", "display", "--json", "--target-org", alias])
        else:
            output = run_cmd([cli, "force:org:display", "--json", "-u", alias])
        return json.loads(output)

    def org_login_web() -> None:
        command = [cli]
        if cli.startswith("sf"):
            command += ["org", "login", "web", "--alias", alias]
            if login_url:
                command += ["--instance-url", login_url]
        else:
            command += ["force:auth:web:login", "-a", alias]
            if login_url:
                command += ["-r", login_url]
        run_cmd(command)

    try:
        data = org_display()
    except Exception:
        org_login_web()
        data = org_display()

    result = data.get("result") or {}
    access_token = result.get("accessToken")
    instance_url = result.get("instanceUrl")
    if not access_token or not instance_url:
        raise RuntimeError("Could not extract accessToken/instanceUrl from Salesforce CLI output.")
    return {"accessToken": access_token, "instanceUrl": instance_url}


def bulk2_create_job(instance_url: str, access_token: str, api_version: str, soql: str) -> str:
    """Create a Bulk API 2.0 query job and return its id.

    Args:
        instance_url: Salesforce instance base URL.
        access_token: OAuth token from Salesforce CLI.
        api_version: Salesforce API version (e.g., ``60.0``).
        soql: SOQL query string.

    Returns:
        str: Created Bulk API job id.

    Raises:
        RuntimeError: If Salesforce returns an HTTP error.
    """
    url = f"{instance_url}/services/data/v{api_version}/jobs/query"
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
    payload = {
        "operation": "query",
        "query": soql,
        "contentType": "CSV",
        "columnDelimiter": "COMMA",
        "lineEnding": "LF",
    }
    response = requests.post(url, headers=headers, json=payload, timeout=60)
    if response.status_code >= 400:
        raise RuntimeError(
            "Bulk job creation failed HTTP {status}:\n{body}\nSOQL:\n{soql}".format(
                status=response.status_code, body=response.text, soql=soql
            )
        )
    return response.json()["id"]


def bulk2_wait_job(
    instance_url: str, access_token: str, api_version: str, job_id: str
) -> dict[str, Any]:
    """Poll a Bulk API 2.0 query job until completion.

    Args:
        instance_url: Salesforce instance base URL.
        access_token: OAuth token from Salesforce CLI.
        api_version: Salesforce API version (e.g., ``60.0``).
        job_id: Bulk API 2.0 query job id to poll.

    Returns:
        dict[str, Any]: Job status response.

    Raises:
        RuntimeError: If the job finishes in a failure state.
    """
    url = f"{instance_url}/services/data/v{api_version}/jobs/query/{job_id}"
    headers = {"Authorization": f"Bearer {access_token}"}
    while True:
        response = requests.get(url, headers=headers, timeout=60)
        response.raise_for_status()
        info = response.json()
        state = info.get("state")
        if state in ("JobComplete", "Failed", "Aborted"):
            if state != "JobComplete":
                raise RuntimeError(f"Bulk job ended in state {state}: {info}")
            return info
        time.sleep(3)


def bulk2_fetch_results(
    instance_url: str,
    access_token: str,
    api_version: str,
    job_id: str,
    max_records: int = 50000,
) -> pd.DataFrame:
    """Return concatenated DataFrame of Bulk API 2.0 results.

    Args:
        instance_url: Salesforce instance base URL.
        access_token: OAuth token from Salesforce CLI.
        api_version: Salesforce API version (e.g., ``60.0``).
        job_id: Bulk API 2.0 query job id to fetch.
        max_records: Page size used for result pagination.

    Returns:
        pandas.DataFrame: All pages concatenated.
    """
    url = f"{instance_url}/services/data/v{api_version}/jobs/query/{job_id}/results"
    headers = {"Authorization": f"Bearer {access_token}", "Accept": "text/csv"}

    locator: Optional[str] = None
    frames: list[pd.DataFrame] = []

    while True:
        params = {"maxRecords": max_records}
        if locator:
            params["locator"] = locator

        response = requests.get(url, headers=headers, params=params, timeout=300)
        response.raise_for_status()

        if response.text.strip():
            frames.append(pd.read_csv(StringIO(response.text)))

        locator = response.headers.get("Sforce-Locator")
        if not locator or locator.lower() == "null":
            break

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def read_sitetracker_csv(
    path: Path,
    docid_col: str,
    selected_columns: Optional[Sequence[str]] = None,
    encoding: str = "utf-8-sig",
) -> pd.DataFrame:
    """Load the SiteTracker export CSV with optional column trimming.

    Args:
        path: Path to the CSV export from SiteTracker.
        docid_col: Column name holding ``ContentDocumentId`` values.
        selected_columns: Columns to retain (DocId is always retained).
        encoding: CSV encoding for input. Defaults to ``utf-8-sig`` for Excel BOM support.

    Returns:
        pandas.DataFrame: Loaded CSV data, filtered if requested.

    Raises:
        SystemExit: If ``docid_col`` is missing from the CSV header.
    """
    dataframe = pd.read_csv(path, dtype=str, encoding=encoding, low_memory=False)
    if docid_col not in dataframe.columns:
        raise SystemExit(f"Column not found in SiteTracker CSV: {docid_col}")

    if not selected_columns:
        return dataframe

    keep_cols = [col.strip() for col in selected_columns if col and col.strip() in dataframe.columns]
    if docid_col not in keep_cols:
        keep_cols.append(docid_col)
    return dataframe[keep_cols]


def extract_docids(dataframe: pd.DataFrame, docid_col: str) -> Set[str]:
    """Return unique, non-empty ContentDocumentIds from the SiteTracker CSV.

    Args:
        dataframe: SiteTracker DataFrame.
        docid_col: Column name with ContentDocument identifiers.

    Returns:
        set[str]: Unique ContentDocumentIds.
    """
    docids = dataframe[docid_col].dropna().astype(str)
    docids = docids[docids.str.len() > 0]
    return set(docids.unique().tolist())


def build_contentversion_soql() -> str:
    """Build SOQL for latest ContentVersion metadata.

    Returns:
        str: Compacted SOQL string querying ``IsLatest = true`` rows.
    """
    soql = """
    SELECT
      ContentDocumentId,
      Title,
      FileType,
      FileExtension,
      ContentSize,
      CreatedDate,
      CreatedById,
      CreatedBy.Name,
      LastModifiedDate,
      OwnerId
    FROM ContentVersion
    WHERE IsLatest = true
    """
    return " ".join(soql.split())


def add_file_urls(dataframe: pd.DataFrame, instance_url: str) -> pd.DataFrame:
    """Attach Lightning file URLs to the ContentVersion results.

    Args:
        dataframe: ContentVersion DataFrame containing ``ContentDocumentId``.
        instance_url: Salesforce instance base URL.

    Returns:
        pandas.DataFrame: Input DataFrame with ``FileUrl`` column added.
    """
    dataframe["FileUrl"] = dataframe["ContentDocumentId"].apply(
        lambda content_id: f"{instance_url}/lightning/r/ContentDocument/{content_id}/view"
    )
    return dataframe


def merge_site_tracker_and_files(
    site_tracker_df: pd.DataFrame,
    files_df: pd.DataFrame,
    docid_col: str,
    instance_url: str,
) -> pd.DataFrame:
    """Merge SiteTracker rows with Salesforce file metadata.

    Args:
        site_tracker_df: SiteTracker attachment DataFrame.
        files_df: ContentVersion metadata DataFrame.
        docid_col: Column name matching ContentDocumentId values.
        instance_url: Salesforce instance base URL.

    Returns:
        pandas.DataFrame: Merged DataFrame with optional attachment URL column.
    """
    merged = site_tracker_df.merge(
        files_df,
        how="left",
        left_on=docid_col,
        right_on="ContentDocumentId",
        suffixes=("", "_File"),
    )

    if "Id" in merged.columns:
        merged["SiteTrackerAttachmentUrl"] = merged["Id"].apply(
            lambda attachment_id: (
                f"{instance_url}/lightning/r/sitetracker__Attachment__c/{attachment_id}/view"
                if pd.notna(attachment_id)
                else pd.NA
            )
        )
    return merged


def parse_arguments(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    """Parse CLI arguments.

    Args:
        argv: Optional list of CLI arguments; defaults to ``sys.argv[1:]``.

    Returns:
        argparse.Namespace: Parsed arguments.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--alias", required=True, help="Salesforce CLI org alias")
    parser.add_argument(
        "--login-url",
        default=None,
        choices=(*SALESFORCE_LOGIN_URLS, None),
        help="Optional: https://login.salesforce.com or https://test.salesforce.com",
    )
    parser.add_argument("--api-version", default="60.0", help="Salesforce API version")
    parser.add_argument("--sitetracker-csv", required=True, type=Path, help="CSV exported from sitetracker__Attachment__c")
    parser.add_argument(
        "--docid-col",
        required=True,
        help="Column name holding ContentDocumentId (e.g. sitetracker__ContentDocumentRecord__c)",
    )
    parser.add_argument(
        "--out",
        default=Path("merged_sitetracker_files.csv"),
        type=Path,
        help="Output merged CSV (SiteTracker rows with file details)",
    )
    parser.add_argument(
        "--out-files-only",
        default=Path("sitetracker_files_only.csv"),
        type=Path,
        help="Output CSV of file details only (deduped)",
    )
    parser.add_argument("--bulk-max-records", type=int, default=50000, help="Bulk results set size (maxRecords)")
    parser.add_argument(
        "--sitetracker-columns",
        default=None,
        help="Optional comma-separated list of SiteTracker columns to keep (reduces output width).",
    )
    parser.add_argument(
        "--encoding",
        default="utf-8-sig",
        help="CSV encoding (default handles UTF-8 with BOM from Excel exports)",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> None:
    """Export Salesforce file metadata and merge into a SiteTracker export.

    Args:
        argv: Optional list of CLI arguments; defaults to ``sys.argv[1:]``.
    """
    args = parse_arguments(argv)

    auth = sf_org_auth(args.alias, args.login_url)
    instance_url = auth["instanceUrl"]
    access_token = auth["accessToken"]

    sitetracker_columns = (
        [column.strip() for column in args.sitetracker_columns.split(",") if column.strip()]
        if args.sitetracker_columns
        else None
    )

    site_tracker_df = read_sitetracker_csv(
        args.sitetracker_csv,
        args.docid_col,
        sitetracker_columns,
        args.encoding,
    )
    docid_set = extract_docids(site_tracker_df, args.docid_col)
    if not docid_set:
        raise SystemExit("No ContentDocumentIds found in the SiteTracker CSV.")

    print(f"SiteTracker rows loaded: {len(site_tracker_df)}")
    print(f"Distinct ContentDocumentIds in SiteTracker: {len(docid_set)}")

    soql = build_contentversion_soql()
    print("Bulk exporting ContentVersion (IsLatest=true)...")
    job_id = bulk2_create_job(instance_url, access_token, args.api_version, soql)
    bulk2_wait_job(instance_url, access_token, args.api_version, job_id)
    content_versions = bulk2_fetch_results(
        instance_url, access_token, args.api_version, job_id, max_records=args.bulk_max_records
    )

    if content_versions.empty:
        raise SystemExit("No ContentVersion rows returned. (Permissions? Query All Files?)")

    print(f"Downloaded ContentVersion rows: {len(content_versions)}")

    content_versions["ContentDocumentId"] = content_versions["ContentDocumentId"].astype(str)
    content_versions["ContentSizeBytes"] = pd.to_numeric(content_versions.get("ContentSize"), errors="coerce")
    content_versions["ContentSizeMB"] = (content_versions["ContentSizeBytes"] / (1024 * 1024)).round(2)

    matched_files = content_versions[content_versions["ContentDocumentId"].isin(docid_set)].copy()
    print(f"Matched ContentVersion rows to SiteTracker docids: {len(matched_files)}")

    matched_files = matched_files.drop_duplicates(subset=["ContentDocumentId"])
    matched_files = add_file_urls(matched_files, instance_url)
    matched_files.to_csv(args.out_files_only, index=False, encoding="utf-8")
    print(f"Wrote file details only: {args.out_files_only}")

    merged = merge_site_tracker_and_files(site_tracker_df, matched_files, args.docid_col, instance_url)
    merged.to_csv(args.out, index=False, encoding="utf-8")
    print(f"Wrote merged output: {args.out}")

    missing_count = merged["ContentSizeBytes"].isna().sum() if "ContentSizeBytes" in merged.columns else None
    if missing_count is not None:
        print(f"Rows with no matched file size (missing/permission/invalid docid): {missing_count}")


if __name__ == "__main__":
    main()
