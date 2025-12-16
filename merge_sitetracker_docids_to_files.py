#!/usr/bin/env python3
import argparse
import json
import shutil
import subprocess
import time
from io import StringIO
from typing import Dict, Optional, List

import pandas as pd
import requests


# ----------------------------
# Salesforce CLI auth (sf/sfdx)
# ----------------------------

# python merge_sitetracker_docids_to_files.py `
#  --alias myorg `
#  --sitetracker-csv sitetracker_attachments.csv `
#  --docid-col sitetracker__ContentDocumentRecord__c
#
#  Need list of content documentids, then extracts file details, adds to list and saves an output file
#
 








def pick_cli() -> str:
    for cmd in ("sf.cmd", "sf", "sfdx.cmd", "sfdx"):
        if shutil.which(cmd):
            return cmd
    raise FileNotFoundError(
        "Salesforce CLI not found on PATH. Verify `sf --version` works in this terminal."
    )


def run_cmd(cmd: List[str]) -> str:
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, shell=False)
    if p.returncode != 0:
        raise RuntimeError(f"Command failed:\n  {' '.join(cmd)}\n\nSTDERR:\n{p.stderr.strip()}")
    return p.stdout


def sf_org_auth(alias: str, login_url: Optional[str] = None) -> Dict[str, str]:
    cli = pick_cli()

    def org_display() -> Dict:
        if cli.startswith("sf"):
            out = run_cmd([cli, "org", "display", "--json", "--target-org", alias])
        else:
            out = run_cmd([cli, "force:org:display", "--json", "-u", alias])
        return json.loads(out)

    def org_login_web() -> None:
        if cli.startswith("sf"):
            cmd = [cli, "org", "login", "web", "--alias", alias]
            if login_url:
                cmd += ["--instance-url", login_url]
        else:
            cmd = [cli, "force:auth:web:login", "-a", alias]
            if login_url:
                cmd += ["-r", login_url]
        run_cmd(cmd)

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


# ----------------------------
# Bulk API 2.0 Query helpers
# ----------------------------

def bulk2_create_job(instance_url: str, access_token: str, api_version: str, soql: str) -> str:
    url = f"{instance_url}/services/data/v{api_version}/jobs/query"
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
    payload = {
        "operation": "query",
        "query": soql,
        "contentType": "CSV",
        "columnDelimiter": "COMMA",
        "lineEnding": "LF",
    }
    r = requests.post(url, headers=headers, json=payload, timeout=60)
    if r.status_code >= 400:
        raise RuntimeError(
            f"Bulk job creation failed HTTP {r.status_code}:\n{r.text}\nSOQL:\n{soql}"
        )
    return r.json()["id"]


def bulk2_wait_job(instance_url: str, access_token: str, api_version: str, job_id: str) -> Dict:
    url = f"{instance_url}/services/data/v{api_version}/jobs/query/{job_id}"
    headers = {"Authorization": f"Bearer {access_token}"}
    while True:
        r = requests.get(url, headers=headers, timeout=60)
        r.raise_for_status()
        info = r.json()
        state = info.get("state")
        if state in ("JobComplete", "Failed", "Aborted"):
            if state != "JobComplete":
                raise RuntimeError(f"Bulk job ended in state {state}: {info}")
            return info
        time.sleep(3)


def bulk2_fetch_results(instance_url: str, access_token: str, api_version: str, job_id: str, max_records: int = 50000) -> pd.DataFrame:
    """
    Pages through Bulk API 2.0 results using Sforce-Locator and maxRecords.
    """
    url = f"{instance_url}/services/data/v{api_version}/jobs/query/{job_id}/results"
    headers = {"Authorization": f"Bearer {access_token}", "Accept": "text/csv"}

    locator = None
    frames = []

    while True:
        params = {"maxRecords": max_records}
        if locator:
            params["locator"] = locator

        r = requests.get(url, headers=headers, params=params, timeout=300)
        r.raise_for_status()

        if r.text.strip():
            frames.append(pd.read_csv(StringIO(r.text)))

        locator = r.headers.get("Sforce-Locator")
        if not locator or locator.lower() == "null":
            break

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


# ----------------------------
# Main
# ----------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--alias", required=True, help="Salesforce CLI org alias")
    ap.add_argument("--login-url", default=None, help="Optional: https://login.salesforce.com or https://test.salesforce.com")
    ap.add_argument("--api-version", default="60.0", help="Salesforce API version")

    ap.add_argument("--sitetracker-csv", required=True, help="CSV exported from sitetracker__Attachment__c")
    ap.add_argument("--docid-col", required=True, help="Column name holding ContentDocumentId (e.g. sitetracker__ContentDocumentRecord__c)")

    ap.add_argument("--out", default="merged_sitetracker_files.csv", help="Output merged CSV (SiteTracker rows with file details)")
    ap.add_argument("--out-files-only", default="sitetracker_files_only.csv", help="Output CSV of file details only (deduped)")

    ap.add_argument("--bulk-max-records", type=int, default=50000, help="Bulk results set size (maxRecords)")
    ap.add_argument("--sitetracker-columns", default=None,
                    help="Optional comma-separated list of SiteTracker columns to keep (reduces output width).")
    args = ap.parse_args()

    # Auth
    auth = sf_org_auth(args.alias, args.login_url)
    instance_url = auth["instanceUrl"]
    access_token = auth["accessToken"]

    # Load SiteTracker CSV
    st = pd.read_csv(args.sitetracker_csv, dtype=str, low_memory=False)
    if args.docid_col not in st.columns:
        raise SystemExit(f"Column not found in SiteTracker CSV: {args.docid_col}")

    # Optionally reduce columns
    if args.sitetracker_columns:
        keep_cols = [c.strip() for c in args.sitetracker_columns.split(",") if c.strip()]
        keep_cols = [c for c in keep_cols if c in st.columns]
        # always keep the docid col
        if args.docid_col not in keep_cols:
            keep_cols.append(args.docid_col)
        st = st[keep_cols]

    # Clean / dedupe doc ids
    st_docids = st[args.docid_col].dropna().astype(str)
    st_docids = st_docids[st_docids.str.len() > 0]
    docid_set = set(st_docids.unique().tolist())
    print(f"SiteTracker rows loaded: {len(st)}")
    print(f"Distinct ContentDocumentIds in SiteTracker: {len(docid_set)}")

    # Bulk export ALL latest ContentVersion (org-wide visibility depends on your perms)
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
    soql = " ".join(soql.split())

    print("Bulk exporting ContentVersion (IsLatest=true)...")
    job = bulk2_create_job(instance_url, access_token, args.api_version, soql)
    bulk2_wait_job(instance_url, access_token, args.api_version, job)
    cv = bulk2_fetch_results(instance_url, access_token, args.api_version, job, max_records=args.bulk_max_records)

    if cv.empty:
        raise SystemExit("No ContentVersion rows returned. (Permissions? Query All Files?)")

    print(f"Downloaded ContentVersion rows: {len(cv)}")

    # Ensure types
    cv["ContentDocumentId"] = cv["ContentDocumentId"].astype(str)
    cv["ContentSizeBytes"] = pd.to_numeric(cv.get("ContentSize"), errors="coerce")
    cv["ContentSizeMB"] = (cv["ContentSizeBytes"] / (1024 * 1024)).round(2)

    # Filter to SiteTracker doc ids
    cv_st = cv[cv["ContentDocumentId"].isin(docid_set)].copy()
    print(f"Matched ContentVersion rows to SiteTracker docids: {len(cv_st)}")

    # Dedup file details (should already be 1 row per doc because IsLatest=true)
    cv_st = cv_st.drop_duplicates(subset=["ContentDocumentId"])

    # Write files-only
    # Add clickable URL (optional but handy)
    cv_st["FileUrl"] = cv_st["ContentDocumentId"].apply(
        lambda x: f"{instance_url}/lightning/r/ContentDocument/{x}/view"
    )
    cv_st.to_csv(args.out_files_only, index=False, encoding="utf-8")
    print(f"Wrote file details only: {args.out_files_only}")

    # Merge back onto SiteTracker rows (can be big)
    merged = st.merge(
        cv_st,
        how="left",
        left_on=args.docid_col,
        right_on="ContentDocumentId",
        suffixes=("", "_File")
    )

    # Optional record URL for the SiteTracker attachment row (if Id exists)
    if "Id" in merged.columns:
        merged["SiteTrackerAttachmentUrl"] = merged["Id"].apply(
            lambda x: f"{instance_url}/lightning/r/sitetracker__Attachment__c/{x}/view" if pd.notna(x) else pd.NA
        )

    merged.to_csv(args.out, index=False, encoding="utf-8")
    print(f"Wrote merged output: {args.out}")

    # Quick stats
    missing = merged["ContentSizeBytes"].isna().sum() if "ContentSizeBytes" in merged.columns else None
    if missing is not None:
        print(f"Rows with no matched file size (missing/permission/invalid docid): {missing}")


if __name__ == "__main__":
    main()
