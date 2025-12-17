import pandas as pd
from sitetracker_file_exporter import (
    add_file_urls,
    extract_docids,
    merge_site_tracker_and_files,
    read_sitetracker_csv,
)


def test_read_sitetracker_csv_filters_and_keeps_docid(tmp_path):
    csv_path = tmp_path / "attachments.csv"
    data = pd.DataFrame(
        {
            "sitetracker__ContentDocumentRecord__c": ["DOC1", "DOC2"],
            "Name": ["A", "B"],
            "Other": ["X", "Y"],
        }
    )
    data.to_csv(csv_path, index=False)

    filtered = read_sitetracker_csv(
        csv_path,
        "sitetracker__ContentDocumentRecord__c",
        ["Name"],
        encoding="utf-8",
    )

    assert list(filtered.columns) == ["Name", "sitetracker__ContentDocumentRecord__c"]


def test_extract_docids_skips_blank_and_duplicates():
    df = pd.DataFrame(
        {
            "DocId": ["123", "", None, "123", "456"],
        }
    )
    assert extract_docids(df, "DocId") == {"123", "456"}


def test_merge_site_tracker_and_files_adds_urls():
    site_tracker_df = pd.DataFrame(
        {
            "Id": ["ATT1"],
            "DocId": ["DOC1"],
        }
    )
    files_df = pd.DataFrame(
        {
            "ContentDocumentId": ["DOC1"],
            "Title": ["File 1"],
            "FileUrl": ["http://example.com/DOC1"],
        }
    )

    merged = merge_site_tracker_and_files(
        site_tracker_df,
        files_df,
        "DocId",
        "http://example.com",
    )

    assert merged.loc[0, "Title"] == "File 1"
    assert (
        merged.loc[0, "SiteTrackerAttachmentUrl"]
        == "http://example.com/lightning/r/sitetracker__Attachment__c/ATT1/view"
    )


def test_add_file_urls_appends_lightning_url():
    df = pd.DataFrame({"ContentDocumentId": ["DOC1"]})
    result = add_file_urls(df, "http://example.com")
    assert result.loc[0, "FileUrl"] == "http://example.com/lightning/r/ContentDocument/DOC1/view"
