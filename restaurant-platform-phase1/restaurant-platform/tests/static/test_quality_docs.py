"""
Keeps the quality documents in docs/quality/ internally consistent.

Several of this project's earlier documents drifted from the truth (a wrong
chart layout, a stale cluster status, a wrong disk figure). These documents
hold registers whose whole value is that their numbers and cross-references
can be trusted, so the cross-references and the totals are checked
mechanically: requirement ids exist and are all traced, every test the matrix
cites exists, defect and risk ids are unique and every id cited is real, and
every summary table agrees with the rows it summarises.

    python -m pytest tests/static/test_quality_docs.py -v
"""
import ast
import collections
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from helpers import ROOT  # noqa: E402

QUALITY = ROOT / "docs" / "quality"
DOCS = sorted(p for p in QUALITY.glob("*.md"))
DOC_NAMES = {p.name for p in DOCS}


def read(name):
    return (QUALITY / name).read_text()


def cells(line):
    """Split a Markdown table row on unescaped pipes."""
    return [c.strip() for c in re.split(r"(?<!\\)\|", line.strip())[1:-1]]


REQ_ID = r"(?:CON|FR-[A-Z]{3}|NFR-[A-Z]{3})-\d{2}"


def srs_ids():
    return re.findall(rf"^\| ({REQ_ID}) \|", read("01-requirements-specification.md"), flags=re.M)


def matrix_rows():
    text = read("03-requirements-traceability-matrix.md")
    body = text[text.index("## The matrix"):text.index("## Tests that trace to no requirement")]
    return [cells(line) for line in body.splitlines() if re.match(rf"^\| {REQ_ID} \|", line)]


def defect_rows():
    body = read("06-defect-log.md")
    body = body[body.index("## Part A"):]
    return [cells(line) for line in body.splitlines() if re.match(r"^\| DEF-\d{3} \|", line)]


def risk_rows():
    text = read("05-risk-register.md")
    body = text[:text.index("## Summary")]
    return [cells(line) for line in body.splitlines() if re.match(r"^\| RSK-\d{3} \|", line)]


def all_test_names():
    names = set()
    for path in ROOT.rglob("test_*.py"):
        if "node_modules" in path.parts:
            continue
        names |= {n.name for n in ast.walk(ast.parse(path.read_text())) if isinstance(n, ast.FunctionDef) and n.name.startswith("test_")}
    return names


# ------------------------------------------------------------------ requirements and the matrix

def test_requirement_ids_are_unique_and_well_formed():
    ids = srs_ids()
    assert len(ids) >= 80, f"only {len(ids)} requirements found; the SRS table format may have changed"
    duplicates = [i for i, n in collections.Counter(ids).items() if n > 1]
    assert not duplicates, f"duplicate requirement ids: {duplicates}"


def test_every_requirement_is_traced_in_the_matrix_and_nothing_else_is_cited():
    in_srs, in_matrix = set(srs_ids()), {row[0] for row in matrix_rows()}
    assert in_srs - in_matrix == set(), f"requirements missing from the traceability matrix: {sorted(in_srs - in_matrix)}"
    assert in_matrix - in_srs == set(), f"the matrix cites requirements the SRS does not define: {sorted(in_matrix - in_srs)}"


def test_every_test_cited_in_the_matrix_exists():
    cited = set(re.findall(r"`(test_[a-z0-9_]+)`", read("03-requirements-traceability-matrix.md")))
    missing = cited - all_test_names()
    assert not missing, f"the matrix cites tests that do not exist: {sorted(missing)}"


def test_the_matrix_coverage_summary_matches_its_rows():
    text = read("03-requirements-traceability-matrix.md")
    rows = collections.Counter(row[4] for row in matrix_rows())
    summary = text[text.index("## Coverage summary"):text.index("## The matrix")]
    stated = {cells(line)[0]: int(cells(line)[1]) for line in summary.splitlines() if line.startswith("| ") and cells(line)[1].isdigit()}
    for label, count in rows.items():
        assert stated.get(label) == count, f"summary says {stated.get(label)} for '{label}', rows give {count}"


# ------------------------------------------------------------------ defects

def test_defect_ids_are_unique_and_sequential():
    ids = [row[0] for row in defect_rows()]
    assert ids == [f"DEF-{n:03d}" for n in range(1, len(ids) + 1)], "defect ids must run DEF-001, DEF-002, ... with no gaps or repeats"


def test_the_defect_log_summary_matches_its_entries():
    text = read("06-defect-log.md")
    rows = defect_rows()
    entries = int(re.search(r"\*\*(\d+) entries\*\*", text).group(1))
    assert entries == len(rows), f"the summary says {entries} entries, the tables hold {len(rows)}"
    severity = collections.Counter(row[4] for row in rows)
    status = collections.Counter(row[7] for row in rows)
    summary = text[text.index("### By severity"):text.index("### By part")]
    stated = {cells(line)[0]: int(cells(line)[1]) for line in summary.splitlines() if line.startswith("| ") and cells(line)[1].isdigit()}
    for label, count in {**severity, **status}.items():
        assert stated.get(label) == count, f"summary says {stated.get(label)} for '{label}', entries give {count}"


def test_every_defect_id_cited_anywhere_in_the_quality_documents_exists():
    real = {row[0] for row in defect_rows()}
    for doc in DOCS:
        cited = set(re.findall(r"\bDEF-\d{3}\b", doc.read_text()))
        assert cited <= real, f"{doc.name} cites defects that do not exist: {sorted(cited - real)}"


# ------------------------------------------------------------------ risks

def test_risk_ids_are_unique_and_sequential():
    ids = [row[0] for row in risk_rows()]
    assert ids == [f"RSK-{n:03d}" for n in range(1, len(ids) + 1)]


def test_every_risk_id_cited_in_the_quality_documents_exists():
    real = {row[0] for row in risk_rows()}
    for doc in DOCS:
        cited = set(re.findall(r"\bRSK-\d{3}\b", doc.read_text()))
        assert cited <= real, f"{doc.name} cites risks that do not exist: {sorted(cited - real)}"


# ------------------------------------------------------------------ structure

def test_every_quality_document_has_a_version_and_a_date():
    for doc in DOCS:
        head = doc.read_text()[:1500]
        assert re.search(r"^\| Version \|", head, flags=re.M) and re.search(r"^\| Date \|", head, flags=re.M), f"{doc.name} lacks a version/date control table"


def test_the_index_lists_every_document_and_every_link_resolves():
    index = read("README.md")
    linked = set(re.findall(r"\]\(((?:\d\d-)[a-z0-9-]+\.md)\)", index))
    expected = DOC_NAMES - {"README.md"}
    assert linked == expected, f"index lists {sorted(linked)}; the folder holds {sorted(expected)}"
    for doc in DOCS:
        for target in re.findall(r"\]\(([^)#]+\.md)\)", doc.read_text()):
            if target.startswith(("http", "mailto")):
                continue
            assert (doc.parent / target).resolve().exists(), f"{doc.name} links to {target}, which does not exist"
