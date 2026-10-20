"""Cleaning step.

On top of the v3 rules it handles the newer, v4-style exports (the ones shaped
like ``sisalv4_v3_to_v4_changes.csv``), which repeat their join keys and their
whole entity block across several header sections.  The v4-specific
differences, all of them requested:

* ``composite_entity_id`` (and its v4 partner ``single_entity_id``) are dropped.
* ``entity_scan`` is kept as a column and is treated like the other "yes/no"
  asset flags, so it can appear in ``external_description``.
* ``depth_dating`` is folded into ``depth``.
* ``dating_lamina_id`` is kept.

Usage
-----
    python src/clean_data_all.py data/not_validated_files/sisalv4_v3_to_v4_changes.csv
    python src/clean_data_all.py 'data/original/*.csv' -o data/cleaned
"""

from __future__ import annotations

import argparse
import glob
import re
from pathlib import Path

import numpy as np
import pandas as pd

UNIT_LIST = Path("src/unitList_SISALv3.xlsx")

# Column names carrying no information downstream.
DROP_COLUMNS = ["composite_entity_id", "single_entity_id"]

# Separator SISAL uses when several site notes are concatenated into one cell.
NOTES_SEPARATOR = " \\ "

# Flags whose "yes" value is reported in external_description.  entity_scan is
# new in v4 and sits next to cave_map, the other asset-style flag.
# (Frozen counterpart: the inline `cols` list in clean_data2.py, without
# entity_scan.  Edit here only.)
EXTERNAL_DESCRIPTION_FLAGS = [
    "host_rock_trace_elements",
    "drip_water_trace_elements",
    "d13C",
    "d18O",
    "wokam",
    "organics",
    "fluid_inclusions",
    "mineralogy_petrology_fabric",
    "clumped_isotopes",
    "noble_gas_temperatures",
    "C14",
    "ODL",
    "Sr_Ca",
    "cave_map",
    "entity_scan",
    "trace_elements_datafile",
    "trace_elements_metadatafile",
]

# Contact strings that survive the automatic "First Last" -> "Last, First"
# rewrite in a broken state.  Merges the two `names` dicts of the frozen
# clean_data4.py and clean_data5.py; edit here only.
CONTACT_FIXES = {
    "Hatvani, Istvan Gabor Gabor": "Hatvani, Istvan Gabor",
    "Nikita Kaushal": "Kaushal, Nikita",
    "Carlos Pérez-Mejías": "Pérez-Mejías, Carlos",
    "Juan Luis Bernal Wormull": "Bernal Wormull, Juan Luis",
    "Marcela Eduarda Della Libera de Godoy": "Della Libera de Godoy, Marcela Eduarda",
    "Monika Markhowska": "Markowska, Monika",
    "Pauline Treble": "Treble, Pauline",
    "Monika Markowska": "Markowska, Monika",
    "Andy Baker": "Baker, Andy",
    "Natasha Sekhon": "Sekhon, Natasha",
    "Monica Arienzo": "Arienzo, Monica",
    "Nicolás M. Stríkis": "Stríkis, Nicolás M.",
    "Vitor Azevedo": "Azevedo, Vitor",
    "Jennifer B. Wurtzel": "Wurtzel, Jennifer B.",
    "Markowska, Carol Tadros/Monika": "Markowska, Monika | Tadros, Carol V.",
    "McDonough, Treble, Pauline/Liza": "Treble, Pauline | McDonough, Liza",
    "Wolf, Vanessa Skiba/Annabel": "Skiba, Vanessa | Wolf, Annabel",
}

# pandas appends .1, .2, ... to repeated header names on read.
MANGLED_SUFFIX = re.compile(r"^(?P<base>.+)\.(?P<n>\d+)$")

AGE_TYPE = "Calibrated radiocarbon years BP"

# Age columns whose _uncert_pos / _uncert_neg are turned into age bounds.
# (Frozen counterpart: the inline `columns` list in clean_data6.py.)
AGE_COLUMNS = [
    "corr_age",
    "lam_age",
    "interp_age",
    "lin_interp_age",
    "lin_reg_age",
    "Bchron_age",
    "Bacon_age",
    "OxCal_age",
    "copRa_age",
    "StalAge_age",
]

# SISAL date_type -> ndb.geochrontypes.geochrontype.  Anything not listed
# (cross-dating, events, "other", "unknown") is "Other".
GEOCHRON_TYPES = {
    "C14": "Carbon-14",
    "MC-ICP-MS U/Th": "Uranium series",
    "ICP-MS U/Th Other": "Uranium series",
    "Alpha U/Th": "Uranium series",
    "TIMS": "Uranium series",
    "U/Th unspecified": "Uranium series",
    "Multiple methods": "Multiple methods",
}
GEOCHRON_TYPE_DEFAULT = "Other"


# --------------------------------------------------------------------------
# reshaping a v4-style export into the v3 layout
# --------------------------------------------------------------------------
def _values_agree(left: pd.Series, right: pd.Series) -> pd.Series:
    """Elementwise equality that ignores int/float and whitespace differences."""
    num_left = pd.to_numeric(left, errors="coerce")
    num_right = pd.to_numeric(right, errors="coerce")
    both_numeric = num_left.notna() & num_right.notna()

    agree = pd.Series(True, index=left.index)
    agree[both_numeric] = np.isclose(num_left[both_numeric], num_right[both_numeric])

    textual = left.notna() & right.notna() & ~both_numeric
    agree[textual] = (
        left[textual].astype(str).str.strip() == right[textual].astype(str).str.strip()
    )
    return agree


def collapse_repeated_columns(data: pd.DataFrame) -> pd.DataFrame:
    """Drop the repeated header blocks of a v4-style export.

    These exports join the SISAL tables so widely that the join keys and the
    whole entity block appear several times.  Every repeat holds the same
    values as its first occurrence (or nothing at all), so only the first is
    kept.  Values present only in a repeat are copied over first.
    """
    for column in list(data.columns):
        match = MANGLED_SUFFIX.match(str(column))
        if match is None:
            continue
        base = match.group("base")
        if base not in data.columns:
            continue

        repeat, first = data[column], data[base]
        conflicting = (repeat.notna() & first.notna() & ~_values_agree(repeat, first)).sum()
        if conflicting:
            print(f"  warning: {column} disagrees with {base} on {conflicting} rows; keeping {base}")

        fill_from_repeat = first.isna() & repeat.notna()
        if fill_from_repeat.any():
            data.loc[fill_from_repeat, base] = repeat[fill_from_repeat]
        data = data.drop(columns=[column])
    return data


def merge_notes(data: pd.DataFrame) -> pd.DataFrame:
    """Fold several site notes into the single cell the v3 exports carried.

    A site with three notes is returned by the v4 export as three rows per
    sample, which is the only reason those files are three times too long.
    """
    if "notes" not in data.columns or "site_id" not in data.columns:
        return data

    def join(group: pd.Series):
        seen = group.dropna().astype(str).str.strip()
        seen = seen[seen != ""].unique()
        if len(seen) == 0:
            return np.nan
        return NOTES_SEPARATOR.join(seen)

    data["notes"] = data.groupby("site_id")["notes"].transform(join)
    return data


def harmonize_depth(data: pd.DataFrame) -> pd.DataFrame:
    """Make ``depth`` the single depth column, as in v3.

    v4 exports carry both: ``depth`` is already the coalesced depth (the
    dating depth on dating rows, the sample depth on sample rows) and
    ``depth_dating`` repeats the dating half of it.  Folding it in before the
    units are attached is what keeps ``depth_units`` out of the output, since
    the unit list names the variable ``depth_dating``.
    """
    if "depth_dating" not in data.columns:
        return data
    if "depth" not in data.columns:
        return data.rename(columns={"depth_dating": "depth"})

    data["depth"] = data["depth"].fillna(data["depth_dating"])
    return data.drop(columns=["depth_dating"])


def restore_v3_column_order(data: pd.DataFrame) -> pd.DataFrame:
    """Put ``sample_id`` back where the v3 exports listed it.

    v3 carried one ``sample_id`` column, right after ``dating_id``.  v4 repeats
    it once per measurement table, so its first occurrence, the one kept by
    ``collapse_repeated_columns``, lands much later, in the sample block.
    """
    if "sample_id" not in data.columns or "dating_id" not in data.columns:
        return data

    columns = list(data.columns)
    columns.remove("sample_id")
    columns.insert(columns.index("dating_id") + 1, "sample_id")
    return data[columns]


# --------------------------------------------------------------------------
# contacts, duplicates, references, units
# --------------------------------------------------------------------------
def fix_names(name):
    """Rewrite "First Last" contacts as "Last, First"; leave the rest alone.

    Live definition.  The frozen clean_data.py holds an identical copy without
    the non-string guard; that copy is history and is not maintained.
    """
    if not isinstance(name, str):
        return name

    def format_name(part):
        parts = part.split()
        if len(parts) == 2:
            return f"{parts[1]}, {parts[0]}"
        if len(parts) == 3:
            return f"{parts[2]}, {parts[0]} {parts[1]}"
        return part

    pieces = name.split(" | ")
    if len(pieces) > 1:
        return " | ".join(format_name(part) for part in pieces)
    return format_name(name)


def drop_duplicate_rows(data: pd.DataFrame) -> pd.DataFrame:
    """Deduplicate treating NaN as a value, then restore the NaNs."""
    filled = data.fillna("FFILL").drop_duplicates()
    restored = filled.copy()
    restored[restored == "FFILL"] = np.nan
    return restored.reset_index(drop=True)


def aggregate_citations(citations: pd.DataFrame, key: str) -> pd.DataFrame:
    """One row per ``key``, with its references concatenated."""
    citations = citations[[key, "ref_id", "publication_DOI", "citation"]].drop_duplicates()
    try:
        return citations.groupby(key).agg(
            {
                "ref_id": lambda x: ", ".join(x.astype(str)),
                "publication_DOI": ", ".join,
                "citation": " | ".join,
            }
        ).reset_index()
    except TypeError:
        # At least one group has a missing DOI or citation.
        return citations.groupby(key).agg(
            {
                "ref_id": lambda x: ", ".join(map(str, x.dropna())),
                "publication_DOI": lambda x: ", ".join(map(str, x.dropna())),
                "citation": lambda x: " | ".join(map(str, x.dropna())),
            }
        ).reset_index()


def attach_citations(data: pd.DataFrame, references: pd.DataFrame) -> pd.DataFrame:
    """Merge the references back on, per sample, then per entity.

    The per-sample merge leaves the dating rows empty, because a dating row
    carries no ``sample_id``.  SISAL references are entity-level, so those rows
    fall back to their entity's references.  On a v3 file every row is a sample
    row and the fallback does nothing.
    """
    data = data.merge(aggregate_citations(references, "sample_id"), on="sample_id", how="left")

    missing = data["ref_id"].isna()
    if missing.any() and "entity_id" in references.columns:
        by_entity = aggregate_citations(references, "entity_id").set_index("entity_id")
        for column in ("ref_id", "publication_DOI", "citation"):
            data.loc[missing, column] = data.loc[missing, "entity_id"].map(by_entity[column])
    return data


def add_units(data: pd.DataFrame, units: pd.DataFrame) -> pd.DataFrame:
    """Append a ``<variable>_units`` column for every variable in the unit list."""
    for variable in units.index:
        if variable in data.columns:
            data[f"{variable}_units"] = units.loc[variable, "unit"]
    return data


def normalize_numbers(value):
    """Sort and deduplicate a comma-separated list of reference ids.

    Live definition.  The frozen clean_data2.py holds an identical copy with a
    narrower sort key; that copy is history and is not maintained.
    """
    if pd.isna(value):
        return value
    tokens = [token.strip() for token in str(value).split(",")]
    tokens = [token for token in tokens if token and token.lower() != "nan"]
    try:
        # int() of the float form: a v4 export whose ref_id column holds NaNs
        # is read as float, so the ids arrive as "563.0".
        return ", ".join(str(n) for n in sorted({int(float(t)) for t in tokens}))
    except ValueError:
        return ", ".join(sorted(set(tokens)))


def add_reference_columns(data: pd.DataFrame) -> pd.DataFrame:
    """Tidy ``ref_id`` and derive ``doi_database`` and ``external_description``."""
    data["ref_id"] = data["ref_id"].apply(normalize_numbers)
    data["doi_database"] = data["data_DOI_URL"].apply(
        lambda x: "NOAA" if isinstance(x, str) and "NOAA" in x.upper() else None
    )
    flags = [col for col in EXTERNAL_DESCRIPTION_FLAGS if col in data.columns]
    data["external_description"] = data.apply(
        lambda row: ", ".join(
            col for col in flags if str(row[col]).strip().lower() == "yes"
        ),
        axis=1,
    )
    return data


def clean_controlled_vocabulary(data: pd.DataFrame) -> pd.DataFrame:
    """Normalise the free-ish text fields onto the values Neotoma accepts."""
    note_sheet_columns = ["land_use", "rock_age", "vegetation_type"]
    blank_filled = ["rock_age", "geology", "speleothem_type", "decay_constant", "date_type"]

    present = [col for col in blank_filled if col in data.columns]
    data[present] = data[present].fillna("")

    if "decay_constant" in data.columns:
        data["decay_constant"] = data["decay_constant"].str.strip()

    if "rock_age" in data.columns:
        data["rock_age"] = data["rock_age"].str.strip().str.replace("unknown", "", regex=False)

    if "geology" in data.columns:
        data["geology"] = data["geology"].apply(
            lambda x: "Dolomite limestone"
            if isinstance(x, str) and "dolomite" in x.lower() and "limestone" in x.lower()
            else x
        )
        data["geology"] = data["geology"].apply(
            lambda x: "Dolostone (dolomite)"
            if isinstance(x, str) and "dolomite" in x.lower() and "limestone" not in x.lower()
            else x
        )
        data["geology"] = (
            data["geology"]
            .str.replace("mixed (see notes)", "mixed", regex=False)
            .str.replace("other (see notes)", "other", regex=False)
            .str.strip()
        )

    if "speleothem_type" in data.columns:
        data["speleothem_type"] = (
            data["speleothem_type"]
            .str.replace("other (see notes)", "other", regex=False)
            .str.strip()
        )

    for column in note_sheet_columns:
        if column not in data.columns:
            continue
        data[column] = (
            data[column]
            .str.replace("other (see notes)", "other (add note to notes sheet)", regex=False)
            .str.replace("mixed (see notes)", "mixed (add note to notes sheet)", regex=False)
            .str.strip()
        )
    return data


def add_geochron_type(data: pd.DataFrame) -> pd.DataFrame:
    """Derive ``geochron_type`` (a Neotoma geochron type name) from ``date_type``."""
    if "date_type" not in data.columns or "geochron_type" in data.columns:
        return data

    def to_neotoma(value):
        if pd.isna(value) or str(value).strip() == "":
            return ""
        return GEOCHRON_TYPES.get(str(value).strip(), GEOCHRON_TYPE_DEFAULT)

    data.insert(
        data.columns.get_loc("date_type") + 1,
        "geochron_type",
        data["date_type"].apply(to_neotoma),
    )
    return data


def add_age_bounds(data: pd.DataFrame) -> pd.DataFrame:
    """Turn the uncertainties into age bounds, as clean_data6.py did.

    For an age of 10 with uncert_pos = 2 and uncert_neg = 4 the younger bound
    is 10 - 4 = 6 and the older bound is 10 + 2 = 12.  The raw uncertainties
    are kept in ``*_copy`` columns, which also mark a column as already done,
    so running this twice does not apply it twice.
    """
    copies = {}
    for age in AGE_COLUMNS:
        pos, neg = f"{age}_uncert_pos", f"{age}_uncert_neg"
        if not {age, pos, neg} <= set(data.columns) or f"{pos}_copy" in data.columns:
            continue
        copies[f"{pos}_copy"] = data[pos].copy()
        copies[f"{neg}_copy"] = data[neg].copy()
        age_values = pd.to_numeric(data[age], errors="coerce")
        data[pos] = age_values + pd.to_numeric(data[pos], errors="coerce")
        data[neg] = age_values - pd.to_numeric(data[neg], errors="coerce")
    return pd.concat([data, pd.DataFrame(copies)], axis=1)


def add_sisal_ids(data: pd.DataFrame) -> pd.DataFrame:
    """Rename the SISAL keys and add ``collunit_notes``, as clean_data7.py did.

    Runs last because the reference merge above still needs ``entity_id``.
    """
    data = data.rename(columns={"site_id": "sisal_site_id", "entity_id": "sisal_entity_id"})
    if "collunit_notes" not in data.columns and {"sisal_site_id", "sisal_entity_id"} <= set(data.columns):
        data["collunit_notes"] = (
            "SISAL_SiteID: " + data["sisal_site_id"].astype(str)
            + "; SISAL_EntityID: " + data["sisal_entity_id"].astype(str)
        )
    return data


# --------------------------------------------------------------------------
# pipeline
# --------------------------------------------------------------------------
def clean(data: pd.DataFrame, units: pd.DataFrame) -> pd.DataFrame:
    data = collapse_repeated_columns(data)
    data = merge_notes(data)
    data = harmonize_depth(data)
    data = restore_v3_column_order(data)
    data = data.drop(columns=[c for c in DROP_COLUMNS if c in data.columns])
    data = drop_duplicate_rows(data)

    references = data[["entity_id", "sample_id", "ref_id", "publication_DOI", "citation"]]
    data = data.drop(columns=["ref_id", "publication_DOI", "citation"])

    data["agetype"] = AGE_TYPE

    # ";" and "," both separate contacts; normalise before reformatting so a
    # multi-contact cell is split the same way regardless of which was used.
    data["contact"] = (
        data["contact"].str.replace(";", " |", regex=False).str.replace(",", " |", regex=False)
    )
    data["contact"] = data["contact"].apply(fix_names)

    data = drop_duplicate_rows(data)

    data = add_units(data, units)
    data = attach_citations(data, references)

    data = add_reference_columns(data)
    data = clean_controlled_vocabulary(data)

    for wrong, right in CONTACT_FIXES.items():
        data["contact"] = data["contact"].str.replace(wrong, right, regex=False)

    data = add_geochron_type(data)
    data = add_age_bounds(data)
    data = add_sisal_ids(data)

    return data


def clean_file(path: Path, output_dir: Path, units: pd.DataFrame) -> Path:
    print(path)
    data = pd.read_csv(path, low_memory=False)
    cleaned = clean(data, units)
    output_dir.mkdir(parents=True, exist_ok=True)
    destination = output_dir / path.name
    cleaned.to_csv(destination, index=False)
    print(
        f"  {len(data)} rows, {data.shape[1]} cols -> "
        f"{len(cleaned)} rows, {cleaned.shape[1]} cols"
    )
    return destination


def main() -> None:
    parser = argparse.ArgumentParser(description="Clean a SISAL entity export.")
    parser.add_argument("inputs", nargs="+", help="CSV files or glob patterns")
    parser.add_argument("-o", "--output-dir", default="data/cleaned", type=Path)
    parser.add_argument("--unit-list", default=UNIT_LIST, type=Path)
    args = parser.parse_args()

    units = pd.read_excel(args.unit_list, index_col="variable name")

    paths = []
    for pattern in args.inputs:
        matches = sorted(glob.glob(pattern))
        paths.extend(Path(m) for m in matches) if matches else paths.append(Path(pattern))

    for path in paths:
        clean_file(path, args.output_dir, units)

    print("Finished!")


if __name__ == "__main__":
    main()
