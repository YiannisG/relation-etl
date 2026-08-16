from __future__ import annotations

import logging
from dataclasses import dataclass, field

logger = logging.getLogger("etl.transform")


# SANITY CHECK
MAX_EXON_LENGTH = 3_000_000


@dataclass
class QuarantineRecord:
    table: str
    record: dict
    reason: str


@dataclass
class TransformResult:
    """Provides a sufficient summary of all transformations"""
    genes: list[dict] = field(default_factory=list)
    transcripts: list[dict] = field(default_factory=list)
    exons: list[dict] = field(default_factory=list)
    quarantine: list[QuarantineRecord] = field(default_factory=list)
    merge_log: list[str] = field(default_factory=list)


def _merge_duplicates(
    records: list[dict], key: str, table: str, result: TransformResult, priority_field: str | None = None,
) -> dict[str, dict]:
    """
    Handle merging of duplicates.
    Where a primary key has already been seen for a table, for fields of that key take the first that is not None as the default choice and quarentine the other.
    If a priority_field (bool) is specified, the primary key entry with this value==True will be used
        ## This is messy - TODO - generalise this concept further to so non-bool fields can be accepted also
    """
    merged: dict[str, dict] = {}
    for rec in records:
        pk = rec.get(key)
        if pk is None:
            result.quarantine.append(
                QuarantineRecord(
                    table=table, record=rec, reason=f"missing primary key '{key}'"
                )
            )
            continue
        if pk not in merged:
            merged[pk] = dict(rec)
            continue
        existing = merged[pk]
        incoming_is_priority = bool(priority_field) and rec.get(priority_field) is True
        existing_is_priority = bool(priority_field) and existing.get(priority_field) is True
        for field_name, value in rec.items():
            if value is None:
                continue
            if existing.get(field_name) is None:
                existing[field_name] = value
            elif existing[field_name] != value:
                if incoming_is_priority and not existing_is_priority:
                    result.merge_log.append(
                        f"{table} {pk}: conflicting '{field_name}' "
                        f"{value!r} kept ({priority_field}=True), "
                        f"{existing[field_name]!r} discarded"
                    )
                    existing[field_name] = value
                else:
                    result.merge_log.append(
                        f"{table} {pk}: conflicting '{field_name}' "
                        f"{existing[field_name]!r} kept, {value!r} discarded"
                    )
    return merged


def _normalize_gene_name(name: str | None) -> str | None:
    """Normalisation of gene name to convert to all upper case."""
    if name is None:
        return None
    return name.strip().upper()


def _valid_exon_coords(start, end) -> tuple[bool, str | None]:
    """Basic validation for exon coordinate properties. Inconsistencies should be quarantined"""
    if not isinstance(start, (int, float)) or not isinstance(end, (int, float)):
        return False, "start/end not numeric"
    if start < 0 or end < 0:
        return False, "negative coordinates"
    if end <= start:
        return False, "end is <= start"
    if (end - start) > MAX_EXON_LENGTH:
        return False, "length exceeds specified maximum length"
    return True, None


def transform(raw: dict[str, list[dict]]) -> TransformResult:
    """
    The order of processing is: 1. Gene, 2. Transcript, 3. exons.
    If a gene is missing a primary key and is dropped, child transcripts (and hence exons) will also be dropped. This minimises fluff in the data that is not useful and keeps things tidy.
    All dropped entities are recorded in the quarantine log for review.
    """
    result = TransformResult()

    # genes
    merged_genes = _merge_duplicates(
        records=raw["genes"], key="gene_id", table="genes", result=result
    )
    for gene in merged_genes.values():
        gene["gene_name"] = _normalize_gene_name(gene.get("gene_name"))
    result.genes = list(merged_genes.values())
    gene_ids = set(merged_genes.keys())

    # transcripts
    merged_transcripts = _merge_duplicates(
        records=raw["transcripts"],
        key="transcript_id",
        table="transcripts",
        result=result,
        priority_field="is_canonical"
    )
    clean_transcripts = {}
    for tx_id, tx in merged_transcripts.items():
        if tx.get("gene_id") not in gene_ids:
            result.quarantine.append(
                QuarantineRecord(
                    table="transcripts",
                    record=tx,
                    reason=f"orphan: gene_id {tx.get('gene_id')!r} not found in genes",
                )
            )
            continue
        clean_transcripts[tx_id] = tx
    result.transcripts = list(clean_transcripts.values())
    transcript_ids = set(clean_transcripts.keys())

    # exons
    merged_exons = _merge_duplicates(
        records=raw["exons"], key="exon_id", table="exons", result=result
    )
    clean_exons = []
    for exon in merged_exons.values():
        ok, reason = _valid_exon_coords(start=exon.get("start"), end=exon.get("end"))
        if not ok:
            result.quarantine.append(
                QuarantineRecord(
                    table="exons", record=exon, reason=f"invalid coordinates: {reason}"
                )
            )
            continue
        if exon.get("transcript_id") not in transcript_ids:
            result.quarantine.append(
                QuarantineRecord(
                    table="exons",
                    record=exon,
                    reason=f"orphan: transcript_id {exon.get('transcript_id')!r} not found in transcripts",
                )
            )
            continue
        clean_exons.append(exon)
    result.exons = clean_exons
    logger.info(
        f"transform complete: {len(result.genes)} genes, {len(result.transcripts)} transcripts, {len(result.exons)} exons kept; "
        f"{len(result.quarantine)} quarantined; {len(result.merge_log)} merge conflicts logged"
    )
    return result
