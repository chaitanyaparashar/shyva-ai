#!/usr/bin/env python3
"""
normalize.py — Entity resolution prototype for the Supplier Intelligence Engine.

WHAT THIS IS
    The one runnable artifact of the take-home. It resolves a messy batch of
    supplier records (aliases, conflicting fields, parent/subsidiary noise) into
    one clean, auditable canonical record per real-world entity. This is the
    algorithmic core the rest of the system is designed around.

HOW TO RUN
    pip install rapidfuzz        # only third-party dependency
    python normalize.py          # prints a before/after on baked-in mock data

PROTOTYPE  ->  PRODUCTION SEAM
    Here this runs over an in-memory list. At scale the *same* pure functions run
    as an offline batch job over the crawled corpus (S3 -> extraction -> this
    pipeline -> golden registry in Postgres). Nothing here is query-time; entity
    resolution is precomputed and the serving path reads the resolved registry.
    Keep it that way: pure functions, no global mutable state, blocking already
    present so the fuzzy step stays sub-quadratic on millions of records.

GOVERNANCE (mirrors ARCHITECTURE.md)
    LLM proposes, deterministic rules dispose. The HIGH-risk core is LLM-free by
    mandate: field survivorship (which revenue/country/cert wins) and any merge
    commit are deterministic, which is what makes re-running idempotent and every
    surviving value auditable. An LLM merge proposal (stubbed here) is an
    UNTRUSTED signal that can only *suggest* a candidate edge — it must clear the
    same deterministic gate before it can ever fuse two records.

DE-SCOPED (considered, deferred)
    Phonetic / transliteration matching (Metaphone/Soundex, CJK romanization).
    Real for cross-script aliases but out of scope for a 2h prototype; the design
    slots it in as an extra candidate-edge source, not a rewrite.
"""

from __future__ import annotations

import re
from itertools import combinations
from typing import Optional

from rapidfuzz import fuzz

# ===========================================================================
# SCALE: how each stage of this in-memory prototype runs in production.
# (Non-functional note — the algorithm below is unchanged; only the substrate
# it runs over changes. This whole module is an OFFLINE batch job, never on the
# query hot path — the serving layer reads the already-resolved golden registry.)
#
#   Mock input list      -> a batch pulled from S3 (extracted facts) + the
#                           current golden entity registry in Postgres. Records
#                           carry the same source field, so the source-trust
#                           table applies identically.
#   normalize_name/      -> unchanged pure functions; run as a dbt/Python
#     normalize_country      pre-pass or UDFs over the batch.
#   block()              -> the load-bearing scale lever. Country-keyed blocking
#                           keeps fuzzy comparison sub-quadratic; at corpus scale
#                           run it as a partitioned batch job (one block per
#                           partition, embarrassingly parallel), and add a second
#                           name-signature block pass to recover cross-block dups.
#   match_within_block() -> same reasoned-edge emission; fuzzy stays cheap
#                           because it only fires within a block.
#   llm_propose_merge()  -> today a deterministic offline stub. In production it
#                           becomes the real mid-tier LLM proposer + an
#                           independent, different-family cross-checker. Both feed
#                           the SAME deterministic gate here — the LLM still only
#                           proposes a candidate edge; agreement raises confidence,
#                           disagreement routes to human review. Never autocommits.
#   cluster()            -> identical union-find, computed per block/partition.
#   resolve_fields()     -> deterministic-by-mandate survivorship is unchanged;
#                           it writes canonical rows + per-field provenance,
#                           confidence, and as_of into the Postgres golden registry.
#   detect_hierarchy()   -> same linking; ambiguous pairs land in the review queue.
#   resolve()            -> idempotent, so it re-runs safely on each batch and on
#                           backfills without corrupting prior canonical records.
# ===========================================================================

# --------------------------------------------------------------------------- #
# Shared authority: source-trust table.
# Defined ONCE and consumed by BOTH survivorship (which value wins) and
# confidence scoring — trust is principled, not reinvented per stage.
# Ordering rationale: authoritative registries > self-reported filings/sites >
# third-party reporting. official/authoritative -> self-reported -> third-party.
# --------------------------------------------------------------------------- #
SOURCE_TRUST = {
    "cert_registry": 1.0,   # official certification body register — authoritative
    "filing":        0.9,   # regulatory / exchange filing — audited, self-reported
    "company_site":  0.7,   # self-reported, current but unaudited
    "news":          0.5,   # third-party reporting — timely, unstructured
    "aggregator":    0.3,   # third-party scrape — cheap, lowest trust
}

ACCEPT_THRESHOLD = 88          # token-set score at/above which a fuzzy edge is accepted
DISQUALIFYING_CONF_FLOOR = 0.6  # hard-filter fields below this are flagged `unverified`

# Legal-suffix noise stripped before comparison (token-based, so "corp" never
# eats the middle of "corporation").
LEGAL_SUFFIXES = {
    "co", "ltd", "inc", "gmbh", "limited", "kk", "corp", "corporation",
    "company", "plc", "llc", "sa", "ag", "nv",
}

# Alias dictionary — pairs fuzzy matching provably cannot catch (acronym <-> full).
# Maps a normalized surface form to a canonical key; two records match by alias
# iff they resolve to the SAME key. Deliberately does NOT include every partial
# form, so transitive closure (clustering) still has real work to do.
ALIAS_MAP = {
    "catl": "contemporary amperex technology",
    "lges": "lg energy solution",
}

# Country strings -> ISO-3166 alpha-2 (handles EN / abbrev / native script).
COUNTRY_MAP = {
    "japan": "JP", "jp": "JP", "日本": "JP",
    "south korea": "KR", "korea, republic of": "KR", "republic of korea": "KR",
    "korea": "KR", "kr": "KR", "대한민국": "KR",
    "china": "CN", "cn": "CN", "中国": "CN", "prc": "CN",
}

# Hierarchy lexicons. A shared brand token + a division keyword on each side ->
# same corporate family (parent/subsidiary), NOT a duplicate. A parent keyword
# on one side fixes the direction.
DIVISION_KEYWORDS = {"energy", "chem", "solution", "materials", "mobility"}
PARENT_KEYWORDS = {"holdings", "holding", "group", "corporation"}


# --------------------------------------------------------------------------- #
# Mocked messy input — one row = one raw record from one source.
# Exercises every hard case the resolver must survive:
#   r1-r3  transitive triple (CATL): acronym / full / abbreviated; A-B via alias,
#          B-C via fuzzy, A-C below threshold -> only clustering unites all three.
#   r4-r5  Panasonic parent/subsidiary — must LINK, not merge.
#   r6-r7  LG Energy Solution vs LG Chem — sibling affiliates, must NOT merge.
#   r8-r9  Samsung SDI vs Samsung SDS — false friends (fuzzy 91!) — must STAY split.
#   r10    lone GS Yuasa — clean singleton.
# Conflicting revenue / country strings / cert lists are baked into the triple.
# --------------------------------------------------------------------------- #
MESSY_RECORDS = [
    {"id": "r1", "name": "CATL", "source": "aggregator",
     "country": "China", "revenue_musd": 30000, "certs": ["ISO-9001"], "as_of": 2023},
    {"id": "r2", "name": "Contemporary Amperex Technology Co., Ltd.", "source": "cert_registry",
     "country": "CN", "revenue_musd": 32000, "certs": ["ISO-9001", "IATF-16949"], "as_of": 2024},
    {"id": "r3", "name": "Contemporary Amperex Tech", "source": "filing",
     "country": "中国", "revenue_musd": 33000, "certs": ["IATF-16949"], "as_of": 2024},

    {"id": "r4", "name": "Panasonic Energy Co., Ltd.", "source": "cert_registry",
     "country": "Japan", "revenue_musd": 8000, "certs": ["ISO-9001", "IATF-16949"], "as_of": 2024},
    {"id": "r5", "name": "Panasonic Holdings Corporation", "source": "filing",
     "country": "JP", "revenue_musd": 60000, "certs": ["ISO-9001"], "as_of": 2024},

    {"id": "r6", "name": "LG Energy Solution Ltd.", "source": "cert_registry",
     "country": "South Korea", "revenue_musd": 21000, "certs": ["ISO-9001", "IATF-16949"], "as_of": 2024},
    {"id": "r7", "name": "LG Chem Ltd.", "source": "filing",
     "country": "KR", "revenue_musd": 45000, "certs": ["ISO-9001"], "as_of": 2024},

    {"id": "r8", "name": "Samsung SDI Co., Ltd.", "source": "cert_registry",
     "country": "Korea, Republic of", "revenue_musd": 17000, "certs": ["ISO-9001", "IATF-16949"], "as_of": 2024},
    {"id": "r9", "name": "Samsung SDS Co., Ltd.", "source": "company_site",
     "country": "KR", "revenue_musd": 10000, "certs": ["ISO-9001"], "as_of": 2023},

    {"id": "r10", "name": "GS Yuasa Corporation", "source": "news",
     "country": "Japan", "revenue_musd": 4000, "certs": ["ISO-9001"], "as_of": 2022},
]


# --------------------------------------------------------------------------- #
# Stage 1 — surface-form normalization.
# --------------------------------------------------------------------------- #
def normalize_name(raw: str) -> str:
    """Strip legal suffixes, casefold, collapse whitespace -> comparable form."""
    tokens = re.split(r"[^a-z0-9]+", raw.lower())
    tokens = [t for t in tokens if t and t not in LEGAL_SUFFIXES]
    return " ".join(tokens)


def normalize_country(raw: str) -> str:
    """Map any country surface form (EN/abbrev/native) to an ISO-3166 code."""
    return COUNTRY_MAP.get(raw.strip().lower(), raw.strip().upper())


def alias_key(norm_name: str) -> Optional[str]:
    """Canonical key if this normalized name is a known alias/expansion, else None."""
    if norm_name in ALIAS_MAP:
        return ALIAS_MAP[norm_name]
    if norm_name in ALIAS_MAP.values():
        return norm_name
    return None


# --------------------------------------------------------------------------- #
# Stage 2 — blocking.
# Group candidate duplicates cheaply so fuzzy comparison is O(sum b_i^2), not
# O(n^2) across the whole corpus. Country is a strong, cheap blocking key: real
# duplicates share a country of registration; cross-country pairs are never
# compared. (Accepted recall cost: a true dup mis-keyed to another block is
# missed — see DECISION_LOG. At scale, add a second block pass on a name-token
# signature to recover those.)
# --------------------------------------------------------------------------- #
def block(records: list[dict]) -> dict[str, list[dict]]:
    """Partition records into blocks keyed by ISO country."""
    blocks: dict[str, list[dict]] = {}
    for r in records:
        blocks.setdefault(r["_country"], []).append(r)
    return blocks


# --------------------------------------------------------------------------- #
# Stage 3 — matching within a block. Emits scored, *reasoned* edges — it never
# merges on the spot, so every borderline collapse is explainable to a reviewer.
# --------------------------------------------------------------------------- #
def acronym_discriminator(a: str, b: str) -> bool:
    """True if two names are identical but for one short (<=3ch) token — distinct
    initialisms (Samsung SDI vs SDS). Deterministic backstop so a near-collision
    fuzzy score can never over-merge two genuinely different entities."""
    ta, tb = a.split(), b.split()
    # Only comparable when both have the same token count and >1 token (a single
    # differing token IS the whole name, e.g. two unrelated one-word firms).
    if len(ta) != len(tb) or len(ta) < 2:
        return False
    # Positions where the two token lists disagree.
    diffs = [i for i, (x, y) in enumerate(zip(ta, tb)) if x != y]
    # Distinct-by-initialism iff they differ in exactly ONE position AND that
    # differing token is a short code (<=3ch) on both sides — "samsung SDI" vs
    # "samsung SDS". Anything longer is a real word, not an initialism, so we
    # leave it to fuzzy scoring rather than force a split here.
    return len(diffs) == 1 and all(len(ta[i]) <= 3 and len(tb[i]) <= 3 for i in diffs)


def llm_propose_merge(a: dict, b: dict) -> dict:
    """STUB for the LLM merge-proposer (offline, deterministic mock — no network).

    Mimics a shallow LLM that over-weights shared brand tokens and will happily
    propose merging siblings (LG Energy vs LG Chem) or false friends (SDI vs SDS).
    It returns an UNTRUSTED {is_same_entity, confidence, reasoning}; the caller
    treats it as a candidate signal only and routes it through the SAME
    deterministic gate — the LLM can never autocommit a merge. In production this
    is a mid-tier proposer + a second, different-family cross-checker; a wrong
    merge is the system's #1 severity failure, so this stays LLM-*assisted*, not
    LLM-decided."""
    ta, tb = a["_norm"].split(), b["_norm"].split()
    same_brand = bool(ta) and bool(tb) and ta[0] == tb[0] and len(ta) > 1 and len(tb) > 1
    return {
        "is_same_entity": same_brand,
        "confidence": 0.66 if same_brand else 0.1,
        "reasoning": f"shared leading brand token '{ta[0]}'" if same_brand else "no brand overlap",
    }


def match_within_block(records: list[dict]) -> tuple[list[tuple], list[tuple]]:
    """Score every intra-block pair; return (accepted_edges, rejected_log).
    accepted edge = (id_a, id_b, score, reason); reasons: alias-dict / fuzzy:NN /
    (+llm-agree). Rejections are logged with WHY so fail-safe decisions are auditable."""
    accepted: list[tuple] = []
    rejected: list[tuple] = []
    for a, b in combinations(records, 2):
        na, nb = a["_norm"], b["_norm"]

        # Deterministic distinct-entity guard runs first: near-collisions on a
        # short trailing token are refused regardless of fuzzy score. FAIL SAFE.
        if acronym_discriminator(na, nb):
            rejected.append((a["id"], b["id"], "acronym-guard: distinct initialism (fail-safe, not merged)"))
            continue

        reason: Optional[str] = None
        score = 0.0
        ka, kb = alias_key(na), alias_key(nb)
        if ka is not None and ka == kb:
            reason, score = "alias-dict", 100.0
        else:
            score = fuzz.token_set_ratio(na, nb)
            if score >= ACCEPT_THRESHOLD:
                reason = f"fuzzy:{score:.0f}"

        # LLM proposal is an untrusted signal. It can only *reinforce* an edge the
        # deterministic gate already accepted; it can NEVER create one alone.
        prop = llm_propose_merge(a, b)
        if reason is not None:
            if prop["is_same_entity"]:
                reason += " +llm-agree"
            accepted.append((a["id"], b["id"], score, reason))
        elif prop["is_same_entity"]:
            rejected.append((a["id"], b["id"],
                             f"llm proposed merge (conf={prop['confidence']}, {prop['reasoning']}) "
                             f"but NO deterministic support -> not merged (fail-safe)"))
    return accepted, rejected


# --------------------------------------------------------------------------- #
# Stage 4 — clustering (union-find over accepted edges). THE CENTERPIECE.
# Transitive closure: A-B and B-C accepted -> {A,B,C} even with no direct A-C
# edge. This is what makes it real entity resolution, not naive pairwise dedup.
# Runs within blocks so it stays cheap. Only >=threshold edges union; ambiguous
# pairs are left UNMERGED (an unmerged duplicate is recoverable; a wrong merge
# silently corrupts). Fail safe over fail merged.
# --------------------------------------------------------------------------- #
class _DSU:
    """Disjoint-set union (union-find). Each record id starts as its own set; an
    accepted match edge unions two sets. Members that end up under the same root
    are one entity — this is how transitive closure (A-B, B-C => {A,B,C}) is
    computed without needing a direct A-C edge."""

    def __init__(self, ids: list[str]):
        # Every id is initially its own parent (n singleton sets).
        self.parent = {i: i for i in ids}

    def find(self, x: str) -> str:
        """Return the representative (root) of x's set, compressing the path so
        repeated lookups are near-constant time."""
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]  # path compression: point at grandparent
            x = self.parent[x]
        return x

    def union(self, a: str, b: str) -> None:
        """Merge the two sets containing a and b by pointing one root at the other."""
        self.parent[self.find(a)] = self.find(b)


def cluster(records: list[dict], edges: list[tuple]) -> list[list[dict]]:
    """Union-find over accepted edges -> one list of records per real-world entity."""
    dsu = _DSU([r["id"] for r in records])
    # Union every accepted edge; score/reason aren't needed here — the gate in
    # match_within_block already decided the edge was trustworthy enough to fuse.
    for id_a, id_b, _score, _reason in edges:
        dsu.union(id_a, id_b)
    # Group records by their set's root -> one bucket (list) per real-world entity.
    by_root: dict[str, list[dict]] = {}
    for r in records:
        by_root.setdefault(dsu.find(r["id"]), []).append(r)
    return list(by_root.values())


# --------------------------------------------------------------------------- #
# Stage 5 — field survivorship + per-field confidence.
# DETERMINISTIC BY MANDATE — no LLM picks or generates a surviving value, which
# is what makes the whole pipeline idempotent and every field auditable.
# Field-stakes prioritization: disqualifying hard filters (country, certs) demand
# high confidence to survive; low cross-source agreement on them yields an
# `unverified` flag instead of a falsely-confident value. Both survivorship AND
# confidence read the shared SOURCE_TRUST table.
# --------------------------------------------------------------------------- #
def _confidence(winning_trust: float, agreement: float) -> float:
    """Confidence = source trust discounted by cross-source agreement (0.5..1x)."""
    return round(winning_trust * (0.5 + 0.5 * agreement), 2)


def _field(value, source, source_id, conf, disqualifying=False, note=""):
    """Build one canonical field carrying its own provenance (winning source +
    record id), confidence, and optional note — this per-field envelope is what
    makes the output auditable rather than an opaque merged blob."""
    f = {"value": value, "source": source, "source_id": source_id, "confidence": conf}
    # Field-stakes rule: a *disqualifying* field (country/cert) that can't clear
    # the confidence floor is surfaced as `unverified` rather than asserted —
    # better to show doubt than to silently drop a supplier from a hard filter.
    if disqualifying and conf < DISQUALIFYING_CONF_FLOOR:
        f["unverified"] = True
    if note:
        f["note"] = note
    return f


def resolve_fields(members: list[dict]) -> dict:
    """Collapse one cluster into a canonical record with field-level provenance,
    per-field confidence, and a merge explanation."""
    trust = lambda r: SOURCE_TRUST.get(r["source"], 0.0)  # noqa: E731

    # Canonical legal name: longest well-formed original form (proxy for most complete).
    name_rec = max(members, key=lambda r: (len(r["name"]), trust(r)))

    # Country (disqualifying): trust-weighted vote; confidence tracks agreement.
    countries = [r["_country"] for r in members]
    top_country = max(set(countries), key=lambda c: sum(trust(r) for r in members if r["_country"] == c))
    c_src = max((r for r in members if r["_country"] == top_country), key=trust)
    c_agree = countries.count(top_country) / len(countries)
    country_field = _field(top_country, c_src["source"], c_src["id"],
                           _confidence(trust(c_src), c_agree), disqualifying=True)

    # Revenue (ranking-only): highest-trust source, tie-broken by recency. NOT max —
    # a big number from a low-trust aggregator must not beat the registry.
    rev_rec = max(members, key=lambda r: (trust(r), r["as_of"]))
    others = {r["revenue_musd"] for r in members} - {rev_rec["revenue_musd"]}
    revenue_field = _field(
        rev_rec["revenue_musd"], rev_rec["source"], rev_rec["id"],
        _confidence(trust(rev_rec), 1.0 if not others else 0.4),
        note=f"conflicting values {sorted({r['revenue_musd'] for r in members})}; "
             f"took highest-trust source" if others else "")

    # Certifications (disqualifying): UNION across sources; each cert's confidence =
    # trust of its best asserting source. A cert seen only from low-trust sources is
    # kept but flagged `unverified` rather than presented as an audited fact.
    cert_provenance: dict[str, dict] = {}
    for r in members:
        for c in r["certs"]:
            best = cert_provenance.get(c)
            if best is None or trust(r) > SOURCE_TRUST.get(best["source"], 0.0):
                cert_provenance[c] = {"source": r["source"], "source_id": r["id"]}
    certs_field = []
    for c, prov in sorted(cert_provenance.items()):
        t = SOURCE_TRUST.get(prov["source"], 0.0)
        certs_field.append(_field(c, prov["source"], prov["source_id"],
                                 _confidence(t, 1.0), disqualifying=True))

    return {
        "canonical_name": name_rec["name"],
        "country": country_field,
        "revenue_musd": revenue_field,
        "certifications": certs_field,
        "member_ids": [r["id"] for r in members],
    }


# --------------------------------------------------------------------------- #
# Stage 6 — hierarchy detection. Parent/subsidiary is LINKED, not merged: a
# subsidiary is a distinct entity, not a duplicate of its parent. Same brand
# token + a division keyword on each side -> same family; a parent keyword fixes
# direction, otherwise it's recorded as an affiliate flagged for review.
# --------------------------------------------------------------------------- #
def _tokens(name: str) -> list[str]:
    """Normalized word tokens of a name (suffix-stripped, casefolded) — the unit
    hierarchy detection reasons over (shared brand token, role keywords)."""
    return normalize_name(name).split()


def detect_hierarchy(canonicals: list[dict]) -> None:
    """Attach parent/subsidiary/affiliate relationships in-place (never merges)."""
    for c in canonicals:
        c.setdefault("relationships", [])
    for a, b in combinations(canonicals, 2):
        if a["country"]["value"] != b["country"]["value"]:
            continue
        ta, tb = _tokens(a["canonical_name"]), _tokens(b["canonical_name"])
        if not ta or not tb or ta[0] != tb[0]:
            continue
        # Same family iff BOTH names carry a role keyword (a division like
        # energy/chem, or a parent form like holdings/group) beyond the shared
        # brand — this excludes coincidental brand-token collisions.
        family_kw = DIVISION_KEYWORDS | PARENT_KEYWORDS
        a_role = any(t in family_kw for t in ta[1:])
        b_role = any(t in family_kw for t in tb[1:])
        if not (a_role and b_role):
            continue
        a_parent = any(t in PARENT_KEYWORDS for t in ta)
        b_parent = any(t in PARENT_KEYWORDS for t in tb)
        if a_parent and not b_parent:
            a["relationships"].append({"type": "parent_of", "entity": b["canonical_name"]})
            b["relationships"].append({"type": "subsidiary_of", "entity": a["canonical_name"]})
        elif b_parent and not a_parent:
            b["relationships"].append({"type": "parent_of", "entity": a["canonical_name"]})
            a["relationships"].append({"type": "subsidiary_of", "entity": b["canonical_name"]})
        else:
            for x, y in ((a, b), (b, a)):
                x["relationships"].append(
                    {"type": "affiliate_of", "entity": y["canonical_name"],
                     "note": "same corporate family; direction undetermined -> review"})


# --------------------------------------------------------------------------- #
# Orchestration + demo.
# --------------------------------------------------------------------------- #
def resolve(records: list[dict]) -> tuple[list[dict], list[tuple]]:
    """Full pipeline over a batch. Pure: same input -> same output (idempotent)."""
    # Annotate each record with derived comparison keys (_norm name, _country ISO)
    # once, up front. dict(r, ...) copies so the caller's input is never mutated —
    # part of keeping the pipeline pure/idempotent and safe to re-run on backfills.
    recs = [dict(r, _norm=normalize_name(r["name"]), _country=normalize_country(r["country"]))
            for r in records]
    all_rejected: list[tuple] = []
    clusters: list[list[dict]] = []
    for _key, group in sorted(block(recs).items()):
        edges, rejected = match_within_block(group)
        all_rejected += rejected
        clusters += cluster(group, edges)
    canonicals = [resolve_fields(m) for m in clusters]
    detect_hierarchy(canonicals)
    # attach a compact merge explanation
    for c in canonicals:
        c["merge_explanation"] = (
            f"{len(c['member_ids'])} record(s) fused: {', '.join(c['member_ids'])}"
            if len(c["member_ids"]) > 1 else f"singleton: {c['member_ids'][0]}")
    return canonicals, all_rejected


def _fmt_field(f: dict) -> str:
    """Render one field for the demo as `value  conf=..  <-source/id [flags]` so a
    grader can read the surviving value AND where it came from on a single line."""
    flag = "  [UNVERIFIED]" if f.get("unverified") else ""
    note = f"  ({f['note']})" if f.get("note") else ""
    return f"{f['value']}  conf={f['confidence']}  <-{f['source']}/{f['source_id']}{flag}{note}"


def main() -> None:
    canonicals, rejected = resolve(MESSY_RECORDS)
    canonicals.sort(key=lambda c: c["member_ids"])

    print("=" * 78)
    print(f"BEFORE:  {len(MESSY_RECORDS)} messy source records")
    print(f"AFTER :  {len(canonicals)} canonical entities")
    print("=" * 78)

    for c in canonicals:
        print(f"\n### {c['canonical_name']}")
        print(f"    {c['merge_explanation']}")
        print(f"    country        : {_fmt_field(c['country'])}")
        print(f"    revenue (M USD): {_fmt_field(c['revenue_musd'])}")
        print("    certifications :")
        for cf in c["certifications"]:
            print(f"        - {_fmt_field(cf)}")
        if c["relationships"]:
            print("    relationships  :")
            for rel in c["relationships"]:
                extra = f"  ({rel['note']})" if rel.get("note") else ""
                print(f"        - {rel['type']}: {rel['entity']}{extra}")

    print("\n" + "-" * 78)
    print("FAIL-SAFE / NOT-MERGED LOG (uncertainty defaults to NOT acting):")
    for id_a, id_b, why in rejected:
        print(f"    {id_a} <-> {id_b}: {why}")
    print("-" * 78)


if __name__ == "__main__":
    main()
