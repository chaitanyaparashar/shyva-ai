# Supplier Intelligence Engine — Take-Home Submission

A 2-hour architecture + coding evaluation. The deliverable is a **thin runnable prototype wrapped in a production-scale design**: exactly one artifact runs, everything else is deliberately described-not-built.

Every design decision is anchored to one test query:

> *"Lithium-ion battery cell manufacturers in Japan or South Korea that supply to automotive OEMs, with ISO-9001 or IATF-16949 certification and an established export presence."*

## The three deliverables

| File | What it is | Runs? |
|---|---|---|
| [`normalize.py`](normalize.py) | **Entity-resolution prototype** — the working core. Layered pipeline: surface normalization → blocking → reasoned match edges → union-find clustering → deterministic per-field survivorship + confidence → parent/subsidiary linking. | **Yes** |
| [`ARCHITECTURE.md`](ARCHITECTURE.md) | The **production system design** — offline pipeline vs. online serving (Mermaid diagram), launch-vs-scale stack table, datastore choices, ingestion/TTL model, data prioritization, latency-vs-accuracy, failure modes, a worked trace of the test query, and the LLM usage map. | No (by design) |
| [`DECISION_LOG.md`](DECISION_LOG.md) | What was prioritized and why, the explicit de-scope list with deferral triggers, a severity-ranked vulnerability list with fallbacks, and the single first thing to build with a full week. | No (by design) |

## Run the prototype

```bash
pip install rapidfuzz     # only third-party dependency; no network, no API keys
python normalize.py       # use python3 if your system has no `python` alias
```

It resolves **10 messy mocked supplier records → 8 canonical entities** and prints each canonical record with field-level provenance, per-field confidence, `unverified` flags, relationships, and a fail-safe log of refused merges. Output is idempotent (byte-identical on re-run).

**Three load-bearing checks the run demonstrates:**

1. **Transitive resolution** — `CATL` / `Contemporary Amperex Technology Co., Ltd.` / `Contemporary Amperex Tech` fuse into one entity via union-find, even though the acronym↔abbreviation pair scores only 21 (well below threshold): alias-dict + fuzzy edges close the cluster transitively. Revenue survivorship takes the cert-registry's 32000 over a higher 33000 from a lower-trust source — the **source-trust table** decides, not max().
2. **No over-merging** — `Samsung SDI` vs `Samsung SDS` (fuzzy 91, above threshold!) are held apart by a deterministic acronym guard, and the stubbed LLM's merge proposals (`Panasonic Holdings`↔`Energy`, `LG Energy`↔`Chem`) are refused for lack of deterministic support: **LLM proposes, rules dispose; fail safe over fail merged.**
3. **Hierarchy, not merging** — `Panasonic Holdings` is linked `parent_of` → `Panasonic Energy`; the ambiguous `LG` pair is flagged `affiliate_of` and routed to human review.

## Design stance in one paragraph

Models are a **recall-and-language engine, not a source of truth**. LLMs handle language-in (query decomposition, extraction) and language-out (RAG synthesis) with the smallest sufficient model per stage; the HIGH-risk core — relationship truth ("supplies to automotive OEMs") and field survivorship — is **deterministic by mandate**, which keeps the pipeline idempotent and every canonical value auditable back to a cited source via the shared source-trust table, per-field provenance, and `as_of` timestamps. A second model appears in exactly one place (an independent cross-checker on merge proposals) because a silent wrong-merge is the system's worst failure. The stack follows the same restraint: Postgres + pgvector + Redis + S3 at launch, with Kafka, Airflow, Spark, and a dedicated vector DB **named and deferred**, each behind a measured trigger.

## Where the prototype ends and production begins

`normalize.py` runs over an in-memory batch; at scale the *same pure functions* run as an offline batch job over the crawled corpus, writing the **golden registry** in Postgres that the online serving path reads — entity resolution never happens at query time. The seam is marked explicitly throughout: the `# SCALE:` block in `normalize.py`, the "Ship at launch vs. Scale to" columns in `ARCHITECTURE.md`, and the built-vs-deferred split in `DECISION_LOG.md`.
