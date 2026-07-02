# 07 — The Conversion Pipeline: How a Website Becomes Agent-Readable

*The Gateway's core intelligence. Key insight: it is not "conversion," it is **recovery** — every website is already generated from structured data (database, CMS, templates, APIs); HTML is a lossy, decorated rendering of structure that already exists. The pipeline taps structure at the highest-fidelity point available and falls back to inference only when it must.*

---

## 1. The tier ladder (best source first)

```
Tier 1: TAP THE SOURCE      → CMS/DB, APIs, sitemaps, feeds, JSON-LD  (no parsing)
Tier 2: DETERMINISTIC       → crawl-once, boilerplate strip, template  (parsing, no AI)
        EXTRACTION            clustering, rule-based field extraction
Tier 3: LLM-ASSISTED        → label fields on samples, EMIT RULES      (AI at build time,
        INDUCTION             + draft descriptions                       never at serve time)
        ─────────────────────────────────────────────────────────────
OUTPUT: signed manifest + semantic views + draft actions → OWNER REVIEW → live
```

### Tier 1 — Tap the source directly
- **CMS/platform plugins** (WordPress, Shopify, ...): read posts/products/prices from the same source of truth the HTML renders from. Zero guessing, perfect fidelity, and free freshness via CMS event hooks. This is why plugins are a first-class Gateway shape.
- **SEO exhaust:** sitemap.xml (URL universe + change hints), RSS/Atom (fresh content), JSON-LD/schema.org (typed products, articles, events, FAQs, orgs), OpenGraph/meta. Commercial sites already ship this for search engines — harvest it all first.
- **OpenAPI/Swagger specs:** actions nearly free — spec→typed-tool conversion is commodity, proven tech.
- Expectation: for a large share of the commercial web, Tier 1 alone yields 70–90% coverage.

### Tier 2 — Deterministic extraction
- **Crawl once** (respecting robots and crawl budget); headless rendering only where content is JS-dependent, and cached — never per-agent-request.
- **Boilerplate stripping** (Readability-class): navigation/footer/cookie-banner removal; main-content isolation.
- **HTML → semantic view:** headings, tables, lists, links preserved; markup noise gone (~80% token reduction is the proven norm).
- **Template clustering — the scaling trick:** cluster pages by DOM structural signature. 50,000 product pages ≠ 50,000 layouts; it's ~one template rendered 50,000 times. Derive **one extraction rule set per template class** (title here, price there, specs in that table). Cost scales with template count (typically 5–20 per site), not page count.
- **Stable chunk IDs:** anchors derived from structural position + content identity so IDs survive re-renders → chunk-level diff sync for returning agents.

### Tier 3 — LLM-assisted induction (build-time only)
Where field semantics are ambiguous, an LLM examines a few sample pages per template cluster and does exactly three jobs:
1. **Label fields** ("this span is the price; that div is the review score; this block is the spec table").
2. **Write the deterministic extraction rules** implementing those labels.
3. **Draft manifest prose** (site description, view descriptions, action candidate descriptions).

The iron rule: **LLM output is configuration, never a serving-path component.** The generated parser runs cheaply, deterministically, forever; no model sits between an agent's request and the response. The LLM writes the parser once; the parser serves millions of requests.

Quality loop: induced rules are validated against held-out pages from the same cluster; sub-threshold accuracy → more samples / owner attention flag. **Drift detection** continuously compares rendered pages vs. produced views (spot hashes + field sanity checks); template changes trigger automatic re-induction + owner notification.

## 2. Actions: drafted, never auto-published

- **Sources for drafts:** HTML forms (already half-typed schemas: field names, types, validation, method/target), OpenAPI specs, and platform plugins (Shopify checkout, WooCommerce, booking plugins → prebuilt, vetted action templates).
- The Action Drafter emits *candidates* with inferred input schemas and suggested risk classes.
- The **owner must**: review each candidate, wire it to the real backend endpoint (or approve the inferred wiring), set/confirm the risk class, configure gates (see [05-OWNER-CONTROL Axis 4](https://github.com/ajar-protocol/ajar/blob/main/docs/05-OWNER-CONTROL.md)), and sign.
- Rationale: auto-guessing read-only content is safe; auto-guessing write-endpoints is how you get disasters. This isn't a limitation of the pipeline — it's the owner-sovereignty principle doing its job.

## 3. What comes out the other end

One command (or plugin activation) later, the Gateway has produced:
- the draft **manifest** for `/.well-known/ajar.json` (unsigned until owner ceremony),
- **semantic views** served from the *same URLs* via content negotiation, chunked with stable IDs and per-chunk hashes,
- a **view index** (machine sitemap: chunk map + hashes) for efficient sync,
- **draft actions** in the approval queue,
- a **coverage report** in the Console: "94% of pages mapped, 3 templates need review, 6 draft actions awaiting approval."

Freshness afterwards: CMS event hooks where available; scheduled content-hash diffing elsewhere; drift alarms.

## 4. Honest limits (documented, not hidden)

1. **Reading (R0) reaches high quality nearly automatically. Actions always need owner participation.** By design.
2. **The hard tail:** canvas-rendered apps, heavy personalization, auth-walled experiences, infinite-scroll-only content — auto-extraction won't cut it; these need owner-side integration (Tier 1 style) or stay human-only. Acceptable: the owner runs the software anyway.
3. **Ambiguity survives:** some fields resist labeling (is that number a price or a model number?). The pipeline flags low-confidence fields for owner review and does not publish guesses.
4. **Legacy fallback (client-side) is a different, lesser thing:** the Kernel's Fallback Engine performs Tier-2-style extraction on manifest-less sites at the agent's expense — unverified, unsigned, consensual (robots/AIPREF/402-respecting), and flagged as such. The Gateway's owner-blessed output is the real product; fallback is the bridge that makes the Client useful on day one.

## 5. Quality bars (tie to ROADMAP Phase 1 exit criteria)

- ≥90% content coverage on 10 structurally diverse pilot sites with ≤1h owner effort.
- ≤20% token cost vs. raw HTML on the same tasks.
- Extraction accuracy per template ≥ target threshold on held-out pages before a template's views go live.
- Zero authenticated/personal content in views unless explicitly configured (verified by test suite).
