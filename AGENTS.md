# AGENTS.md

AI coding agents: the org-wide contract in https://github.com/ajar-protocol/.github/blob/main/AGENTS.md applies fully here.

Repo-specific rules:

- No serving-path LLM calls. Tier 3 induction emits reviewed configuration only.
- All signing, hashing, canonicalization, key rotation, and revocation flow through the designated signer module.
- Fail closed with registry error codes at every protocol boundary.
- Fresh installs expose nothing; draft output is unsigned until explicit owner approval.
- Authenticated or personal paths are unexposable unless the owner explicitly overrides with warnings.
- Keep Phase 1 read-layer work separate from Phase 3 action behavior unless the task says otherwise.
