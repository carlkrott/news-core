<!--
Pull request template for news-core.

The repository is in its pre-license phase.  Until the operator
selects a license, no rights are granted to copy, modify, or
redistribute this codebase.  See `CONTRIBUTING.md` §1 and
`SECURITY.md` §8 for the full statement.
-->

## Summary

<!-- One paragraph: what this PR changes and why. -->

## Slice / tranche

<!-- Which slice or tranche does this belong to? (e.g. `W2: publication-safety scanner`)
     If this is a one-off fix, write `out-of-slice: <short reason>`. -->

## Type of change

- [ ] Bug fix (non-breaking change that fixes an issue)
- [ ] New behavior (non-breaking change that adds functionality)
- [ ] Breaking change (fix or feature that would cause existing usage to change)
- [ ] Documentation only
- [ ] Build / CI / tooling only
- [ ] Refactor with no user-facing change

## Required local checks (CI mirrors these)

Run on the local checkout before requesting review:

```bash
# 1. Publication surface is clean (no private evidence, no tokens).
python3 scripts/check_publication_safety.py .

# 2. Focused publication-safety tests pass under stdlib unittest.
python3 -m unittest discover -s tests/news_pipeline -p test_publication_safety.py

# 3. python source tree compiles cleanly.
python3 -m compileall -q scripts tests
```

- [ ] `check_publication_safety.py` exits `0`
- [ ] Focused publication-safety unittest suite passes
- [ ] `compileall` returns silently
- [ ] No new third-party runtime dependencies in `pyproject.toml`
- [ ] No license claims added (no `LICENSE` file, no SPDX header, no `license = ...` in `pyproject.toml`)
- [ ] No live operator configuration under `config/` (only `*.example.toml`)
- [ ] No host identities, Tailnet/LAN endpoints, credentials, or broker socket paths

## Out-of-scope for this PR

- License selection
- Live delivery activation (Telegram, chat IDs, tokens)
- Image publication or registry push
- A downstream scheduler instance as a bundled component
- Unreviewed changes to release/verifier tooling without matching contract
  tests and publication-safety review

## Reviewer notes

<!-- Anything reviewers should look at first: edges, contracts touched, test gaps. -->

## Linked issues

<!-- `Fixes #NNN` / `Refs #NNN` / `None`. -->
