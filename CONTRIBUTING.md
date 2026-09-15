# Contributing

> The repository is released under the MIT License.  Read `LICENSE`
> before copying, modifying, or redistributing the project.  This
> document describes the safe contribution path.

## 1. License

This project is licensed under the MIT License.  Keep licensing changes
separate from ordinary code changes and preserve the exact text in
`LICENSE`.

## 2. Coding style

* Python 3.11+ stdlib only.  No new third-party runtime dependencies.
* Keep tests stdlib-native (`unittest`); `pytest` is not required.
* Keep code byte-stable across reformatting; do not run formatters
  that change line endings or whitespace-only hunks on the
  publication-safety scanner or its tests.
* Follow PEP 8 for style; type hints are encouraged for new code.
* Public functions and modules should have a docstring that
  explains *why*, not *what*.

## 3. Branch / commit policy

* `main` is the canonical branch.
* Use topic branches named `topic/<short-description>`.
* Commit messages should reference the slice / tranche they belong
  to (e.g. `W2: ...`).
* Squash-merge is the default.
* No force-pushes to `main`.

## 4. Pull request checklist

A pull request must include:

1. A short description of the change and the slice / tranche it
   belongs to.
2. Updated or new tests under `tests/`.
3. `python3 scripts/check_publication_safety.py .` exits `0`.
4. `python3 -m unittest discover -s tests/news_pipeline -p test_publication_safety.py`
   passes.
5. `python3 -m compileall -q scripts tests` returns silently.

A pull request must **not** include:

* New third-party dependencies in `pyproject.toml`
  `[project.dependencies]`.
* Unrelated changes to the license or copyright notice.
* Live operator configuration (`config/news-*.toml` non-example).
* Host identities, private endpoints, or credentials.
* Reference to Zeroclaw as a bundled component.

## 5. Reporting issues

* Security issues: see `SECURITY.md`.
* Documentation issues: open a normal pull request or contact the
  operator through the channel listed in `README.md`.

## 6. Code of conduct

Contributors are expected to act in good faith:

* Be respectful, even in disagreement.
* Focus on the technical content.
* Do not introduce offensive content into the repository.

## 7. Acknowledgements

Thank you for considering a contribution.  The publication gate is
deliberately strict; if a pull request is rejected for a non-code
reason, please address that reason directly.
