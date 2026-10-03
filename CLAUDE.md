@.conventions/CLAUDE.md

# vex-platform

A library, not a service: twitch-archive and doomtp-bot pin a tagged release tarball, so a change reaches
them only when a release is tagged on `main` and their pin moves. See CONTRIBUTING.md "Releases".

- Public signatures, tables and API shapes are a contract with both apps. Changing one is a minor bump
  before 1.0, and the release notes say what each app has to change.
- The SQL under `src/vex_platform/migrations/sql/` is frozen once released. A schema change is a new
  revision file, never an edit to an old one.
- Tests need Postgres: `docker compose up -d` starts the test database that `tests/conftest.py`
  defaults to. `VEX_TEST_DSN` points them elsewhere.
- `spike/` is the Phase 0 spike, kept as a record. It isn't linted or maintained.
- The shared conventions for both apps (jobs, audit, API shapes) are in `docs/conventions.md`; keep it
  in step with the code.
