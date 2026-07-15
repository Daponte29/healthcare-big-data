# Status

Polish and deployment phase. Research and training complete. Paper written. UI working locally.


# Active

Nothing in progress — awaiting next task.


# Recently Completed

* Streamlit inference UI (`deploy/app.py`) — upload chest X-ray → 14-label predictions + Grad-CAM. Auto-discovers all trained checkpoints from archive. Fixed Windows OpenMP crash.
* Moved `notebooks/` from `src/notebooks/` to project root for cleaner structure.
* Fixed 14 GitHub CI failures — `Path.exists()` was not mocked alongside `Image.open()` in tests.
* Updated `pyproject.toml` author info (was placeholder "Your Name").
* Added `streamlit` to `environment.yaml` and `pyproject.toml` dev deps.
* Added `results/`, `AI_CONTEXT.md`, `CURRENT_TASK.md` to `.gitignore`.
* Updated README with deploy section and corrected project structure tree.


# Blocked

Nothing currently blocked.


# Backlog

**High**

* MLflow experiment tracking — add to `train.py` to log config, per-epoch AUROC, and checkpoint path. Replaces manual JSON files with a queryable dashboard.
* Dockerfile — containerize the Streamlit app so it runs without the conda env.
* Fix CI — replace manual `pip install` list in `ci.yml` with `pip install -e ".[dev]"` and add `black --check` + `flake8` steps.

**Medium**

* Grad-CAM label selector in UI — let user pick which label to visualize, not just the top predicted one.
* Full-resolution CheXpert evaluation — re-run on non-small dataset to better isolate bias effects for Support Devices and Fracture.
* Model registry — proper versioning instead of manual folder naming.

**Low**

* Batch image upload in UI — multiple X-rays at once with a results table.
* Confidence threshold slider in UI.
* Data versioning (DVC) — track which manifest/stylized images produced which checkpoint.


