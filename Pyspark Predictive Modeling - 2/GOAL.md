# What Is This

Deep learning research project investigating **texture vs shape bias** in DenseNet121 trained on CheXpert chest X-rays. Five model variants are trained on differently stylized images (Gaussian blur, patch shuffle, Canny edge, patch rotation) to induce texture or shape bias. Bias is measured via AUROC matrix and Grad-CAM attention maps across 14 pathology labels. Connected to Geirhos et al. (2019) — ImageNet CNNs are biased toward texture; this project tests whether that extends to medical imaging.


---

# Tech Stack

* **PyTorch 2.5.1** + **TorchVision 0.20.1** — training and inference
* **DenseNet121** — 14-label multi-label classifier
* **Polars** — parquet manifest loading
* **Streamlit** — inference UI
* **Conda env:** `DL_PROJECT` (Python 3.12, CUDA via pytorch channel)


---

# Project Structure

```
├── deploy/app.py                   Streamlit UI — upload X-ray → predictions + Grad-CAM
├── notebooks/                      exploration, smoke test, Grad-CAM analysis
├── src/
│   ├── train.py                    training orchestrator (reads YAML config)
│   ├── evaluate.py                 single-model test set evaluation
│   ├── bias_eval.py                full 4×5 AUROC matrix across all variants
│   ├── plot.py                     unified plotting CLI
│   ├── configs/                    active YAML configs + archive_results_configs/config_1-4/
│   ├── data/
│   │   ├── chexpert_dataset.py     PyTorch Dataset class
│   │   └── style_transfer_algos/   gb, ps, ce, pr image generators
│   ├── models/densenet.py          DenseNetClassifier wrapper
│   └── utils/reliance.py           reliance ratio computation
├── tests/test_chexpert_dataset.py
├── environment.yaml
└── pyproject.toml
```


---

# Setup

```bash
conda env create -f environment.yaml
conda activate DL_PROJECT
```

Dataset lives at `src/data/1/` (not committed — download via `src/data/download_raw_data.py` with Kaggle credentials in `.env`). Parquet manifests generated via `src/data/generate_manifests.py`.


---

# How to Run

```bash
# Train a model
python src/train.py --config src/configs/train_original.yaml

# Evaluate on test set
python src/evaluate.py --config src/configs/archive_results_configs/config_1/train_original.yaml

# Run full bias evaluation matrix
python src/bias_eval.py --config-dir src/configs/archive_results_configs/config_1/

# Launch inference UI
streamlit run deploy/app.py

# Run tests
pytest tests/ -v
```


---

# How It Works

Five model variants trained per config on stylized images:

| Variant | Stylization | Bias |
|----|----|----|
| `original` | None | Baseline |
| `gb` | Gaussian blur | Texture |
| `ps` | Patch shuffle | Texture |
| `ce` | Canny edge only | Shape |
| `pr` | Patch rotation | Shape |

`gb/ps = texture`, `ce/pr = shape` — this mapping is used throughout the codebase (`_CORRECT_BIAS` dict in bias_eval). Four configs were run with different loss functions and samplers. Best performing: Config 1 (BCE, no weighted sampler, 11 labels).


---

# Gotchas & Non-Obvious Decisions

* **Windows OpenMP crash** — always set `os.environ["KMP_DUPLICATE_LIB_OK"] = "TRUE"` before importing torch. Done in all notebooks and `deploy/app.py`.
* **Test mocking** — `CheXpertDataset.__getitem__` checks `img_path.exists()` before `Image.open()`. Tests must patch `pathlib.Path.exists` alongside `Image.open` or tests raise `FileNotFoundError` before the mock is reached.
* **Grad-CAM + DenseNet** — DenseNet uses inplace ReLU which corrupts gradient hooks. Fix: `_patch_densenet_relu()` swaps to `inplace=False` before generating heatmaps. Done in notebook 03 and `deploy/app.py`.
* **Manifest path prefix** — the `Path` column starts with `CheXpert-v1.0-small/...`. The Dataset class strips this prefix to resolve images under `image_root_dir`.
* **Notebook path resolution** — notebooks 01 and 03 scan upward for `pyproject.toml`. Notebook 02 uses `Path.cwd().parents[0]` (1 level up from `notebooks/`).
* **Pre-downsampled images** — CheXpert-v1.0-small is \~320×390px before the 224×224 training resize. Limits fine-grained label performance (Support Devices, Fracture).


---

# Current State

Training complete. Four configs × five variants = 20 trained models, all checkpoints in `src/configs/archive_results_configs/`. Key findings: shape bias improved Cardiomegaly/Enlarged Cardiomediastinum AUROC but degraded Pneumonia. Texture bias more stable overall. Grad-CAM confirms Edema attention is clinically aligned; Pneumonia is not under shape bias. Paper written. Streamlit inference UI complete and working.