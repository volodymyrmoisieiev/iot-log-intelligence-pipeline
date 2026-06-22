# Data

Local project datasets live here: raw inputs, processed outputs, sample files, and dataset profile metadata.

Dataset profile foundation:

- `dataset_profiles.yml` documents the planned `sample`, `medium`, and `full` processing profiles.
- `samples/sample_iot_logs.csv` remains the default tracked sample dataset for demos and CI-safe checks.
- `processed/medium_iot_logs.csv` is the planned larger subset path for future integration-style validation.
- `raw/full_iot_logs.csv` is the planned full raw dataset path for manual or cloud-style validation only.
- `../scripts/create_dataset_profile.py` creates a local `medium` dataset from a larger CSV input.

Local dataset preparation example:

```powershell
python .\scripts\create_dataset_profile.py --input .\data\raw\RT_IOT2022.csv --output .\data\processed\medium_iot_logs.csv --rows 10000
```

Generated files such as `processed/medium_iot_logs.csv` stay ignored by git so local dataset experiments do not become accidental commits.
