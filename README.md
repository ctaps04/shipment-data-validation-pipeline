# Transport Data Quality Pipeline

This project implements a structured data quality pipeline for transport datasets.

It performs:

- Data cleaning
- Field-level validation
- Domain validation
- Relational consistency checks
- Severity-based policy enforcement

The goal is to prevent low-quality or structurally invalid data from reaching downstream systems.

## Architecture

The pipeline follows a hybrid validation model:

1. Cleaning layer
2. Validation layer
3. Error classification
4. Severity-based policy enforcement

All errors are logged.
The pipeline stops only if critical errors are detected.

## Project Structure

The repository is organized as follows:
```text
├── data
│   ├── raw                # Local raw datasets (ignored by git)
│   └── processed          # Local processed outputs (ignored by git)
│
├── tests
│   └── fixtures
│       └── dummy_transport_dataset.xlsx    # Intentionally corrupted dataset for validation testing
│
├── etl_report_transport.py    # Main pipeline script
├── README.md
├── LICENSE
└── .gitignore
```

## Setup

1. Clone the repository

`git clone <repository_url>`
`cd <repository_folder>`

2. (Optional but recommended) Create a virtual environment

`python -m venv venv`
`source venv/bin/activate  # On windows: venv/Scripts/activate`

3. Install dependencies

`pip install -r requirements.txt`

## Usage

1. Place your dataset anywhere (recommended: `data/raw/`)

2. Run:

`python etl_report_transport.py data/raw/dataset`

The script will:
- Clean the dataset
- Validate all fields
- Log detected errors
- Halt execution if critical errors are found

## Execution Model

- The pipeline processes a single dataset per execution. 
- It requires the dataset path as a command line argument.
- It does not automatically scan directories.

## Design Decisions

- Cleaning is separated from validation.
- Validation does not mutate data.
- Error severity is enforced through a centralized policy.
- The pipeline logs all detected errors before enforcing failure conditions.
- The dummy dataset in `tests/fixtures/` is intentionally corrupted to test validation robustness.

## Future Improvements

- More structured error registry
- Batch directory ingestion
- Automated test suite