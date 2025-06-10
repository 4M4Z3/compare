# Weather Model Comparison Tool

This project compares various weather models (AIFS, GenCast, GraphCast, and FourCastNet) against ERA5 reanalysis data. It provides temperature predictions for the next 10 days at specified locations.

## Features

- Support for multiple weather models:
  - AIFS
  - GenCast
  - GraphCast
  - FourCastNet
- 10-day temperature predictions in Fahrenheit
- Location-based forecasting using latitude and longitude
- Daily iteration capability

## Setup

1. Create a virtual environment (recommended):
```bash
python -m venv venv
source venv/bin/activate  # On Unix/macOS
# or
.\venv\Scripts\activate  # On Windows
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

Run the weather comparison tool:

```bash
python weather_runner.py
```

The script will output predictions from all models for the specified location and time period.

## Project Structure

- `weather_models.py`: Contains individual weather model functions
- `weather_runner.py`: Main script for running comparisons
- `requirements.txt`: Project dependencies

## Note

Currently, all model functions return placeholder values (0.0). Implementation of actual API calls and data processing for each weather model will be added in future updates. 