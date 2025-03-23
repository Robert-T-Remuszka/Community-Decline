# J2J Extraction Module

This module provides tools for downloading, processing, and aggregating Job-to-Job (J2J) Flows data from the Census Bureau's LEHD program. The implementation uses object-oriented programming to provide a clean interface and supports both pandas and polars for data processing.

## Overview

Job-to-Job Flows (J2J) data provides statistics on job changes and transitions between industries and geographies. This module processes the data in three main steps:

1. **Download**: Downloads raw J2J files from the Census Bureau's LEHD website
2. **Process**: Extracts specific aggregation level data from raw files
3. **Aggregate**: Aggregates interim data to annual level and combines files

## Features

- Object-oriented design with a common interface for different data processing libraries
- Support for both pandas and polars
- Parallel processing for improved performance
- Configurable file paths and aggregation levels
- Command-line interface for easy usage

## Installation

This module requires the following dependencies:

```bash
pip install pandas polars requests rich
```

For Polars support:
```bash
pip install polars
```

## Usage

### Command Line Interface

The module provides a command-line interface for easy usage:

```bash
# Using pandas (default)
python run_pipeline.py --download --process --aggregate --category education --agg_level 592913

# Using polars
python run_pipeline.py --library polars --download --process --aggregate --category industry --agg_level 592901
```

Available options:
- `--library`: Data processing library to use (`pandas` or `polars`)
- `--raw_folder`, `--interim_folder`, `--proc_folder`: Paths for data storage
- `--download`, `--process`, `--aggregate`: Actions to perform
- `--category`: Category to process/aggregate (e.g., industry, education, sex_industry)
- `--agg_level`: Aggregation level code

Common aggregation level codes:
- 592901: industry
- 592913: education
- 642321: sex_industry

### Python API

```python
from j2j_processor import create_processor

# Create a processor using pandas
processor = create_processor(library="pandas")

# Or using polars
processor = create_processor(library="polars")

# Download J2J files
processor.download_files()

# Process raw files
processor.process_files(agg_level=592913, aggregation_name="education")

# Aggregate interim data
processor.aggregate_files(category="education")
```

## File Structure

```
j2jextraction/
├── j2j_processor.py     # Main processor implementation
├── run_pipeline.py      # Command-line interface
└── README.md            # Documentation
```

## Original Files

This module consolidates and refactors functionality from the following original scripts:
- `download_files.py`: Downloads J2J files
- `j2jproc.py`: Processes raw J2J files
- `agg_files.py`: Aggregates interim J2J data

## Data Processing Flow

1. Raw data is downloaded from the Census Bureau's LEHD website
2. Raw data is processed by filtering for specific aggregation levels
3. Processed data is aggregated to annual level and combined into a single file

## Contributing

Feel free to contribute to this module by submitting issues or pull requests. Please follow the existing code style and include appropriate tests. 