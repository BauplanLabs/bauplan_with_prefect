# Bauplan + Prefect
A collection of reference implementations for lakehouse patterns with Bauplan and Prefect 3.0

## Overview

This repository contains reference implementations of common data engineering patterns using [Bauplan](https://www.bauplanlabs.com/) as the programmable lakehouse and [Prefect](https://www.prefect.io/) for orchestration and scheduling.

## Setup

### Bauplan

Bauplan is the programmable lakehouse: you can load, transform, query data all from your code (CLI or Python). You can learn more [here](https://www.bauplanlabs.com/), read the [docs](https://docs.bauplanlabs.com/) or explore its [architecture](https://arxiv.org/pdf/2308.05368) and [ergonomics](https://arxiv.org/pdf/2404.13682).

To use Bauplan, you need an API key for our preview environment: you can request one [here](https://www.bauplanlabs.com/#join).

Note: the current SDK version is `0.0.3a492` but it is subject to change as the platform evolves - ping us if you need help with any of the APIs used in this project.

### Python environment

We use [uv](https://docs.astral.sh/uv/getting-started/installation/) to manage the required dependencies - you can synchronize the environment from the root of the repo with:

```bash
uv sync
```

## Pattern 1: Python data engineering with Prefect and Bauplan

Check the README in `src/transformation` for a reference implementation of ETL with Bauplan models, Prefect for orchestration and scheduling, and Streamlit for visualization.

## Pattern 2: Write-Audit-Publish (WAP) with Prefect and Bauplan

Check the README in `src/wap` for a reference implementation of the Write-Audit-Publish pattern with Bauplan and Prefect.

## License

The code in the project is licensed under the MIT License (Prefect and Bauplan are owned by their respective owners and have their own licenses).