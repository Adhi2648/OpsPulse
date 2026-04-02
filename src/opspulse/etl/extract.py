from __future__ import annotations

from pathlib import Path

import pandas as pd


def read_workflow_csv(path: Path) -> pd.DataFrame:
    """Read the generated workflow CSV and guarantee source metadata columns."""
    dataframe = pd.read_csv(path)

    if "source_file_name" not in dataframe.columns:
        dataframe["source_file_name"] = path.name

    if "source_row_number" not in dataframe.columns:
        dataframe["source_row_number"] = range(1, len(dataframe) + 1)

    return dataframe
