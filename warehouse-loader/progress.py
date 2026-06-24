from __future__ import annotations

import logging
import sys
from typing import Any

try:
    from tqdm import tqdm
except ImportError:  # pragma: no cover - exercised through runtime fallback
    tqdm = None


class ProgressReporter:
    def __init__(
        self,
        *,
        logger: logging.Logger,
        component_name: str,
        interval: int,
        total: int,
        unit: str,
    ) -> None:
        self._logger = logger
        self._component_name = component_name
        self._interval = interval
        self._total = total if total > 0 else None
        self._bar = None

        if tqdm is not None and sys.stderr.isatty():
            self._bar = tqdm(
                total=self._total,
                desc=component_name,
                unit=unit,
                dynamic_ncols=True,
                mininterval=0.5,
            )

        self._logger.info(
            "%s progress reporter mode=%s interval=%s tqdm_available=%s total=%s",
            component_name,
            "tqdm" if self._bar is not None else "log",
            interval,
            tqdm is not None,
            self._total if self._total is not None else "unbounded",
        )

    def update(self, current_count: int, **counts: Any) -> None:
        if self._bar is not None:
            self._bar.update(1)
            self._bar.set_postfix(counts, refresh=False)

        if current_count % self._interval == 0:
            metrics = " ".join(f"{key}={value}" for key, value in counts.items())
            self._logger.info(
                "%s progress consumed=%s %s",
                self._component_name,
                current_count,
                metrics,
            )

    def close(self) -> None:
        if self._bar is not None:
            self._bar.close()
