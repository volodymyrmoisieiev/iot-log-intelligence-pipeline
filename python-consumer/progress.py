from __future__ import annotations

import logging
import os
import sys
from typing import Any

try:
    from tqdm import tqdm
except ImportError:  # pragma: no cover - exercised through runtime fallback
    tqdm = None


VALID_PROGRESS_MODES = {"auto", "log", "tqdm"}


def resolve_progress_mode() -> str:
    raw_mode = os.getenv("PYTHON_PROGRESS_MODE", "auto").strip().lower()
    if raw_mode in VALID_PROGRESS_MODES:
        return raw_mode
    return "auto"


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
        self._mode = resolve_progress_mode()

        if self._should_use_tqdm():
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
            self._active_mode(),
            interval,
            tqdm is not None,
            self._total if self._total is not None else "unbounded",
        )

    def update(self, current_count: int, **counts: Any) -> None:
        if self._bar is not None:
            self._bar.update(1)
            self._bar.set_postfix(counts, refresh=False)
            return

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

    def is_tqdm_active(self) -> bool:
        return self._bar is not None

    def _should_use_tqdm(self) -> bool:
        if tqdm is None:
            return False
        if self._mode == "log":
            return False
        if self._mode == "tqdm":
            return True
        return sys.stderr.isatty()

    def _active_mode(self) -> str:
        if self._bar is not None:
            return "tqdm"
        if self._mode == "tqdm" and tqdm is None:
            return "log_fallback_no_tqdm"
        if self._mode == "tqdm":
            return "tqdm"
        if self._mode == "auto" and tqdm is not None and not sys.stderr.isatty():
            return "log_fallback_notty"
        return "log"
