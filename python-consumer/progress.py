from __future__ import annotations

import logging
import os
import sys
from typing import Any

try:
    from tqdm import tqdm
except ImportError:  # pragma: no cover - exercised through runtime fallback
    tqdm = None


VALID_PROGRESS_MODES = {"auto", "log", "tqdm", "bar"}
CUSTOM_BAR_WIDTH = 40
CUSTOM_BAR_FILLED = "█"
CUSTOM_BAR_EMPTY = "░"


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
        self._custom_bar_active = False
        self._line_open = False
        self._stream = sys.stderr
        self._mode = resolve_progress_mode()

        if self._should_use_tqdm():
            self._bar = tqdm(
                total=self._total,
                desc=component_name,
                unit=unit,
                dynamic_ncols=True,
                mininterval=0.5,
                file=self._stream,
            )
        elif self._should_use_custom_bar():
            self._custom_bar_active = True

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
        if self._custom_bar_active:
            self._render_custom_bar(current_count, counts)
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
            return
        if self._custom_bar_active and self._line_open:
            self._stream.write("\n")
            self._stream.flush()
            self._line_open = False

    def prepare_log_line(self) -> None:
        if self._custom_bar_active and self._line_open:
            self._stream.write("\n")
            self._stream.flush()
            self._line_open = False

    def is_visual_mode_active(self) -> bool:
        return self._bar is not None or self._custom_bar_active

    def is_tqdm_active(self) -> bool:
        return self.is_visual_mode_active()

    def _should_use_custom_bar(self) -> bool:
        return self._mode == "bar"

    def _should_use_tqdm(self) -> bool:
        if tqdm is None:
            return False
        if self._mode == "log":
            return False
        if self._mode == "bar":
            return False
        if self._mode == "tqdm":
            return True
        return sys.stderr.isatty()

    def _active_mode(self) -> str:
        if self._bar is not None:
            return "tqdm"
        if self._custom_bar_active:
            return "bar"
        if self._mode == "tqdm" and tqdm is None:
            return "log_fallback_no_tqdm"
        if self._mode == "tqdm":
            return "tqdm"
        if self._mode == "auto" and tqdm is not None and not sys.stderr.isatty():
            return "log_fallback_notty"
        return "log"

    def _render_custom_bar(self, current_count: int, counts: dict[str, Any]) -> None:
        label = f"{self._component_name}:".ljust(18)
        if self._total is None:
            percent = 0
            total_display = "?"
            filled = 0
        else:
            bounded_current = min(current_count, self._total)
            percent = int((bounded_current * 100) / self._total)
            filled = int((bounded_current * CUSTOM_BAR_WIDTH) / self._total)
            total_display = str(self._total)
        bar = CUSTOM_BAR_FILLED * filled + CUSTOM_BAR_EMPTY * (CUSTOM_BAR_WIDTH - filled)
        metrics = self._format_metrics(counts)
        self._stream.write(
            f"\r{label} {percent:>3}%|{bar}| {current_count}/{total_display} {metrics}"
        )
        self._stream.flush()
        self._line_open = True

    def _format_metrics(self, counts: dict[str, Any]) -> str:
        if "processed" in counts:
            return (
                f"processed={counts.get('processed', 0)} "
                f"invalid={counts.get('invalid', 0)} "
                f"failed={counts.get('failed', 0)}"
            )
        if "inserted_processed" in counts or "inserted_invalid" in counts:
            inserted = counts.get("inserted_processed", 0) + counts.get("inserted_invalid", 0)
            return f"inserted={inserted} failed={counts.get('failed', 0)}"
        filtered_items = [
            (key, value)
            for key, value in counts.items()
            if key not in {"group_id", "buffered", "batches_flushed"}
        ]
        return " ".join(f"{key}={value}" for key, value in filtered_items)
