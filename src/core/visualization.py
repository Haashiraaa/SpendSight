# src/core/visualization.py

import logging
from typing import Optional
from src.core.analysis import FinanceAnalysis
from haashi_pkg.utility import Logger, FileHandler
from haashi_pkg.plot_engine import PowerCanvas


class FinanceDashboard:

    def __init__(
        self,
        fa: FinanceAnalysis,
        logger: Optional[Logger] = None,
        handler: Optional[FileHandler] = None,
    ) -> None:
        self.logger = logger or Logger(logging.INFO)
        self.handler = handler or FileHandler(logger=self.logger)
        self.fa = fa

    def visualize_data(
        self, save_path: str = "data/dashboard/bank_dashboard_powercanvas.png"
    ) -> None:

        income = self.fa.get_total_income()
        income_series = self.fa.get_income_series()

        expenses = self.fa.get_total_expenses()
        expenses_series = self.fa.get_expenses_series()

        pc = PowerCanvas(
            title="Personal Finance Overview",
            theme="light",
            figsize=(26, 16),
            logger=self.logger
        )

        pc.create_canvas(
            rows=3,
            cols=6,
            height_ratios=[0.18, 0.44, 0.38],
            hspace=0.52,
            wspace=0.38,
        )

        pc.add_kpi_card(
            (0, 0),
            label="Total Income",
            value=f"₦{income:,.0f}",
            sparkline_data=income_series
        )

        pc.add_kpi_card(
            (0, 1),
            label="Total Expenses",
            value=f"₦{expenses:,.0f}",
            sparkline_data=expenses_series,
        )

        save_path = str(self.handler.get_script_dir() / save_path)
        pc.render(save_path=save_path, dpi=180, show=False)
