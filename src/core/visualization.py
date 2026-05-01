# src/core/visualization.py

import logging
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
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

        self.cat_colors = [
            "#4A90D9",  # blue - Food & Dining
            "#2ECC71",  # green - Transport
            "#F39C12",  # orange - Utilities
            "#E91E8C",  # pink/red - Shopping
            "#8E44AD",  # purple - Entertainment
            "#1ABC9C",  # teal - Other
        ]

    def visualize_data(
        self,
        save_path: str = "data/dashboard/bank_dashboard_powercanvas.png"
    ) -> None:

        income = self.fa.get_income_series()
        expenses = self.fa.get_expenses_series()
        savings = self.fa.get_net_savings()
        months = self.fa.get_all_months()
        top_cat, top_cat_spend = self.fa.get_top_spend_category()
        categories, cat_spend = self.fa.get_category_spendings()

        top_data = sorted(zip(categories, cat_spend, self.cat_colors),
                          key=lambda x: x[1], reverse=True)
        top_labels = [x[0] for x in top_data]
        top_vals = [x[1] for x in top_data]
        top_colors = [x[2] for x in top_data]

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
            value=f"₦{sum(income):,.0f}",
            sparkline_data=income
        )

        pc.add_kpi_card(
            (0, 1),
            label="Total Expenses",
            value=f"₦{sum(expenses):,.0f}",
            sparkline_data=expenses,
        )

        pc.add_kpi_card(
            (0, 2),
            label="Net Saved",
            value=f"₦{sum(savings):,.0f}",
            sparkline_data=savings,
        )

        pc.add_kpi_card(
            (0, 3),
            label="Avg Monthly Spend",
            value=f"₦{sum(expenses)/len(expenses):,.0f}",
            sparkline_data=expenses,
        )

        pc.add_kpi_card(
            (0, 4),
            label="Savings Rate",
            value=f"{sum(savings)/sum(income)*100:.1f}%",
            sparkline_data=[round(s/i*100, 1)
                            for s, i in zip(savings, income)],
        )

        pc.add_kpi_card(
            (0, 5),
            label="Top Spend Category",
            value=top_cat,
            delta=f"₦{top_cat_spend:,} total",
            delta_up=False,
        )

        # ─────────────────────────────────────────────────────────────
        # ROW 1 — Income vs Expenses line (col 0-3) | Donut (col 4-5)
        # ─────────────────────────────────────────────────────────────

        # Line chart — income
        pc.add_line(
            (1, 0),
            x=months,
            y=income,
            title="Income vs Expenses — Month by Month",
            xlabel="Month",
            ylabel="Amount (₦)",
            color="#00a86b",
            linewidth=2.8,
            marker="o",
            markersize=6,
            fill=True,
            ylim=(0, max(max(income), max(expenses)) * 1.25),
            col_span=4,
        )

        # Overlay expenses on the same panel
        _ax = pc._get_or_create_flex_panel((1, 0), col_span=4)
        _ax.plot(months, expenses,
                 color="#e63946", linewidth=2.5,
                 linestyle="--", marker="s", markersize=5,
                 label="Expenses", zorder=5)
        _ax.fill_between(range(len(months)), expenses,
                         alpha=0.08, color="#e63946")
        _ax.axhline(y=np.mean(income), color="#00a86b",
                    linewidth=1.0, linestyle=":", alpha=0.5)
        _ax.axhline(y=np.mean(expenses), color="#e63946",
                    linewidth=1.0, linestyle=":", alpha=0.5)
        _ax.yaxis.set_major_formatter(
            plt.FuncFormatter(lambda x, _: f"₦{x:,.0f}"))

        # legend — add income line manually so both show
        from matplotlib.lines import Line2D
        handles = [
            Line2D([0], [0], color="#00a86b", linewidth=2.5,
                   marker="o", markersize=5, label="Income"),
            Line2D([0], [0], color="#e63946", linewidth=2.5,
                   linestyle="--", marker="s", markersize=5, label="Expenses"),
        ]
        _ax.legend(handles=handles, fontsize=10, loc="upper right",
                   facecolor="#ffffff", edgecolor="#e2e8f0",
                   labelcolor="#1e293b")

        # Best month annotation
        best_idx = savings.index(max(savings))
        _ax.annotate(
            f"Best month\n+₦{round(max(savings)):,}",
            xy=(best_idx, income[best_idx]),
            xytext=(best_idx + 0.4, income[best_idx] + 350),
            fontsize=8, color="#00a86b", fontweight="bold",
            arrowprops=dict(arrowstyle="->", color="#00a86b", lw=1.2)
        )

        # Donut — spending by category
        pc.add_pie(
            (1, 4),
            values=cat_spend,
            labels=None,
            title="Spending by Category",
            colors=self.cat_colors,
            autopct="%1.2f%%",
            donut=True,
            col_span=2,
        )

        # Add legend to donut manually
        _dax = pc._get_or_create_flex_panel((1, 4), col_span=2)
        legend_patches = [
            mpatches.Patch(color=color, label=cat)
            for color, cat in zip(self.cat_colors, categories)
        ]
        _dax.legend(handles=legend_patches, loc="lower center",
                    bbox_to_anchor=(0.5, -0.24), ncol=2,
                    fontsize=8, facecolor="#ffffff",
                    edgecolor="#e2e8f0", labelcolor="#1e293b")

        # Centre label on donut
        _dax.text(0, 0.08, f"₦{round(sum(cat_spend)):,}",
                  ha="center", fontsize=15,
                  fontweight="bold", color="#1e293b")
        _dax.text(0, -0.16, "total spend",
                  ha="center", fontsize=8, color="#64748b")

        # ─────────────────────────────────────────────────────────────
        # ROW 2 — Monthly savings bars | Top category hbar | Stats
        # ─────────────────────────────────────────────────────────────

        # Monthly net savings — bar chart
        _sax = pc._get_or_create_flex_panel((2, 0), col_span=2)

        min_val = min(savings)
        max_val = max(savings)

        lower = (min_val * 2.25) if min_val < 0 else -50
        upper = (max_val * 2.25) if max_val > 0 else 50

        bar_colors = ["#00a86b" if s >= 0 else "#e63946" for s in savings]
        bars = _sax.bar(months, savings,
                        color=bar_colors, edgecolor="#ffffff",
                        linewidth=0.8, width=0.6)
        for bar, val in zip(bars, savings):
            ypos = val + 15 if val >= 0 else val - (abs(min_val) * 0.12)
            _sax.text(bar.get_x() + bar.get_width() / 2, ypos,
                      f"₦{round(val):,}", ha="center", fontsize=7.5,
                      color="#1e293b", fontweight="bold")
        _sax.axhline(y=0, color="#64748b", linewidth=0.8)
        _sax.axhline(y=np.mean(savings), color="#2563eb",
                     linewidth=1.2, linestyle="--", alpha=0.6)
        _sax.set_ylim(lower, upper)
        _sax.yaxis.set_major_formatter(
            plt.FuncFormatter(lambda x, _: f"₦{x:,.0f}"))
        pc._style_chart_ax(_sax, title="Monthly Net Savings")
        _sax.tick_params(axis="x", labelsize=8)

        # Top spending categories — horizontal bar
        _hax = pc._get_or_create_flex_panel((2, 2), col_span=2)
        y_pos = np.arange(len(top_labels))
        hbars = _hax.barh(y_pos, top_vals,
                          color=top_colors,
                          edgecolor="#ffffff", height=0.6)
        for bar, val in zip(hbars, top_vals):
            _hax.text(val + 8, bar.get_y() + bar.get_height() / 2,
                      f"  ₦{val:,}", va="center", fontsize=8,
                      color="#1e293b", fontweight="bold")
        _hax.set_yticks(y_pos)
        _hax.set_yticklabels(top_labels, fontsize=8.5, color="#1e293b")
        _hax.set_xlim(0, max(top_vals) * 1.25)
        _hax.xaxis.set_major_formatter(
            plt.FuncFormatter(lambda x, _: f"₦{x:,.0f}"))
        pc._style_chart_ax(_hax, title="Top Spending Categories")

        # Stats summary panel
        pc.add_stats_panel(
            (2, 4),
            stats={
                "Best Month":       f"Mar '26  (+₦{max(savings):,})",
                "Worst Month":      f"Dec '25  (+₦{min(savings):,})",
                "Highest Income":   f"Mar '26  (₦{max(income):,})",
                "Highest Spend":    f"Dec '25  (₦{max(expenses):,})",
                "Top Category":     "Transfers  (₦1,200)",
                "Avg Daily Spend":  f"₦{sum(expenses)/(6*30):.0f} / day",
            },
            title="Statement Summary",
            col_span=2,
        )

        save_path = str(self.handler.get_script_dir() / save_path)
        pc.render(save_path=save_path, dpi=180, show=False)
