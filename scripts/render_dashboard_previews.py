"""Draw a PNG preview of each Grafana dashboard from the SAME panel SQL.

Used for the report/README (Grafana itself renders the live dashboards).
Run:  python scripts/render_dashboard_previews.py  -> docs/dashboards/*.png
"""
import re
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import psycopg2  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "grafana"))
sys.path.insert(0, str(ROOT / "src"))
from build_dashboards import DASHBOARDS  # noqa: E402
from pipeline.config import PG  # noqa: E402

SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100"]
INK, MUTED, GRID = "#1f1f1e", "#6b6a64", "#e6e5e0"
OUT = ROOT / "docs" / "dashboards"


def _style(ax):
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    for s in ("left", "bottom"):
        ax.spines[s].set_color(GRID)
    ax.tick_params(colors=MUTED, labelsize=7)
    ax.grid(axis="both", color=GRID, linewidth=0.6)
    ax.set_axisbelow(True)


def draw(ax, title, ptype, cols, rows, opts):
    ax.set_title(title, fontsize=8.5, color=INK, loc="left", pad=6, fontweight="bold")
    if not rows:
        ax.text(0.5, 0.5, "no data", ha="center", va="center", color=MUTED); ax.axis("off"); return
    if ptype == "stat":
        v = rows[0][0]
        unit = {"percent": " %", "s": " s", "ms": " ms", "decmbytes": " MB"}.get(opts.get("unit"), "")
        fv = float(v) if v is not None else None
        dec = opts.get("decimals", 0 if fv is None or fv.is_integer() or fv >= 100 else 2)
        txt = f"{fv:,.{dec}f}{unit}" if fv is not None else "-"
        ax.text(0.5, 0.42, txt, ha="center", va="center", fontsize=20, color=SERIES[0], fontweight="bold")
        ax.axis("off")
    elif ptype == "barchart":
        labels = [str(r[0]) for r in rows]
        n_series = len(cols) - 1
        horiz = opts.get("orientation") == "horizontal"
        pos = range(len(rows))
        width = (0.5 if horiz else 0.8) / (1 if opts.get("stacking") == "normal" else n_series)
        left = [0] * len(rows)
        for j in range(n_series):
            vals = [float(r[j + 1] or 0) for r in rows]
            if opts.get("stacking") == "normal":
                if horiz:
                    ax.barh(list(pos), vals, 0.5, left=left, color=SERIES[j], label=cols[j + 1])
                else:
                    ax.bar(list(pos), vals, 0.7, bottom=left, color=SERIES[j], label=cols[j + 1])
                left = [a + b for a, b in zip(left, vals)]
            else:
                offs = [p + (j - (n_series - 1) / 2) * width for p in pos]
                (ax.barh if horiz else ax.bar)(offs, vals, width * 0.9, color=SERIES[j], label=cols[j + 1])
        if horiz:  # category name written just above its bar, so long names are never clipped
            ax.set_yticks([]); ax.invert_yaxis()
            for p_, l in zip(pos, labels):
                ax.text(0.003, p_ - 0.42, l[:60], transform=ax.get_yaxis_transform(), fontsize=5.5, color=MUTED, va="bottom")
            ax.set_ylim(len(rows) - 0.4, -0.95)
            if opts.get("log"):
                ax.set_xscale("log")
        else:
            import textwrap
            ax.set_xticks(list(pos))
            ax.set_xticklabels([textwrap.fill(l.replace("_", " "), 16) for l in labels], fontsize=6)
        if n_series > 1:
            ax.legend(fontsize=6, frameon=False)
        _style(ax)
    elif ptype == "piechart":
        vals = [float(r[1]) for r in rows]
        palette = SERIES + ["#e87ba4", "#008300", "#9a6ad6"]
        ax.pie(vals, labels=None, colors=palette[:len(vals)], startangle=90, counterclock=False,
               wedgeprops=dict(width=0.38, edgecolor="white", linewidth=2))
        tot = sum(vals)
        ax.legend([f"{r[0]}  {100 * float(r[1]) / tot:.1f}%" for r in rows], fontsize=6, frameon=False,
                  loc="center left", bbox_to_anchor=(0.95, 0.5))
    elif ptype == "trend":
        x = [r[0] for r in rows]
        for j in range(1, len(cols)):
            ys = [r[j] for r in rows]
            if all(v is None for v in ys):
                continue
            pts = [(a, float(b)) for a, b in zip(x, ys) if b is not None]
            ax.plot([p[0] for p in pts], [p[1] for p in pts], color=SERIES[(j - 1) % 4], lw=2, marker="o", ms=3,
                    label=cols[j].replace("_", " "))
        ax.set_xlabel("seconds since stream started", fontsize=7, color=MUTED)
        ax.legend(fontsize=6, frameon=False)
        _style(ax)
    elif ptype == "table":
        ax.axis("off")
        show_cols = cols[:8]
        cells = [[("" if v is None else (f"{v:.3f}" if isinstance(v, float) else str(v)))[:22] for v in r[:8]] for r in rows[:9]]
        t = ax.table(cellText=cells, colLabels=[c[:14] for c in show_cols], loc="upper center", cellLoc="left")
        t.auto_set_font_size(False); t.set_fontsize(5.5); t.scale(1, 1.15)
        for (i, _j), c in t.get_celld().items():
            c.set_edgecolor(GRID)
            if i == 0:
                c.set_facecolor("#f4f3ef"); c.set_text_props(fontweight="bold")


def render():
    OUT.mkdir(parents=True, exist_ok=True)
    conn = psycopg2.connect(**PG)
    cur = conn.cursor()
    for slug, (title, panels) in DASHBOARDS.items():
        used = 0
        xx = yy = rh = 0
        for _t, pt, _s, w, h, _o in panels:
            if xx + w > 24:
                xx, yy, rh = 0, yy + rh, 0
            hh = h if pt != "stat" else 3
            xx, rh = xx + w, max(rh, hh)
            used = max(used, yy + hh)
        fig = plt.figure(figsize=(16, 0.36 * used + 0.8), dpi=110)
        fig.suptitle(f"SDI | {title}", x=0.01, ha="left", fontsize=13, fontweight="bold", color=INK)
        gs = fig.add_gridspec(used, 24, hspace=3.0, wspace=1.2, left=0.03, right=0.97, top=1 - 0.9 / (0.36 * used + 0.8), bottom=0.03)
        x = y = row_h = 0
        for ptitle, ptype, sql, w, h, opts in panels:
            if x + w > 24:
                x, y, row_h = 0, y + row_h, 0
            sql = re.sub(r"\$__timeFilter\(([^)]+)\)", r"\1 > now() - interval '7 days'", sql)
            cur.execute(sql)
            cols, rows = [d[0] for d in cur.description], cur.fetchall()
            hh = h if ptype != "stat" else 3
            ax = fig.add_subplot(gs[y:y + hh - 1, x:x + w])
            draw(ax, ptitle, ptype, cols, rows, opts)
            x, row_h = x + w, max(row_h, hh)
        fig.savefig(OUT / f"{slug}.png")
        plt.close(fig)
        print("saved", OUT / f"{slug}.png")


if __name__ == "__main__":
    render()
