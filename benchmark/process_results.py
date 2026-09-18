#!/usr/bin/env python3
import csv
import os
from collections import defaultdict

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

BENCH_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_CSVS = [
    os.path.join(BENCH_DIR, "raw_results.csv"),
    os.path.join(BENCH_DIR, "raw_results1.csv"),
]
SUMMARY_CSV = os.path.join(BENCH_DIR, "summary.csv")

# --- Read raw data from all CSV files ---
data = defaultdict(list)
for csv_path in RAW_CSVS:
    if not os.path.exists(csv_path):
        print(f"Warning: {csv_path} not found, skipping")
        continue
    with open(csv_path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = (int(row["listeners"]), int(row["workers"]), int(row["connections"]))
            data[key].append({
                "rps": float(row["requests_per_sec"]),
                "lat": float(row["avg_latency_ms"]),
                "transfer": float(row["transfer_mb_s"]),
            })
    print(f"Loaded {csv_path}")

# --- Compute averages ---
summary = {}
for key, runs in data.items():
    n = len(runs)
    summary[key] = {
        "rps": sum(r["rps"] for r in runs) / n,
        "lat": sum(r["lat"] for r in runs) / n,
        "transfer": sum(r["transfer"] for r in runs) / n,
    }

# --- Write summary CSV ---
with open(SUMMARY_CSV, "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["listeners", "workers", "connections",
                "avg_rps", "avg_latency_ms", "avg_transfer_mb_s"])
    for (l, wk, c) in sorted(summary.keys()):
        s = summary[(l, wk, c)]
        w.writerow([l, wk, c,
                     f"{s['rps']:.2f}",
                     f"{s['lat']:.4f}",
                     f"{s['transfer']:.2f}"])

print(f"Summary saved to {SUMMARY_CSV}")

# --- LaTeX table ---
LATEX_TABLE = os.path.join(BENCH_DIR, "table.tex")
with open(LATEX_TABLE, "w") as f:
    f.write("\\begin{table}[H]\n")
    f.write("\\centering\n")
    f.write("\\caption{Результаты нагрузочного тестирования}\n")
    f.write("\\label{tab:benchmark}\n")
    f.write("\\begin{tabular}{|c|c|c|c|c|c|}\n")
    f.write("\\hline\n")
    f.write("Слушатели & Обработчики & Подключения & "
            "Запросов/с & Задержка, мс & Передача, МБ/с \\\\\n")
    f.write("\\hline\n")
    for (l, wk, c) in sorted(summary.keys()):
        s = summary[(l, wk, c)]
        f.write(f"{l} & {wk} & {c} & "
                f"{s['rps']:.0f} & {s['lat']:.2f} & {s['transfer']:.1f} \\\\\n")
        f.write("\\hline\n")
    f.write("\\end{tabular}\n")
    f.write("\\end{table}\n")
print(f"LaTeX table saved to {LATEX_TABLE}")

# --- Plotting ---
connections_list = sorted(set(c for _, _, c in summary.keys()))

# Group configs by listeners
groups = defaultdict(list)
for (l, wk, c) in summary.keys():
    groups[l].append((wk, c))

listener_counts = sorted(groups.keys())

# --- Plot 1: RPS vs connections ---
fig, axes = plt.subplots(1, len(listener_counts),
                         figsize=(7 * len(listener_counts), 5),
                         sharey=True)
if len(listener_counts) == 1:
    axes = [axes]

for ax, l_count in zip(axes, listener_counts):
    worker_counts = sorted(set(wk for wk, _ in groups[l_count]))
    for wk in worker_counts:
        conns = []
        rps_vals = []
        for c in connections_list:
            if (l_count, wk, c) in summary:
                conns.append(c)
                rps_vals.append(summary[(l_count, wk, c)]["rps"])
        ax.plot(conns, rps_vals, marker='o', linewidth=2,
                label=f"{wk} обработчиков")

    ax.set_title(f"{l_count} слушателя", fontsize=14)
    ax.set_xlabel("Количество подключений", fontsize=12)
    ax.set_ylabel("Запросов/с", fontsize=12)
    ax.legend(fontsize=11)
    ax.grid(True, alpha=0.3)
    ax.xaxis.set_major_formatter(ticker.FuncFormatter(
        lambda x, _: f"{int(x/1000)}K"))

plt.tight_layout()
rps_path = os.path.join(BENCH_DIR, "rps_vs_connections.png")
plt.savefig(rps_path, dpi=150)
plt.close()
print(f"RPS plot saved to {rps_path}")

# --- Plot 2: Latency vs connections ---
fig, axes = plt.subplots(1, len(listener_counts),
                         figsize=(7 * len(listener_counts), 5),
                         sharey=True)
if len(listener_counts) == 1:
    axes = [axes]

for ax, l_count in zip(axes, listener_counts):
    worker_counts = sorted(set(wk for wk, _ in groups[l_count]))
    for wk in worker_counts:
        conns = []
        lat_vals = []
        for c in connections_list:
            if (l_count, wk, c) in summary:
                conns.append(c)
                lat_vals.append(summary[(l_count, wk, c)]["lat"])
        ax.plot(conns, lat_vals, marker='s', linewidth=2,
                label=f"{wk} обработчиков")

    ax.set_title(f"{l_count} слушателя", fontsize=14)
    ax.set_xlabel("Количество подключений", fontsize=12)
    ax.set_ylabel("Средняя задержка, мс", fontsize=12)
    ax.legend(fontsize=11)
    ax.grid(True, alpha=0.3)
    ax.xaxis.set_major_formatter(ticker.FuncFormatter(
        lambda x, _: f"{int(x/1000)}K"))

plt.tight_layout()
lat_path = os.path.join(BENCH_DIR, "latency_vs_connections.png")
plt.savefig(lat_path, dpi=150)
plt.close()
print(f"Latency plot saved to {lat_path}")

# --- Plot 3: Combined bar chart ---
fig, ax = plt.subplots(figsize=(12, 6))
configs = sorted(set((l, wk) for l, wk, _ in summary.keys()))
x_positions = range(len(connections_list))
bar_width = 0.8 / len(configs)

for i, (l, wk) in enumerate(configs):
    rps_vals = []
    for c in connections_list:
        if (l, wk, c) in summary:
            rps_vals.append(summary[(l, wk, c)]["rps"])
        else:
            rps_vals.append(0)
    positions = [x + i * bar_width for x in x_positions]
    ax.bar(positions, rps_vals, bar_width,
           label=f"L={l}, W={wk}")

ax.set_xlabel("Количество подключений", fontsize=12)
ax.set_ylabel("Запросов/с", fontsize=12)
ax.set_title("Сравнение конфигураций", fontsize=14)
ax.set_xticks([x + bar_width * (len(configs) - 1) / 2
               for x in x_positions])
ax.set_xticklabels([f"{c//1000}K" for c in connections_list])
ax.legend(fontsize=9, ncol=3)
ax.grid(True, alpha=0.3, axis='y')

plt.tight_layout()
bar_path = os.path.join(BENCH_DIR, "comparison_bar.png")
plt.savefig(bar_path, dpi=150)
plt.close()
print(f"Bar chart saved to {bar_path}")

## ============ 3D plots: workers × listeners → metric ============
from mpl_toolkits.mplot3d import Axes3D
import numpy as np

conn_levels = [20000, 40000, 80000]
all_listeners = sorted(set(l for l, _, _ in summary.keys()))
all_workers = sorted(set(wk for _, wk, _ in summary.keys()))

COLORS = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b']

def make_3d_figure(metric_key, zlabel, title_prefix, filename):
    fig = plt.figure(figsize=(6 * len(conn_levels), 6))
    for idx, conn in enumerate(conn_levels):
        ax = fig.add_subplot(1, len(conn_levels), idx + 1, projection='3d')

        xs, ys, zs, colors = [], [], [], []
        for li, l_val in enumerate(all_listeners):
            for wi, w_val in enumerate(all_workers):
                if (l_val, w_val, conn) in summary:
                    xs.append(li)
                    ys.append(wi)
                    zs.append(summary[(l_val, w_val, conn)][metric_key])
                    colors.append(COLORS[li % len(COLORS)])

        dx = dy = 0.6
        ax.bar3d(xs, ys, [0]*len(zs), dx, dy, zs, color=colors, alpha=0.85, edgecolor='black', linewidth=0.3)

        ax.set_xticks(range(len(all_listeners)))
        ax.set_xticklabels(all_listeners)
        ax.set_yticks(range(len(all_workers)))
        ax.set_yticklabels(all_workers)
        ax.set_xlabel("Слушатели", fontsize=10, labelpad=8)
        ax.set_ylabel("Обработчики", fontsize=10, labelpad=8)
        ax.set_zlabel(zlabel, fontsize=10, labelpad=8)
        ax.set_title(f"{conn // 1000}K подключений", fontsize=13)
        ax.view_init(elev=25, azim=-50)

    fig.suptitle(title_prefix, fontsize=15)
    fig.subplots_adjust(left=0.02, right=0.98, wspace=0.15, top=0.90)
    path = os.path.join(BENCH_DIR, filename)
    plt.savefig(path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"3D plot saved to {path}")

make_3d_figure("lat",      "Задержка, мс",     "Средняя задержка",       "3d_latency.png")
make_3d_figure("rps",      "Запросов/с",        "Запросы в секунду",      "3d_rps.png")
make_3d_figure("transfer", "Передача, МБ/с",   "Скорость передачи данных", "3d_transfer.png")

## ============ Config (2,8): RPS & Transfer with cubic polynomial fit ============

cfg_conns = sorted(c for (l, wk, c) in summary if l == 2 and wk == 8)
cfg_rps = [summary[(2, 8, c)]["rps"] for c in cfg_conns]
cfg_tr  = [summary[(2, 8, c)]["transfer"] for c in cfg_conns]

x = np.array(cfg_conns, dtype=float)
x_fit = np.linspace(x[0], x[-1], 300)

# Нормализуем x для численной устойчивости polyfit
x_norm = x / 1000.0
x_fit_norm = x_fit / 1000.0

def poly3_label(coeffs):
    a, b, c, d = coeffs
    parts = []
    for pw, coef in [(3, a), (2, b), (1, c), (0, d)]:
        if abs(coef) < 0.005:
            continue
        sign = '+' if coef >= 0 and parts else ''
        if coef < 0:
            sign = '-' if not parts else '- '
            coef = abs(coef)
        if pw == 0:
            parts.append(f"{sign}{coef:.1f}")
        elif pw == 1:
            parts.append(f"{sign}{coef:.2f}x")
        else:
            parts.append(f"{sign}{coef:.2f}x\u00b3" if pw == 3 else f"{sign}{coef:.2f}x\u00b2")
    return ''.join(parts)

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))

# --- RPS ---
y = np.array(cfg_rps)
coeffs_rps = np.polyfit(x_norm, y, 3)
p_rps = np.poly1d(coeffs_rps)
ax1.scatter(x, y, s=70, zorder=5, color='#1f77b4', label='Измерения')
ax1.plot(x_fit, p_rps(x_fit_norm), '--', color='red', linewidth=2,
         label=f'Полином 3-й ст. (x в тыс.)')
ax1.set_xlabel("Количество подключений", fontsize=12)
ax1.set_ylabel("Запросов/с", fontsize=12)
ax1.set_title("L=2, W=8: Запросы в секунду", fontsize=13)
ax1.legend(fontsize=10)
ax1.grid(True, alpha=0.3)
ax1.xaxis.set_major_formatter(ticker.FuncFormatter(lambda v, _: f"{int(v/1000)}K"))

# --- Transfer ---
y2 = np.array(cfg_tr)
coeffs_tr = np.polyfit(x_norm, y2, 3)
p_tr = np.poly1d(coeffs_tr)
ax2.scatter(x, y2, s=70, zorder=5, color='#2ca02c', label='Измерения')
ax2.plot(x_fit, p_tr(x_fit_norm), '--', color='red', linewidth=2,
         label=f'Полином 3-й ст. (x в тыс.)')
ax2.set_xlabel("Количество подключений", fontsize=12)
ax2.set_ylabel("Передача, МБ/с", fontsize=12)
ax2.set_title("L=2, W=8: Скорость передачи", fontsize=13)
ax2.legend(fontsize=10)
ax2.grid(True, alpha=0.3)
ax2.xaxis.set_major_formatter(ticker.FuncFormatter(lambda v, _: f"{int(v/1000)}K"))

plt.tight_layout()
cfg28_path = os.path.join(BENCH_DIR, "cfg_2_8_rps_transfer.png")
plt.savefig(cfg28_path, dpi=150)
plt.close()
print(f"Config (2,8) plot saved to {cfg28_path}")
print(f"  RPS  coeffs (x in thousands): {coeffs_rps}")
print(f"  Transfer coeffs (x in thousands): {coeffs_tr}")

## ============ Config (2,8): 3D surface — connections, latency, transfer ============

cfg_lat = [summary[(2, 8, c)]["lat"] for c in cfg_conns]

fig = plt.figure(figsize=(12, 8))
ax = fig.add_subplot(111, projection='3d')

xp = np.array(cfg_conns, dtype=float)
yp = np.array(cfg_lat)
zp = np.array(cfg_tr)

# Полиномы 3-й степени (x в тысячах)
t_norm = xp / 1000.0
p_lat = np.poly1d(np.polyfit(t_norm, yp, 3))
p_tr3 = np.poly1d(np.polyfit(t_norm, zp, 3))

# Гладкая сетка для поверхности-ленты
N = 300
t_smooth = np.linspace(t_norm[0], t_norm[-1], N)
x_smooth = t_smooth * 1000.0
lat_smooth = p_lat(t_smooth)
tr_smooth = p_tr3(t_smooth)

# Лента: расширяем кривую по Y (задержка) ±width
ribbon_w = 2.5
X_surf = np.array([x_smooth, x_smooth])
Y_surf = np.array([lat_smooth - ribbon_w, lat_smooth + ribbon_w])
Z_surf = np.array([tr_smooth, tr_smooth])

from matplotlib import cm
ax.plot_surface(X_surf, Y_surf, Z_surf, alpha=0.6,
                color='#4c9fe0', edgecolor='none', shade=True)

# Основная кривая поверх ленты
ax.plot(x_smooth, lat_smooth, tr_smooth,
        color='#d62728', linewidth=3, label='Полином 3-й ст.')

# Проекции на стенки
ax.plot(x_smooth, lat_smooth, ax.get_zlim()[0] * np.ones_like(x_smooth),
        color='gray', linewidth=1, alpha=0.5, linestyle='--')
ax.plot(ax.get_xlim()[1] * np.ones_like(x_smooth), lat_smooth, tr_smooth,
        color='gray', linewidth=1, alpha=0.5, linestyle='--')
ax.plot(x_smooth, ax.get_ylim()[1] * np.ones_like(x_smooth), tr_smooth,
        color='gray', linewidth=1, alpha=0.5, linestyle='--')

# Точки измерений
ax.scatter(xp, yp, zp, s=100, color='#d62728', edgecolors='black',
           linewidths=0.8, zorder=10, label='Измерения')

# Вертикальные линии от точек к проекции на дно
for xi, yi, zi in zip(xp, yp, zp):
    ax.plot([xi, xi], [yi, yi], [ax.get_zlim()[0], zi],
            color='gray', linewidth=0.7, alpha=0.4)

ax.set_xlabel("Подключения", fontsize=12, labelpad=10)
ax.set_ylabel("Задержка, мс", fontsize=12, labelpad=10)
ax.set_zlabel("Передача, МБ/с", fontsize=12, labelpad=10)
ax.set_title("L=2, W=8: Подключения — Задержка — Передача", fontsize=14, pad=15)
ax.xaxis.set_major_formatter(ticker.FuncFormatter(lambda v, _: f"{int(v/1000)}K"))
ax.legend(fontsize=11, loc='upper right')
ax.view_init(elev=25, azim=-50)

plt.tight_layout()
path_3d_cfg = os.path.join(BENCH_DIR, "cfg_2_8_conn_lat_transfer_3d.png")
plt.savefig(path_3d_cfg, dpi=150, bbox_inches='tight')
plt.close()
print(f"Config (2,8) 3D plot saved to {path_3d_cfg}")

## ============ Config (2,8): 2D — latency & transfer with polynomial equation ============

def fmt_poly3(c):
    """Format ax³ + bx² + cx + d with x in thousands."""
    terms = []
    for i, (pw, coef) in enumerate(zip([3, 2, 1, 0], c)):
        if abs(coef) < 1e-6:
            continue
        # sign
        if i == 0 or not terms:
            s = '' if coef >= 0 else '-'
        else:
            s = ' + ' if coef >= 0 else ' - '
        v = abs(coef)
        # value + variable
        if pw == 0:
            terms.append(f"{s}{v:.1f}")
        elif pw == 1:
            terms.append(f"{s}{v:.2f}x")
        else:
            sup = '\u00b2' if pw == 2 else '\u00b3'
            terms.append(f"{s}{v:.2f}x{sup}")
    return 'y = ' + ''.join(terms)

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

# --- Latency ---
coeffs_lat = np.polyfit(x_norm, np.array(cfg_lat), 3)
p_lat2 = np.poly1d(coeffs_lat)
ax1.scatter(x, cfg_lat, s=80, zorder=5, color='#d62728', label='Измерения')
ax1.plot(x_fit, p_lat2(x_fit_norm), '-', color='#1f77b4', linewidth=2.5,
         label='Полином 3-й ст.')
eq_lat = fmt_poly3(coeffs_lat)
ax1.text(0.97, 0.95, f'{eq_lat}\n(x — тыс. подключений)',
         transform=ax1.transAxes, fontsize=11, verticalalignment='top',
         horizontalalignment='right',
         bbox=dict(boxstyle='round,pad=0.4', facecolor='wheat', alpha=0.8))
ax1.set_xlabel("Количество подключений", fontsize=12)
ax1.set_ylabel("Задержка, мс", fontsize=12)
ax1.set_title("L=2, W=8: Задержка", fontsize=14)
ax1.legend(fontsize=11, loc='center right')
ax1.grid(True, alpha=0.3)
ax1.xaxis.set_major_formatter(ticker.FuncFormatter(lambda v, _: f"{int(v/1000)}K"))

# --- Transfer ---
coeffs_tr2 = np.polyfit(x_norm, np.array(cfg_tr), 3)
p_tr2 = np.poly1d(coeffs_tr2)
ax2.scatter(x, cfg_tr, s=80, zorder=5, color='#2ca02c', label='Измерения')
ax2.plot(x_fit, p_tr2(x_fit_norm), '-', color='#1f77b4', linewidth=2.5,
         label='Полином 3-й ст.')
eq_tr = fmt_poly3(coeffs_tr2)
ax2.text(0.97, 0.95, f'{eq_tr}\n(x — тыс. подключений)',
         transform=ax2.transAxes, fontsize=11, verticalalignment='top',
         horizontalalignment='right',
         bbox=dict(boxstyle='round,pad=0.4', facecolor='wheat', alpha=0.8))
ax2.set_xlabel("Количество подключений", fontsize=12)
ax2.set_ylabel("Передача, МБ/с", fontsize=12)
ax2.set_title("L=2, W=8: Скорость передачи", fontsize=14)
ax2.legend(fontsize=11, loc='center right')
ax2.grid(True, alpha=0.3)
ax2.xaxis.set_major_formatter(ticker.FuncFormatter(lambda v, _: f"{int(v/1000)}K"))

plt.tight_layout()
path_2d = os.path.join(BENCH_DIR, "cfg_2_8_lat_transfer_poly3.png")
plt.savefig(path_2d, dpi=150)
plt.close()
print(f"Config (2,8) 2D poly3 plot saved to {path_2d}")

## ============ Config (2,8): 3D heatmap surface — degradation point ============
from scipy.interpolate import griddata

# Raw data for (2,8) from all runs
raw_28 = []
for key, runs in data.items():
    if key[0] == 2 and key[1] == 8:
        conn = key[2]
        for r in runs:
            raw_28.append((conn, r["lat"], r["rps"], r["transfer"]))

raw_28 = np.array(raw_28)
r_conn = raw_28[:, 0]
r_lat  = raw_28[:, 1]
r_rps  = raw_28[:, 2]
r_tr   = raw_28[:, 3]

r_conn = r_conn[0:-1:1]
r_lat = r_lat[0:-1:1]
r_rps  = r_rps[0:-1:1]
r_tr   = r_tr[0:-1:1]

# Smooth grid for surface
grid_conn = np.linspace(r_conn.min(), r_conn.max(), 100)
grid_lat  = np.linspace(r_lat.min(), r_lat.max(), 100)
G_conn, G_lat = np.meshgrid(grid_conn, grid_lat)

# Interpolate RPS onto the (connections, latency) grid
G_rps = griddata((r_conn, r_lat), r_rps, (G_conn, G_lat), method='cubic')

fig = plt.figure(figsize=(12, 9))
ax = fig.add_subplot(111, projection='3d')

# Surface with thermal colormap
surf = ax.plot_surface(G_conn, G_lat, G_rps,
                       cmap='hot', alpha=0.8, edgecolor='none',
                       antialiased=True)

# Scatter raw points on top
sc = ax.scatter(r_conn, r_lat, r_rps, c=r_rps, cmap='hot',
                s=50, edgecolors='black', linewidths=0.5, zorder=10)

# Degradation zone annotation — vertical plane at ~25K
deg_y = np.linspace(r_lat.min(), r_lat.max(), 50)
deg_z = np.linspace(0, r_rps.max(), 50)
DY, DZ = np.meshgrid(deg_y, deg_z)
DX = np.full_like(DY, 25000)
ax.plot_surface(DX, DY, DZ, alpha=0.12, color='blue', edgecolor='none')
ax.text(27000, r_lat.max(), r_rps.max() * 0.5,
        'Зона\nдеградации', fontsize=11, color='blue', fontweight='bold')

cbar = fig.colorbar(surf, ax=ax, shrink=0.55, pad=0.08)
cbar.set_label('Запросов/с', fontsize=11)

ax.set_xlabel('Подключения', fontsize=12, labelpad=10)
ax.set_ylabel('Задержка, мс', fontsize=12, labelpad=10)
ax.set_zlabel('Запросов/с', fontsize=12, labelpad=10)
ax.set_title('L=2, W=8: Тепловая карта производительности', fontsize=14, pad=15)
ax.xaxis.set_major_formatter(ticker.FuncFormatter(lambda v, _: f"{int(v/1000)}K"))
ax.view_init(elev=30, azim=-55)

plt.tight_layout()
heatmap_path = os.path.join(BENCH_DIR, "cfg_2_8_heatmap_3d.png")
plt.savefig(heatmap_path, dpi=150, bbox_inches='tight')
plt.close()
print(f"Config (2,8) 3D heatmap saved to {heatmap_path}")

print("\nAll done!")
