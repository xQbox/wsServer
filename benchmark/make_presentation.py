#!/usr/bin/env python3
"""Generate course work presentation as .pptx"""
import os
from pptx import Presentation
from pptx.util import Inches, Pt, Emu
from pptx.enum.text import PP_ALIGN, MSO_ANCHOR
from pptx.dml.color import RGBColor

BENCH_DIR = os.path.dirname(os.path.abspath(__file__))
HEATMAP = os.path.join(BENCH_DIR, "cfg_2_8_heatmap_3d.png")
OUT = os.path.join(BENCH_DIR, "presentation.pptx")

# Colors
BG = RGBColor(0xFF, 0xFF, 0xFF)
BLACK = RGBColor(0x1A, 0x1A, 0x1A)
TITLE_COLOR = RGBColor(0x33, 0x33, 0x33)
SUBTITLE = RGBColor(0x66, 0x66, 0x66)

prs = Presentation()
prs.slide_width = Inches(13.333)
prs.slide_height = Inches(7.5)

def add_bg(slide, color=BG):
    bg = slide.background
    fill = bg.fill
    fill.solid()
    fill.fore_color.rgb = color

def add_text(slide, left, top, width, height, text, size=18,
             bold=False, color=BLACK, align=PP_ALIGN.LEFT, spacing=1.2):
    txBox = slide.shapes.add_textbox(Inches(left), Inches(top),
                                      Inches(width), Inches(height))
    tf = txBox.text_frame
    tf.word_wrap = True
    p = tf.paragraphs[0]
    p.text = text
    p.font.size = Pt(size)
    p.font.bold = bold
    p.font.color.rgb = color
    p.alignment = align
    p.space_after = Pt(size * (spacing - 1))
    return tf

def add_bullet_slide(slide, left, top, width, height, items, size=17,
                     color=BLACK, bold_prefix=False):
    txBox = slide.shapes.add_textbox(Inches(left), Inches(top),
                                      Inches(width), Inches(height))
    tf = txBox.text_frame
    tf.word_wrap = True
    for i, item in enumerate(items):
        if i == 0:
            p = tf.paragraphs[0]
        else:
            p = tf.add_paragraph()
        p.space_after = Pt(6)
        p.space_before = Pt(2)

        if bold_prefix and ":" in item:
            prefix, rest = item.split(":", 1)
            run1 = p.add_run()
            run1.text = prefix + ":"
            run1.font.size = Pt(size)
            run1.font.bold = True
            run1.font.color.rgb = color
            run2 = p.add_run()
            run2.text = rest
            run2.font.size = Pt(size)
            run2.font.color.rgb = color
        else:
            run = p.add_run()
            run.text = item
            run.font.size = Pt(size)
            run.font.color.rgb = color
    return tf

def title_bar(slide, text):
    add_text(slide, 0.6, 0.3, 12, 0.8, text, size=28, bold=True,
             color=TITLE_COLOR)

# ====== Slide 1: Title ======
s = prs.slides.add_slide(prs.slide_layouts[6])  # blank
add_bg(s)
add_text(s, 1, 1.5, 11.3, 1.2,
         "Разработка статического HTTP-сервера",
         size=36, bold=True, color=BLACK, align=PP_ALIGN.CENTER)
add_text(s, 1, 2.9, 11.3, 0.6,
         "Курсовая работа по дисциплине «Компьютерные сети»",
         size=20, color=SUBTITLE, align=PP_ALIGN.CENTER)
add_text(s, 1, 4.2, 11.3, 0.5,
         "Студент: Родинков А. Г.    Группа: ИУ7-71БВ",
         size=18, color=BLACK, align=PP_ALIGN.CENTER)
add_text(s, 1, 4.8, 11.3, 0.5,
         "Руководитель: Клочков М. Н.",
         size=18, color=BLACK, align=PP_ALIGN.CENTER)
add_text(s, 1, 6.2, 11.3, 0.5,
         "МГТУ им. Н. Э. Баумана - 2026",
         size=16, color=SUBTITLE, align=PP_ALIGN.CENTER)

# ====== Slide 2: Goal & Tasks ======
s = prs.slides.add_slide(prs.slide_layouts[6])
add_bg(s)
title_bar(s, "Цель и задачи")

add_text(s, 0.8, 1.4, 11.5, 0.8,
         "Цель — разработка многопоточного сервера для отдачи "
         "статического содержимого с диска по протоколу HTTP.",
         size=20, bold=True, color=BLACK)

add_text(s, 0.8, 2.5, 11.5, 0.5, "Задачи:", size=20, bold=True, color=BLACK)

add_bullet_slide(s, 1.0, 3.2, 11, 3.5, [
    "1. Проанализировать архитектуру компьютерной сети, протоколы и системные вызовы",
    "2. Описать алгоритмы работы основных компонентов сервера",
    "3. Реализовать сервер на языке C",
    "4. Провести нагрузочное тестирование и определить оптимальную конфигурацию",
], size=19)

# ====== Slide 3: Analysis ======
s = prs.slides.add_slide(prs.slide_layouts[6])
add_bg(s)
title_bar(s, "Аналитический раздел")

add_bullet_slide(s, 0.8, 1.4, 11.5, 5.5, [
    "Сокеты: конечная точка взаимодействия в модели TCP/IP, файловый дескриптор в Linux",
    "TCP: надёжный протокол с установлением соединения (трёхстороннее рукопожатие)",
    "HTTP: протокол прикладного уровня, модель запрос-ответ (GET, POST, HEAD, ...)",
    "Потоки: модель 1:1 (NPTL) — каждому пользовательскому потоку соответствует поток ядра",
    "Мультиплексирование: select/poll — O(n); epoll — O(k), где k — готовые дескрипторы",
    "Вывод: epoll — предпочтительный механизм для многопоточного сервера",
], size=18, bold_prefix=True)

# ====== Slide 4: Architecture ======
s = prs.slides.add_slide(prs.slide_layouts[6])
add_bg(s)
title_bar(s, "Архитектура сервера")

add_bullet_slide(s, 0.8, 1.4, 5.8, 5, [
    "Главный поток: инициализация, создание пулов, ожидание SIGTERM/SIGINT",
    "Потоки-слушатели (L шт.): accept() + epoll, передача задач в очередь",
    "Потоки-обработчики (W шт.): извлечение из очереди, конечный автомат",
], size=17, bold_prefix=True)

add_text(s, 0.8, 4.3, 5.8, 0.5, "Конечный автомат:", size=17, bold=True, color=BLACK)
add_text(s, 0.8, 4.9, 5.8, 0.5,
         "READING → SENDING_HEADERS → SENDING_BODY → DONE",
         size=16, color=TITLE_COLOR)

add_text(s, 7.2, 1.4, 5.5, 0.5, "Ключевые решения:", size=19, bold=True, color=BLACK)
add_bullet_slide(s, 7.2, 2.1, 5.5, 4.5, [
    "SO_REUSEPORT — балансировка соединений ядром между слушателями",
    "EPOLLONESHOT + EPOLLET — исключение гонок между потоками",
    "sendfile() — zero-copy передача файлов из ядра",
    "eventfd — механизм корректного завершения потоков",
], size=16)

# ====== Slide 5: Implementation ======
s = prs.slides.add_slide(prs.slide_layouts[6])
add_bg(s)
title_bar(s, "Средства реализации")

add_bullet_slide(s, 0.8, 1.4, 11.5, 5, [
    "Язык: C (стандарт C17)",
    "Компилятор: GCC, флаги -Wall -Wextra -Wpedantic -std=c17",
    "Системные вызовы: epoll_create1, epoll_ctl, epoll_wait, accept, recv, send, sendfile",
    "Потоки: POSIX Threads (pthread_create, pthread_join)",
    "Сборка: GNU Make",
    "Платформа: Linux 6.8, Intel Core i9-11900KF (8 ядер / 16 потоков)",
], size=19, bold_prefix=True)

# ====== Slide 6: Test methodology ======
s = prs.slides.add_slide(prs.slide_layouts[6])
add_bg(s)
title_bar(s, "Методика нагрузочного тестирования")

add_bullet_slide(s, 0.8, 1.4, 5.5, 3, [
    "Инструмент: wrk (8 потоков)",
    "Длительность прогона: 3 с",
    "Прогонов на комбинацию: 10",
    "Ресурс: статическая HTML-страница",
], size=18, bold_prefix=True)

add_text(s, 0.8, 3.8, 5.5, 0.5,
         "Уровни нагрузки: 10K, 20K, 40K, 80K, 100K подключений",
         size=17, color=BLACK)

# Table of configs
add_text(s, 7, 1.4, 5.5, 0.5, "Конфигурации сервера:", size=19, bold=True, color=BLACK)

tbl_data = [
    ["Конф.", "1", "2", "3", "4", "5", "6"],
    ["L", "2", "2", "2", "3", "3", "4"],
    ["W", "2", "4", "8", "4", "8", "4"],
]
rows, cols = 3, 7
tbl = s.shapes.add_table(rows, cols, Inches(7), Inches(2.2),
                          Inches(5.5), Inches(1.5)).table
for r in range(rows):
    for c in range(cols):
        cell = tbl.cell(r, c)
        cell.text = tbl_data[r][c]
        cell.fill.solid()
        cell.fill.fore_color.rgb = RGBColor(0xE0, 0xE0, 0xE0) if r == 0 else BG
        for p in cell.text_frame.paragraphs:
            p.font.size = Pt(15)
            p.font.color.rgb = BLACK
            p.alignment = PP_ALIGN.CENTER

add_text(s, 7, 4.2, 5.5, 1.5,
         "Метрики: пропускная способность (req/s), "
         "средняя задержка (мс), скорость передачи (МБ/с)",
         size=17, color=BLACK)

# ====== Slide 7: Heatmap results ======
s = prs.slides.add_slide(prs.slide_layouts[6])
add_bg(s)
title_bar(s, "Результаты: тепловая карта (L=2, W=8)")

if os.path.exists(HEATMAP):
    s.shapes.add_picture(HEATMAP, Inches(0.3), Inches(1.2),
                         height=Inches(5.8))

add_bullet_slide(s, 7.2, 1.4, 5.5, 5, [
    "10K подкл.: 168 414 req/s, 57 мс",
    "20K подкл.: 71 480 req/s, 69 мс",
    "",
    "Точка деградации: ~25 000 подключений",
    "RPS падает в 19 раз (71K → 3.7K)",
    "",
    "Причина: исчерпание ресурсов ядра ОС на обслуживание файловых дескрипторов",
], size=17, bold_prefix=True)

# ====== Slide 8: Research conclusions ======
s = prs.slides.add_slide(prs.slide_layouts[6])
add_bg(s)
title_bar(s, "Выводы исследования")

add_bullet_slide(s, 0.8, 1.4, 11.5, 5.5, [
    "1. Увеличение обработчиков (W: 2→8) повышает RPS на 77% и снижает задержку на 43%",
    "2. Увеличение слушателей свыше 2 ухудшает производительность в 2–3 раза (конкуренция за SO_REUSEPORT)",
    "3. При нагрузке >40 000 подключений узкое место — ограничения ОС, а не архитектура потоков",
    "4. Оптимальная конфигурация: L=2, W=8 — максимальный RPS и минимальная задержка",
], size=20)

# ====== Slide 9: Results ======
s = prs.slides.add_slide(prs.slide_layouts[6])
add_bg(s)
title_bar(s, "Результаты работы")

add_bullet_slide(s, 0.8, 1.4, 11.5, 5.5, [
    "1. Проанализированы архитектура компьютерной сети, протоколы TCP/HTTP и системные вызовы Linux",
    "2. Спроектированы алгоритмы главного потока, потоков-слушателей и потоков-обработчиков",
    "3. Реализован многопоточный HTTP-сервер на языке C (epoll + sendfile + POSIX Threads)",
    "4. Проведено нагрузочное тестирование: 6 конфигураций, 5 уровней нагрузки, 10 прогонов",
    "5. Определена оптимальная конфигурация: 2 слушателя, 8 обработчиков",
], size=20)

prs.save(OUT)
print(f"Presentation saved to {OUT}")
