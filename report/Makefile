TEX_DIR = Tex/tex
MAIN    = iu7-71bv_rodinkov
LATEX   = xelatex -interaction=nonstopmode -halt-on-error
BIBER   = biber

.PHONY: all clean bib



all: build clean

build:
	cd $(TEX_DIR) && $(LATEX) $(MAIN).tex
	cd $(TEX_DIR) && $(BIBER) $(MAIN)
	cd $(TEX_DIR) && $(LATEX) $(MAIN).tex
	cd $(TEX_DIR) && $(LATEX) $(MAIN).tex
	cp $(TEX_DIR)/$(MAIN).pdf .
	xdg-open iu7-71bv_rodinkov.pdf


clean:
	find $(TEX_DIR) -maxdepth 1 -type f ! -name '*.tex' ! -name '*.bib' -delete
	
	
