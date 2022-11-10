MAIN = spec.tex

BASE := $(basename $(MAIN))

BIBFILES := $(wildcard *.bib)
STYFILES := $(wildcard *.sty)

GS_FONTPATH := $(shell x=`kpsewhich cmr10.pfb`; echo $$GS_FONTPATH:`dirname $$x`)
export GS_FONTPATH

#LATEX = pdflatex \\nonstopmode\\input
USEPDFLATEX = 1
PDFLATEX = pdflatex
LATEX = latex
BIBTEX = bibtex -min-crossrefs=1000
METAPOST = mpost
DVIPS := dvips -P pdf -j0 -G0 -t letter
PS2PDF := ps2pdf
PS2PDF_LETTER := GS_OPTIONS=-sPAPERSIZE=letter $(PS2PDF) -sPAPERSIZE=letter
PDF2PS := $(shell (which acroread >/dev/null 2>&1 && echo acroread -toPostScript) || echo pdf2ps)

PDFFIGS += examples-1.pdf \
	remove1-1.pdf remove1-2.pdf remove1-3.pdf remove1-31.pdf remove1-4.pdf remove1-5.pdf \
	remove1-6.pdf remove1-7.pdf remove1-8.pdf remove1-9.pdf \
	remove2-1.pdf remove2-2.pdf remove2-3.pdf remove2-4.pdf remove2-5.pdf \
	remove2-6.pdf remove2-7.pdf remove2-8.pdf remove2-9.pdf \
	remove2-11.pdf remove2-12.pdf remove2-15.pdf remove2-16.pdf \
	insert1-1.pdf insert1-2.pdf insert1-3.pdf insert1-4.pdf insert1-5.pdf

RERUN = egrep -q '(^LaTeX Warning:|\(natbib\)).* Rerun'
UNDEFINED = egrep -q '^(LaTeX|Package natbib) Warning:.* undefined'

RERUNLATEX = if test ! -f $(2).bbl || $(RERUN) $(2).log || $(UNDEFINED) $(2).log; then \
		set -x; $(BIBTEX) $(2); $(1); \
		! $(RERUN) $(2).log || $(1); \
		! $(RERUN) $(2).log || $(1); \
	fi

%.dvi: %.ltx
	test ! -s $*.aux || $(BIBTEX) $* || rm -f $*.aux $*.bbl
	$(LATEX) $<
	@$(call RERUNLATEX,$(LATEX) $<,$*)

%.ps: %.dvi
	$(DVIPS) $< -o $@

%.pdf: %.ps
	$(PS2PDF_LETTER) $<

ifeq ($(USEPDFLATEX),1)
%.pdf: %.tex
	test ! -s $*.aux || $(BIBTEX) $* || rm -f $*.aux $*.bbl
	$(PDFLATEX) $<
	@$(call RERUNLATEX,$(PDFLATEX) $<,$*)
endif

all: $(BASE).pdf

$(BASE).dvi $(BASE).pdf: $(TEXFILES) $(STYFILES) $(BIBFILES) $(PDFFIGS)

bib:
	$(LATEX) $(MAIN)
	rm -f $(BASE).dvi
	$(MAKE) $(BASE).dvi

%-1.pdf %-2.pdf %-3.pdf %-4.pdf %-5.pdf %-6.pdf %-7.pdf %-8.pdf %-9.pdf \
%-10.pdf %-11.pdf %-12.pdf %-13.pdf %-14.pdf %-15.pdf %-16.pdf %-17.pdf %-18.pdf %-19.pdf \
%-20.pdf %-21.pdf %-22.pdf %-23.pdf %-24.pdf %-25.pdf %-26.pdf %-27.pdf %-28.pdf %-29.pdf \
%-30.pdf %-31.pdf %-32.pdf %-33.pdf %-34.pdf %-35.pdf %-36.pdf %-37.pdf %-38.pdf %-39.pdf \
: %.mp elements.mp elemfig.sty patches.mp masstree.mp
	rm -f $*.[0-9] $*.[0-9][0-9] $*.[0-9][0-9][0-9]
	TEX=$(LATEX) $(METAPOST) $<
	mptopdf `ls $*.* | grep -e '\.[0-9][0-9]*$$'`

clean:
	rm -f $(EPSFIGS) $(PDFFIGS) $(EPSGRAPHS) $(PDFGRAPHS)
	rm -f *.ps $(BASE).pdf *.aux
	rm -f *.dvi *.aux *.log *.bbl *.blg *.lof *.lot *.toc *.bak *.[1-9] *.[1-3][0-9] *.mpout *~

.PHONY: clean bib all
