all: intro.html intro_onepage.html

intro_onepage.html: intro.md
	pandoc -s -o intro_onepage.html intro.md

intro.html: intro.md
	pandoc -s --webtex -t slidy -o intro.html intro.md

clean:
	rm -rf intro.html intro_onepage.html
