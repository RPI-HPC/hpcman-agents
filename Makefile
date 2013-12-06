# Some operations we frequently need.
# NB: Might eventually be worth autoconfiscating.

INSTALLDIR = /opt/hpcman/agents

clean:
	rm -f *~ hpcagent/*~
	rm -f *.pyc *.pyo hpcagent/*.pyc hpcagent/*.pyo
	rm -f TAGS hpcagent/TAGS


install:
	mkdir -p $(INSTALLDIR)
	cp hdagent.py ldapagent.py proxyagent.py $(INSTALLDIR)
	mkdir -p $(INSTALLDIR)/hpcagent
	cp hpcagent/*.py $(INSTALLDIR)/hpcagent


tags: TAGS
TAGS: *.py hpcagent/*.py Makefile
	(cd hpcagent && ctags -e *.py)
	ctags -e --etags-include=hpcagent/TAGS *.py Makefile
