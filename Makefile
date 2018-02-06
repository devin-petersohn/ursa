define help

Supported targets: prepare, develop, clean, and test.

The 'prepare' target installs Ursa's build requirements into the current virtualenv.

The 'develop' target creates an editable install of Ursa and its runtime requirements.

The 'clean' target undoes the effect of 'develop'.

The 'test' target runs Ursa's unit tests.

endef
export help
help:
	@printf "$$help"

# This Makefile uses bash features like printf and <()
SHELL=bash
python=python # version should be set by virtualenv
python3=$(shell (python -c 'import sys; print("%i" % (sys.hexversion<0x03000000))'))
pip=pip
extras=
current_commit:=$(shell git log --pretty=oneline -n 1 -- $(pwd) | cut -f1 -d " ")
dirty:=$(shell (git diff --exit-code && git diff --cached --exit-code) > /dev/null || printf -- --DIRTY)
raysource=(git+https://github.com/ray-project/ray.git#subdirectory=python)

green=\033[0;32m
normal=\033[0m\n
red=\033[0;31m


develop:
	$(pip) install -e .$(extras)

clean_develop:
	- $(pip) uninstall -y ursa
	- rm -rf *.egg-info

test:
	./test_script.sh

clean: clean_develop

check_build_reqs:
	@$(python) -c 'import pytest' \
	@$(python) -c 'import ray' \
		|| ( printf "$(redpip)Build requirements are missing. Run 'make prepare' to install them.$(normal)" ; false )

prepare:
	@if [ $(python3) = 0 ]; then\
		$(pip) install flake8; \
	fi
	$(pip) install pytest==2.8.3
	$(pip) install cython
	$(pip) install cmake
	$(pip) install ray

.PHONY: help \
		prepare \
		develop clean_develop \
		test \
		clean \
		check_build_reqs
