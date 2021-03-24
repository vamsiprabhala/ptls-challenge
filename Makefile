
# define the name of the virtual environment directory
VENV := .ptls-venv

# default target, when make executed without arguments
# all: .ptls-venv

$(VENV)/bin/activate: requirements.txt
	python3 -m venv $(VENV)
	./$(VENV)/bin/pip install -r requirements.txt

# venv is a shortcut target
venv: $(VENV)/bin/activate

run: venv
	./$(VENV)/bin/python3 ptls_challenge/app.py $(JSON_FILE_PATH)

clean:
	deactivate
	rm -rf $(VENV)
	find . -type f -name '*.pyc' -delete

.PHONY: all venv run clean