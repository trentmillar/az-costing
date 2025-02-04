.PHONY: setup clean run

# Python version and venv settings
PYTHON = python3
VENV = venv
VENV_BIN = $(VENV)/bin

setup: $(VENV)/bin/activate

$(VENV)/bin/activate:
	$(PYTHON) -m venv $(VENV)
	$(VENV_BIN)/pip install --upgrade pip
	$(VENV_BIN)/pip install -r requirements.txt

run: setup
	$(VENV_BIN)/python subscription_costs.py

clean:
	rm -rf $(VENV)
	rm -f subscription_costs_*.xlsx 
