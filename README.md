# Codding Chalange

## Prepair Environment
I choose [POETRY](https://python-poetry.org/docs/) to manage project, because it stores all dependencies of the packages with the versions so I can be sure that created environment will be exactly the same.

I choose `flake8` as `linter` and `black` to make code cleaner.

After cloning repository

setup of poetry needs to be done:
```
poetry install
poetry shell
```

For running taskx_x:
```
cd src/
python taskx_x.py
```

all input files are saved in `in` folder
all outpout files as saved in `out` folder

possible extensions possible:
* split code with functions for more reusability
* create documentation using mkdocs for uploading to gihub pages from doocstrings
* create wheel package
* add github actions for easy deployment
* in case of use on databricks use dbx https://github.com/databrickslabs/dbx
* add `pytest` testing with coverage

## aditional notes for tasks:
## Task2_5 - Apache Airflow

Apache Airflow 2.x is used for this task. Below there are diagrams for 2 alternative solutions, second was used in a code for readabilty

### without groups
![airflow2_without_groups](/assets/img/airflow2_without_groups.png "Airflow without groups")

### with groups
![airflow2_with_groups](/assets/img/airflow2_with_groups.png "Airflow with groups")
