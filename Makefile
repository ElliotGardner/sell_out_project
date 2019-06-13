.PHONY: venv create ingest populate update features train score evaluate test daily initial all

sell_out_env/bin/activate: requirements.txt
	test -d sell_out_env || virtualenv sell_out_env
	. sell_out_env/bin/activate; pip install -r requirements.txt
	touch sell_out_env/bin/activate

venv: sell_out_env/bin/activate

create: 
	. sell_out_env/bin/activate; python run.py create --config config/config.yml

ingest:
	. sell_out_env/bin/activate; python run.py ingest --config config/config.yml

populate:
	. sell_out_env/bin/activate; python run.py populate --config config/config.yml

update: 
	. sell_out_env/bin/activate; python run.py update --config config/config.yml

features: config/last_update.txt
	. sell_out_env/bin/activate; python run.py features --config config/config.yml

training: config/last_update.txt
	. sell_out_env/bin/activate; python run.py train --config config/config.yml

score: models/classifier.pkl models/regressor.pkl
	. sell_out_env/bin/activate; python run.py score --config config/config.yml

evaluate:
	. sell_out_env/bin/activate; python run.py evaluate --config config/config.yml

test:
	. sell_out_env/bin/activate; py.test

initial: venv create populate

daily: update features training score evaluate

all: initial daily
