.PHONY: venv create populate update features train score evaluate daily initial all

sell_out_env/bin/activate: requirements.txt
	cd ~/sell_out_project; test -d sell_out_env || virtualenv sell_out_env
	. sell_out_env/bin/activate; pip install -r requirements.txt
	touch sell_out_env/bin/activate

venv: sell_out_env/bin/activate

create: 
	cd ~/sell_out_project; . sell_out_env/bin/activate; python src/create_database.py --config config/config.yml

<<<<<<< HEAD
ingest:
	cd ~/sell_out_project; . sell_out_env/bin/activate; python src/ingest_datapy --config config/config.yml

populate:
	cd ~/sell_out_project; . sell_out_env/bin/activate; python src/populate_database.py --config config/config.yml

update: 
	cd ~/sell_out_project; . sell_out_env/bin/activate; python src/update_database.py --config config/config.yml
=======
populate: 
	cd ~/sell_out_project; . sell_out_env/bin/activate; python src/populate_database.py --config config/config.yml

update: 
	cd ~/sell_out_project; . sell_out_env/bin/activate; python src/update_database.py --config config/config.yml --API_token ***REMOVED***
>>>>>>> 4554ca42774fcc879c9bda8a8fe2a7a92b648ff7

features: config/last_update.txt
	cd ~/sell_out_project; . sell_out_env/bin/activate; python src/generate_features.py --config config/config.yml

training: config/last_update.txt
	cd ~/sell_out_project; . sell_out_env/bin/activate; python src/train_model.py --config config/config.yml

score: models/classifier.pkl models/regressor.pkl
	cd ~/sell_out_project; . sell_out_env/bin/activate; python src/score_model.py --config config/config.yml

evaluate:

daily: update features training score evaluate

initial: venv create populate

all: initial daily
