#!/usr/bin/env python3

import os, sys, json

sys.path.append(os.path.join(os.getcwd(), "build/lib/recipe_scrapers"))

from recipe_scrapers import scrape_me


r = scrape_me("https://www.allrecipes.com/recipe/158968/spinach-and-feta-turkey-burgers/")
# print(json.dumps(r.nutrients(), indent=4, sort_keys=True))
# print(json.dumps(r.nutrients_unitized(), indent=4, sort_keys=True))
print(json.dumps(
    r.to_dict(unitized=True, skip_attribs="links"), indent=4, sort_keys=True))

"""
via: https://hub.docker.com/_/mongo/

docker pull mongo

EXTERNAL_MONGO_DATA_DIR=/Users/kev/projs/allrecipes-mongo
INTERNAL_MONGO_DATA_DIR=/data/db

$ docker run --name kevs-mongo -v \
  $EXTERNAL_MONGO_DATA_DIR:$INTERNAL_MONGO_DATA_DIR -d mongo
8ea6723c2a2640ccbba6134a12f38f7746156ef1cab4474767651ee26ec5dfc1

$ docker exec -it 8ea6723c2a26 bash  # get shell in that container
$ ls -l $INTERNAL_MONGO_DATA_DIR
abc
$ ls -l $$EXTERNAL_MONGO_DATA_DIR
abc

abc == abc
"""
