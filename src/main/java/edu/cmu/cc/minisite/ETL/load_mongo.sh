#!/bin/sh -x

# Download the data file
gsutil cp gs://cmucc-public/dataset/reddit/0120data/posts.json .

# Load the data file into MongoDB
# 1. Database name    -- reddit_db
# 2. Collection name  -- posts
mongoimport --db reddit_db --collection posts --drop --file posts.json

