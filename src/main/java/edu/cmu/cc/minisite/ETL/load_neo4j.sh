#!/bin/sh -x

# connect to the Neo4j VM as the user `clouduser`
gcloud compute --project wenhe-gcp-social-network ssh --zone us-east1-b clouduser@neo4j-vm

# download the csv files
sudo gsutil cp gs://cmucc-public/dataset/reddit/0120data/users.csv .  
sudo gsutil cp gs://cmucc-public/dataset/reddit/0120data/links.csv . 

# stop the neo4j server first
sudo service neo4j stop

# purge the existing default database named as graph.db
# as the data import tool can only import to a new database
sudo rm -rf /var/lib/neo4j/data/databases/graph.db

# define the column header for users.csv and links.csv

# username is a unique identifier
echo 'username:ID,password,url' > users_header.csv

# be careful of the direction here
# the schema of users.csv is "[Followee, Follower]"
# and "Follower" follows "Followee"
echo ':END_ID,:START_ID' > links_header.csv    

# import the dataset as the root user; the graph.db will be owned by the root user
# and cannot use accessed by neo4j until you run the next `chown` command
sudo neo4j-admin import --database=graph.db --nodes:User "users_header.csv,users.csv" --relationships:FOLLOWS "links_header.csv,links.csv"    
# once the import is finished, you can see a report as follows:
# ...    
# IMPORT DONE in 4m 1s 303ms. 
# Imported:
#   2611448 nodes
#   16024501 relationships
#   7834340 properties
# Peak memory usage: 541.14 MB

# change the ownership of the database to neo4j
sudo chown -R neo4j:adm /var/lib/neo4j/data/databases/graph.db

# do not forget to start the neo4j service, note that this can take 1-2 minutes
sudo service neo4j start

# run the following command, and make sure you can see the log "Started"
sudo service neo4j status

