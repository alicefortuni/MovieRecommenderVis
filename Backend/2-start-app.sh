#!/bin/bash 

#START NEO4J
sudo service neo4j start

#OPEN APP 
file="$MOVIE_FRONTEND_DIR/index.html"
xdg-open "$file"