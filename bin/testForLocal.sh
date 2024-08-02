#!/bin/bash

bash simulation.sh 
bash runClient.sh 

sleep 30

bash closeClient.sh
bash stop.sh