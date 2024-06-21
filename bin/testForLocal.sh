#!/bin/bash

bash simulation.sh 
bash runClient.sh 

sleep 100

bash closeClient.sh
bash stop.sh