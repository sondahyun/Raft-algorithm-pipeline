#!/bin/bash

bash simulation.sh
bash runClient.sh

sleep 5

bash closeClient.sh
bash stop.sh