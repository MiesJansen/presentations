#!/bin/bash

time curl --data-binary @sample_data.csv http://localhost:8000/product_data/put
