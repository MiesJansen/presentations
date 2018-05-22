#!/bin/bash

time curl --data-binary @sample_data.csv http://localhost:8001/product_data/put
curl -H @/Users/mj/.crystal_keys  http://localhost:3000/api/update_all_product_fields
