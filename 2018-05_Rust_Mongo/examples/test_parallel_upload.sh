#!/usr/bin/env bash
set -e
#set -x

#mongod --dbpath /tmp/mongo
#RUST_BACKTRACE=1 ROCKET_PORT=8000 RUST_LOG=crystal_micro_server=trace cargo run
#RUST_BACKTRACE=1 ROCKET_PORT=8001 RUST_LOG=crystal_micro_server=trace cargo run

parallel_upload_attempts=62
num_rand_columns=5
num_rows_of_dummy_data=1000
col_prefix="col"
DELIMITER=","
CMS_SERVER_PORTS=(8000 8001)
keyfield="bond"

#34 seconds for 2 cols, 100 rows

num_cores=$(getconf _NPROCESSORS_ONLN)

function add_header {
    num_cols=$1
    header=$keyfield
    for i in `seq 1 $num_cols`
    do
        header="${header}${DELIMITER}${col_prefix}${i}"
    done
    echo ${header}
}

function get_product_row {
    id=$1
    num_cols=$2

    data="${id}"

    for i in `seq 1 $num_cols`
    do
        #rand_num=$(LC_CTYPE=C tr -dc 0-9 < /dev/urandom | fold -w 8 | head -n 1)
        rand_num=32579289
        data="${data}${DELIMITER}${rand_num}"
    done
    echo ${data}
}

num_rows=0
for i in `seq 1 $parallel_upload_attempts`;
do
    product_data="$(add_header $num_rand_columns)"
    #product_id=$(LC_CTYPE=C tr -dc a-z < /dev/urandom | fold -w 8 | head -n 1)
    product_id="bond_id"
    for j in `seq 1 $num_rows_of_dummy_data`;
    do
        product_data="$product_data
$(get_product_row $product_id $num_rand_columns)"
    done

    # parallel upload the data
    for port in ${CMS_SERVER_PORTS[@]}
    do
        echo "connecting to port: $port"
        for i in `seq 1 $num_cores`;
        do
            num_rows=$((num_rows + num_rows_of_dummy_data))
           curl -H @/Users/mj/.crystal_keys --data "$product_data"  http://localhost:${port}/product_data/put &
        done
    done
done
echo "jobs sent!"
echo "inserting: $num_rows"