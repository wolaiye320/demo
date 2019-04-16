#!/bin/bash
i=1
for i in $(seq 1 60)
    do
        customernum=`openssl rand -base64 8 | cksum | cut -c1-8`
        pricenum=`openssl rand -base64 8 | cksum | cut -c1-4`
        citynum=`openssl rand -base64 8 | cksum | cut -c1-2`
        itemnum=`openssl rand -base64 8 | cksum | cut -c1-6`
        echo "customer"$customernum","$pricenum",""city"$citynum",""item"$itemnum >> /opt/custom/double11.log
        sleep 1
    done