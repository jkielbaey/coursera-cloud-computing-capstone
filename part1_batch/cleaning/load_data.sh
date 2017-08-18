#!/bin/bash

# Script to download RAW data from S3 to /DATA.
DATA_DIR=/data
DATA_DEVICE=/dev/xvdb

if ! test -b $DATA_DEVICE; then
  echo "Error! Data device $DATA_DEVICE is missing." >&2
  exit 1
fi

test -d $DATA_DIR || sudo mkdir $DATA_DIR

if ! mountpoint $DATA_DIR >/dev/null; then
  echo "Directory $DATA_DIR does not contain a mounted file system."
  sudo blkid $DATA_DEVICE | grep -qi ext4
  [ $? -ne 0 ] && sudo mkfs -t ext4 $DATA_DEVICE
  [ $? -ne 0 ] && echo "Error! Failed to create file system." && exit 1
  sudo mount $DATA_DEVICE $DATA_DIR
fi

test -d $DATA_DIR/lost+found && sudo rmdir $DATA_DIR/lost+found

sudo chown ec2-user:ec2-user $DATA_DIR
for d in RAW UNZIP CLEAN; do
  test -d $DATA_DIR/$d || mkdir $DATA_DIR/$d
done

aws configure set default.s3.max_concurrent_requests 20

aws s3 sync s3://jkielbaey-capstone-eu-west-1/RAW $DATA_DIR/RAW
