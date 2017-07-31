#!/bin/bash

aws emr create-default-roles

aws emr create-cluster \
  --name 'capstone-cluster' \
  --region eu-west-1 \
  --release-label emr-5.7.0 \
  --use-default-roles \
  --tags 'capstone=true' 'Name=capstone-cluster' \
  --no-termination-protected \
  --log-uri 's3://jkielbaey-capstone-eu-west-1/EMR/' \
  --no-enable-debugging \
  --applications Name=Hadoop Name=Hive Name=Pig Name=Hue \
  --auto-scaling-role EMR_AutoScaling_DefaultRole \
  --ec2-attributes 'KeyName=id_rsa.jkielbaey-20160502,SubnetId=subnet-7ce0f425,AdditionalSlaveSecurityGroups=[sg-47e7273f],EmrManagedSlaveSecurityGroup=sg-329d334a,EmrManagedMasterSecurityGroup=sg-b09f31c8,AdditionalMasterSecurityGroups=[sg-47e7273f]' \
  --steps file://load_s3_hdfs_hive.json \
  --configurations file://emr-configurations.json \
  --instance-groups \
      'InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m4.large' \
      'InstanceGroupType=CORE,InstanceCount=5,InstanceType=c4.2xlarge,EbsConfiguration={EbsOptimized=true,EbsBlockDeviceConfigs=[{VolumeSpecification={VolumeType=gp2,SizeInGB=100}}]}' \
  --scale-down-behavior TERMINATE_AT_INSTANCE_HOUR
