#!/bin/bash

$ aws emr create-default-roles

$ aws emr create-cluster \
    --name 'capstone-cluster-20170802' \
    --region eu-west-1 \
    --release-label emr-5.7.0 \
    --use-default-roles \
    --tags 'capstone=true' \
    --no-termination-protected \
  --no-enable-debugging \
    --auto-scaling-role EMR_AutoScaling_DefaultRole \
    --applications Name=Hadoop Name=Hive Name=Pig Name=Hue \
    --ec2-attributes 'KeyName=id_rsa.jkielbaey-20160502,AdditionalSlaveSecurityGroups=[sg-47e7273f],SubnetId=subnet-7ce0f425,EmrManagedSlaveSecurityGroup=sg-988b31e0,EmrManagedMasterSecurityGroup=sg-3b952f43,AdditionalMasterSecurityGroups=[sg-47e7273f]' \
    --log-uri 's3n://jkielbaey-capstone-eu-west-1/EMR/' \
    --instance-groups \
        'InstanceGroupType=MASTER,Name=Master,InstanceCount=1,InstanceType=m4.large,EbsConfiguration={EbsOptimized=true,EbsBlockDeviceConfigs=[{VolumeSpecification={VolumeType=gp2,SizeInGB=32},VolumesPerInstance=1}]}' \
        'InstanceGroupType=CORE,Name=Core,InstanceCount=5,InstanceType=c4.2xlarge,EbsConfiguration={EbsOptimized=true,EbsBlockDeviceConfigs=[{VolumeSpecification={VolumeType=gp2,SizeInGB=100},VolumesPerInstance=1}]}' \
    --configurations file://emr-configurations.json \
    --scale-down-behavior TERMINATE_AT_INSTANCE_HOUR

$ aws emr add-steps --cluster-id j-V5SERYYW830T --steps file://load_s3_hdfs_hive.json
