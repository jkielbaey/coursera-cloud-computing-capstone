#!/bin/bash

DATE=`date +%Y%m%d_%H%M%S`
aws emr create-default-roles

aws emr create-cluster \
    --name "spark-cluster-${DATE}" \
    --region eu-west-1 \
    --release-label emr-5.8.0 \
    --use-default-roles \
    --tags 'Name=Spark Cluster' \
    --no-termination-protected \
    --no-enable-debugging \
    --auto-scaling-role EMR_AutoScaling_DefaultRole \
    --applications Name=Hadoop Name=Spark Name=Zeppelin Name=Ganglia \
    --ec2-attributes 'KeyName=id_rsa.jkielbaey-20160502,AdditionalSlaveSecurityGroups=[sg-47e7273f],SubnetId=subnet-7ce0f425,EmrManagedSlaveSecurityGroup=sg-988b31e0,EmrManagedMasterSecurityGroup=sg-3b952f43,AdditionalMasterSecurityGroups=[sg-47e7273f]' \
    --log-uri 's3n://jkielbaey-capstone-eu-west-1/EMR/' \
    --instance-groups \
        'InstanceGroupType=MASTER,Name=Master,InstanceCount=1,InstanceType=m4.xlarge,EbsConfiguration={EbsOptimized=true,EbsBlockDeviceConfigs=[{VolumeSpecification={VolumeType=gp2,SizeInGB=32},VolumesPerInstance=1}]}' \
        'InstanceGroupType=CORE,Name=Core,InstanceCount=5,BidPrice=0.15,InstanceType=r4.2xlarge,EbsConfiguration=
  {EbsOptimized=true,EbsBlockDeviceConfigs=[{VolumeSpecification={VolumeType=gp2,SizeInGB=32},VolumesPerInstance=1}]}' \
    --scale-down-behavior TERMINATE_AT_INSTANCE_HOUR
