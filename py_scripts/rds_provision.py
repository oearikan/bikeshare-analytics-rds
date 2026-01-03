import os
import boto3
import requests
from botocore.exceptions import WaiterError, ClientError

def create_rds(inst_name, reg_name):
    """Create a PostgreSQL RDS instance on AWS. Below I have my own VpcSecurityGroupIds=["sg-0819e0671ef09fce5"], DBSubnetGroupName="default-vpc-0a00bcd32514bc8e8" You have to find your own and replace those values."""

    rds = boto3.client("rds", region_name=reg_name)
    print(f"Attempting to create RDS instance named {inst_name}...")

    try:
        rds.create_db_instance(
            DBInstanceIdentifier=inst_name,
            DBName=inst_name,
            DBInstanceClass="db.t4g.micro",
            Engine="postgres",
            EngineVersion="17.6",
            MasterUsername="postgres",
            MasterUserPassword=os.environ["PGPW"],  # required
            AllocatedStorage=20,
            MaxAllocatedStorage=25,
            StorageType="gp2",
            StorageEncrypted=True,
            BackupRetentionPeriod=1,
            PreferredBackupWindow="07:45-08:15",
            PreferredMaintenanceWindow="mon:05:44-mon:06:14",
            MultiAZ=False,
            PubliclyAccessible=True,
            AutoMinorVersionUpgrade=True,
            VpcSecurityGroupIds=["sg-0819e0671ef09fce5"],
            DBSubnetGroupName="default-vpc-0a00bcd32514bc8e8",
            DBParameterGroupName="default.postgres17",
            DeletionProtection=False,
            CopyTagsToSnapshot=True,
            NetworkType="IPV4",
        )

        print(f"RDS instance {inst_name} creation initiated...")
    
    except ClientError as e:
        code = e.response["Error"]["Code"]

        if code == "DBInstanceAlreadyExists":
            print("RDS already exists. No action taken.")
        else:
            print(f"RDS creation failed: {code}")
            raise

    try:
        rds.get_waiter("db_instance_available").wait(
            DBInstanceIdentifier=inst_name
        )
        print("SUCCESS: RDS instance is available.")
    except WaiterError:
        print("ERROR: RDS creation did not complete successfully.")
        raise

def create_inbound_rule(inst_name, reg_name):
    """Add inbound PostgreSQL rule (TCP 5432) from caller's public IP to the security group attached to the given RDS instance."""

    print(f"Attempting to create an inbound rule for admin/postgres user of {inst_name}")
    rds = boto3.client("rds", region_name=reg_name)
    ec2 = boto3.client("ec2", region_name=reg_name)

    # Resolve RDS security group
    print("Resolving the security group ID")
    resp = rds.describe_db_instances(DBInstanceIdentifier=inst_name)
    sg_id = resp["DBInstances"][0]["VpcSecurityGroups"][0]["VpcSecurityGroupId"]

    # Resolve caller public IP
    print("Resolving public IP of the caller")
    my_ip = requests.get("https://api.ipify.org").text.strip()
    cidr = f"{my_ip}/32"

    print("Trying to create the inbound rule")
    try:
        ec2.authorize_security_group_ingress(
            GroupId=sg_id,
            IpPermissions=[
                {
                    "IpProtocol": "tcp",
                    "FromPort": 5432,
                    "ToPort": 5432,
                    "IpRanges": [
                        {
                            "CidrIp": cidr,
                            "Description": "Local psql access",
                        }
                    ],
                }
            ],
        )
        print(f"Inbound rule added to {sg_id}: TCP 5432 from {cidr}")

    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code == "InvalidPermission.Duplicate":
            print("Inbound rule already exists. No action taken.")
        else:
            raise

def delete_rds(inst_name, reg_name):
    """Delete a database instance and verify its deletion"""

    rds = boto3.client("rds", region_name = reg_name)
    print(f"Initiating deletion of {inst_name}...")

    rds.delete_db_instance(
        DBInstanceIdentifier=inst_name,
        SkipFinalSnapshot=True
    )
    
    try:
        rds.get_waiter("db_instance_deleted").wait(
            DBInstanceIdentifier=inst_name
        )
        print(f"Deletion of {inst_name} complete.")

    except WaiterError:
        print(f"Deletion of {inst_name} failed or timed out.")

def get_rds_conn_info(inst_name, reg_name):
    """Get the connection string for previously created database"""
    rds = boto3.client("rds", region_name=reg_name)

    resp = rds.describe_db_instances(DBInstanceIdentifier=inst_name)["DBInstances"][0]

    return {
        "host": resp["Endpoint"]["Address"],
        "port": resp["Endpoint"]["Port"],
        "dbname": resp["DBName"],
    }
