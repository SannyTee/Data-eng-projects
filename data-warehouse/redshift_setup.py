import boto3
import json
import configparser
import logging
import argparse

from botocore.exceptions import ClientError

logging.basicConfig(format='%(asctime)s :%(levelname)s - %(message)s', level=logging.INFO)

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))


def create_iam_role(iam):
    """
    create an IAM role
    :param iam: instance of an IAM client
    :return: Arn of the created role.
    """
    role_name = config.get('IAM_ROLE', 'ROLE_NAME')
    role_description = 'allows redshift to access other aws services'
    try:
        logging.info(f'creating {role_name}')
        iam.create_role(
            Path='/',
            RoleName=role_name,
            Description=role_description,
            AssumeRolePolicyDocument=json.dumps({
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"Service": ["redshift.amazonaws.com"]},
                        "Action": ["sts:AssumeRole"]
                    }
                ]
            })
        )
        # attach policy created
        iam.attach_role_policy(RoleName=role_name, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
        logging.info(f'{role_name} created successfully')
        return iam.get_role(RoleName=role_name)['Role']['Arn']
    except iam.exceptions.EntityAlreadyExistsException:
        logging.warning(f'Role {role_name} already exist')
    except Exception as e:
        logging.error(f'Error occurred creating Role {role_name}\n {e}')


def delete_iam_role(iam):
    """
    Delete an IAM role
    :param iam: instance of an IAM client
    :return:  None
    """
    role_name = config.get('IAM_ROLE', 'ROLE_NAME')
    try:
        logging.info(f'Deleting role {role_name}')
        iam.delete_role(RoleName=role_name)
        logging.info(f'{role_name} role successfully deleted')
    except iam.exceptions.NoSuchEntityException:
        logging.warning(f'{role_name} role does not exist')
    except iam.exceptions.DeleteConflictException:
        logging.error(f'Ensure policies attached to {role_name} role are already detached')
    except Exception as e:
        logging.error(f'Error occurred deleting Role {role_name}\n {e}')


def detach_iam_role_policy(iam, role_name, policy_arn):
    """
    Detach policy from IAM role
    :param iam: instance of an IAM client
    :param role_name: Role to detach policy from
    :param policy_arn:  Arn of policy to be detached
    :return: None
    """
    try:
        iam.detach_role_policy(RoleName=role_name, PolicyArn=policy_arn)
    except iam.exceptions.NoSuchEntityException:
        logging.warning('Either role or/and policy does not exist')
    except Exception as e:
        logging.error(f'Error occurred detaching policy \n {e}')


def create_cluster(client, iam_role_arn):
    """
    Creates a redshift cluster
    :param client: An instance of redshift client
    :param iam_role_arn: Arn of role to be attached to redshift cluster
    :return: None
    """
    cluster_type = config.get('REDSHIFT', 'CLUSTER_TYPE')
    node_type = config.get('REDSHIFT', 'NODE_TYPE')
    number_of_nodes = int(config.get('REDSHIFT', 'NUMBER_OF_NODES'))
    db_name = config.get('REDSHIFT', 'DB_NAME')
    cluster_identifier = config.get('REDSHIFT', 'CLUSTER_IDENTIFIER')
    master_username = config.get('REDSHIFT', 'MASTER_USERNAME')
    master_password = config.get('REDSHIFT', 'MASTER_PASSWORD')

    try:
        logging.info(f'creating cluster {cluster_identifier}')
        client.create_cluster(
            DBName=db_name,
            ClusterIdentifier=cluster_identifier,
            ClusterType=cluster_type,
            NodeType=node_type,
            NumberOfNodes=number_of_nodes,
            MasterUsername=master_username,
            MasterUserPassword=master_password,
            IamRoles=[iam_role_arn]
        )
        logging.info(f'{cluster_identifier} cluster created successfully')
    except client.exceptions.ClusterAlreadyExistsFault:
        logging.warning(f'Cluster {cluster_identifier} already exist')
    except Exception as e:
        logging.error(f"An error occurred creating cluster:\n {e}")


def delete_cluster(client, cluster_id):
    """
    Deletes a Redshift cluster
    :param client: An instance of Redshift client
    :param cluster_id: identifier of cluster to be deleted
    :return: None
    """
    try:
        logging.info(f'deleting {cluster_id} cluster')
        client.delete_cluster(
            ClusterIdentifier=cluster_id,
            SkipFinalClusterSnapshot=True,
        )
        logging.info(f'{cluster_id} cluster deleted successfully')
    except client.exceptions.ClusterNotFoundFault:
        logging.warning(f'{cluster_id} Cluster does not exist')
    except Exception as e:
        logging.error(f'Error occurred deleting {cluster_id} cluster.\n {e}')


def allow_conn(resource, vpc_id, port):
    try:
        vpc = resource.Vpc(id=vpc_id)
        default_sg = list(vpc.security_groups.all())[0]
        default_sg.authorize_ingress(
            GroupName=default_sg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(port),
            ToPort=int(port)
        )
    except ClientError as e:
        error = e.response['Error']['Code']
        if error == 'InvalidPermission.Duplicate':
            logging.warning('Permission previously granted')
        else:
            logging.error('An error occurred with granting permission')


def setup():
    """
    set up the redshift cluster
    :return: None
    """
    key_id = config.get('AWS', 'ACCESS_KEY_ID')
    key = config.get('AWS', 'SECRET_ACCESS_KEY')

    ec2 = boto3.resource('ec2', region_name='us-east-2', aws_access_key_id=key_id, aws_secret_access_key=key)
    iam = boto3.client('iam', region_name='us-east-2', aws_access_key_id=key_id, aws_secret_access_key=key)
    redshift = boto3.client('redshift', region_name='us-east-2', aws_access_key_id=key_id, aws_secret_access_key=key)

    # create role
    iam_role_arn = create_iam_role(iam)

    # create cluster
    create_cluster(redshift, iam_role_arn)

    # allow connection
    port = config.get('REDSHIFT', 'PORT')
    cluster_identifier = config.get('REDSHIFT', 'CLUSTER_IDENTIFIER')
    vpc_id = redshift.describe_clusters(ClusterIdentifier=cluster_identifier)['Clusters'][0]['VpcId']
    allow_conn(ec2, vpc_id, port)


def teardown():
    """
    Deletes redshift cluster and Iam role.
    :return: None
    """
    key_id = config.get('AWS', 'ACCESS_KEY_ID')
    key = config.get('AWS', 'SECRET_ACCESS_KEY')
    cluster_identifier = config.get('REDSHIFT', 'CLUSTER_IDENTIFIER')
    role_name = config.get('IAM_ROLE', 'ROLE_NAME')

    # delete cluster
    redshift = boto3.client('redshift', region_name='us-east-2', aws_access_key_id=key_id, aws_secret_access_key=key)
    iam = boto3.client('iam', region_name='us-east-2', aws_access_key_id=key_id, aws_secret_access_key=key)
    delete_cluster(redshift, cluster_identifier)

    # delete role
    policy_arn = 'arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess'
    detach_iam_role_policy(iam, role_name, policy_arn)
    delete_iam_role(iam)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument('--create', type=str, help='set up the database')
    parser.add_argument('--destroy', type=str, help='tear down the database')

    args = parser.parse_args()

    if not args.create and not args.destroy:
        logging.warning('create or destroy flag is required')

    if args.create:
        setup()

    if args.destroy:
        teardown()
