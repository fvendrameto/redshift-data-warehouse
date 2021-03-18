import boto3

from config import CONFIG

KEY = CONFIG.get('AWS', 'KEY')
SECRET = CONFIG.get('AWS', 'SECRET')

CLUSTER_IDENTIFIER = CONFIG.get('CLUSTER', 'CLUSTER_IDENTIFIER')
CLUSTER_TYPE = CONFIG.get('CLUSTER', 'CLUSTER_TYPE')
NUM_NODES = CONFIG.get('CLUSTER', 'NUM_NODES')
NODE_TYPE = CONFIG.get('CLUSTER', 'NODE_TYPE')
DB_NAME = CONFIG.get('DB', 'DB_NAME')
DB_USER = CONFIG.get('DB', 'DB_USER')
DB_PASSWORD = CONFIG.get('DB', 'DB_PASSWORD')
DB_PORT = CONFIG.get('DB', 'DB_PORT')

DB_ROLE_ARN = CONFIG.get('IAM_ROLE', 'ARN')


def create_cluster():
    ec2 = boto3.resource('ec2',
                         region_name='us-west-2',
                         aws_access_key_id=KEY,
                         aws_secret_access_key=SECRET)

    redshift = boto3.client('redshift',
                            region_name='us-west-2',
                            aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET)

    _ = redshift.create_cluster(ClusterType=CLUSTER_TYPE,
                                NodeType=NODE_TYPE,
                                NumberOfNodes=int(NUM_NODES),
                                DBName=DB_NAME,
                                ClusterIdentifier=CLUSTER_IDENTIFIER,
                                MasterUsername=DB_USER,
                                MasterUserPassword=DB_PASSWORD,
                                IamRoles=[DB_ROLE_ARN])

    waiter = redshift.get_waiter('cluster_available')
    print('Waiting for cluster status "Available"')
    waiter.wait(ClusterIdentifier=CLUSTER_IDENTIFIER)

    cluster_propeties = redshift.describe_clusters(
        ClusterIdentifier=CLUSTER_IDENTIFIER)['Clusters'][0]

    host = cluster_propeties['Endpoint']['Address']
    vpc_id = cluster_propeties['VpcId']

    try:
        vpc = ec2.Vpc(id=vpc_id)
        defaultSg = list(vpc.security_groups.all())[0]

        defaultSg.authorize_ingress(GroupName=defaultSg.group_name,
                                    CidrIp='0.0.0.0/0',
                                    IpProtocol='TCP',
                                    FromPort=int(DB_PORT),
                                    ToPort=int(DB_PORT))
    except Exception as e:
        print(f'Error creating inbound VPC rule {e}')

    CONFIG['DB']['HOST'] = host
    with open('dwh.cfg', 'w') as config_file:
        CONFIG.write(config_file)


if __name__ == '__main__':
    create_cluster()
