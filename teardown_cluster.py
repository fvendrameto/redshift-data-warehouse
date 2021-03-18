from config import CONFIG
import boto3

KEY = CONFIG.get('AWS', 'KEY')
SECRET = CONFIG.get('AWS', 'SECRET')

CLUSTER_IDENTIFIER = CONFIG.get('CLUSTER', 'CLUSTER_IDENTIFIER')

def teardown_cluster():
    redshift = boto3.client('redshift',
                            region_name='us-west-2',
                            aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET)

    redshift.delete_cluster(ClusterIdentifier=CLUSTER_IDENTIFIER,
                            SkipFinalClusterSnapshot=True)

    waiter = redshift.get_waiter('cluster_deleted')
    print('Waiting for cluster status "Deleted"')
    waiter.wait(ClusterIdentifier=CLUSTER_IDENTIFIER)
    print('Cluster successfuly deleted')

    CONFIG['DB']['HOST'] = ''
    with open('dwh.cfg', 'w') as config_file:
        CONFIG.write(config_file)

if __name__ == '__main__':
    teardown_cluster()
