import argparse
from dotenv import dotenv_values
import os
import boto3

config = {
    **os.environ
}


if __name__ == '__main__':
    parser = argparse.ArgumentParser('Amazon MTurk HIT client')
    parser.add_argument('--env', '-e', nargs=1, default='.env', help='Environment file', metavar='file')
    args = parser.parse_args()

    print(f'env: {args.env}')
    
    config = {
        **dotenv_values(args.env[0]),
        **os.environ
    }

    client = boto3.client(
        'mturk',
        endpoint_url=config['endpoint_url'],
        region_name=config['region_name'],
        aws_access_key_id=config['aws_access_key_id'],
        aws_secret_access_key=config['aws_secret_access_key']
    )

    print(client.get_account_balance()['AvailableBalance'])
