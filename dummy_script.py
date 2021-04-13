import time
import argparse

def unique_name():
    import petname
    import string
    import random
    return petname.Generate(3, '-', 10) + '-' + \
           ''.join([random.choice(string.ascii_letters + string.digits) for _ in range(6)])


parser = argparse.ArgumentParser()
parser.add_argument('--arg1', type=str, default='',
                    help='')
parser.add_argument('--arg2', type=str, default='',
                    help='')
parser.add_argument('--arg3', type=str, default='',
                    help='')
args = parser.parse_args()
print(f'dummy script: {unique_name()}, arg1: {args.arg1} arg2: {args.arg2} arg3: {args.arg3}')
time.sleep(1)
print('finish dummy')

