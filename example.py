from pbs_utils import *

"""
Example of using a script to run jobs in free nodes
"""
# setup
usable_nodes = ['cfa', 'cfi', 'cfl', 'cfh', 'cfe', 'cff']
path = 'temp/'
execute_lines, pbs_kwargs, parser_kwargs = [], [], []
# look for free nodes
df_node = nodelist()
# loop over free nodes:
for _, free_row in df_node.loc[(df_node['status'] == 'free') |
                               (df_node['status'] == 'partially free')].iterrows():
    # skip non relevant nodes:
    if free_row['type'] not in usable_nodes:
        continue

    # define resources for job

    # request max ~90% of the node RAM
    max_mem = int(float(free_row['memory']) * 0.9)
    mem = max_mem - int(free_row['use_memory'])  # request free memory
    if mem <= 0:
        continue  # skip crowded node

    # request free cpus of the node
    ncpus = int(free_row['cores']) - int(free_row['use_cores'])

    # reformat the resources
    select, mem, node_type = 1, str(mem) + 'gb', free_row['type']
    queue = 'idle'
    # select, ncpus, mem, node_type = 1, 1, '1gb', 'cfi'  # debug
    # queue = 'medium'  # debug

    # create jobs requirements:
    name = unique_name(3, 4)
    execute_lines.append('conda activate //home/oz/anaconda3/envs/sgdml/ \n' +
                         'python dummy_script.py')  # run the dummy script (time.sleep(1))

    resources = dict(select=select, ncpus=ncpus, mem=mem, node_type=node_type)
    pbs_kwargs.append(dict(mail='ozyosef.mendelsohn@weizmann.ac.il',
                           name=name,
                           resources=resources,
                           walltime='72:00:00',
                           queue=queue,
                           path=path))
    # give the dummy script arguments:
    parser_kwargs.append(dict(arg1=petname.name(),
                              arg2=petname.name(),
                              arg3=petname.name()))

    # if len(execute_lines) >= 10: # debug
    #     break
# create the jobs DataFrame
job_df = jobs_dataframe(execute_lines, pbs_kwargs, parser_kwargs)

# submit the jobs!
submit_over_df(job_df, max_time=1000, wait_time=1, max_queue=1, restart=10)

print('--------------------job done!--------------------------')