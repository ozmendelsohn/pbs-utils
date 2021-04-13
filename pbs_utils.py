import pandas as pd
import numpy as np
import subprocess as sp
import os
import shutil
import time
import petname
import string
import random
import re

pd.options.display.max_colwidth = 250


def unique_name(name_size=3, rand_size=4):
    """
    Return a random easy to read name  in string form
    ----------
        name_size : int
            Number of random words
        rand_size : int
            Size of random string at the end of the name.
    Returns
    -------
        unique_name: string
            the string of the unique name
    """

    return petname.Generate(name_size, '-', 10) + '-' + \
           ''.join([random.choice(string.ascii_letters + string.digits) for i in range(rand_size)])


def nodelist():
    """
    Get a pandas DataFrame object contain the utilization status of the nodes in the ChemFarm cluster.
    _______
    Returns
    -------
        nodelist_DataFrame: :obj:pandas.DataFrame
            DataFrame with current utilization status of the nodes.
            column type:
                Type of the node.
            column: cores:

            column: status:
                The PBS status of the job: R - running, Q - waiting in the queue, S - suspended
    """
    process = sp.Popen(['nodelist -g'], stdout=sp.PIPE, stderr=sp.PIPE, shell=True)
    out, err = process.communicate()
    lines, types, status, use_cores, cores, use_memory, mem = [], [], [], [], [], [], []
    for l in out.splitlines():
        l = l.decode('utf-8')
        type_match = re.search(r'(\w\w\w)\d\d\d', l)

        cpu_match = re.search(r'(\d{1,2}): (\d\d)', l)

        mem_match = re.search(r'(\d{1,3}):\s+(\d{1,3}) gb', l)

        status_match = re.search(r'[|] ([a-z\-]+ [a-z]*)', l)
        if type_match and cpu_match and mem_match and status_match:
            type = type_match.group(1)
            use_cpu = cpu_match.group(1)
            total_cpu = cpu_match.group(2)
            use_mem = mem_match.group(1)
            total_mem = mem_match.group(2)
            node_status = status_match.group(1).strip()

            types.append(type)
            status.append(node_status)
            cores.append(total_cpu)
            mem.append(total_mem)
            use_cores.append(use_cpu)
            use_memory.append(use_mem)
    df = pd.DataFrame(dict(
        type=types,
        cores=cores,
        use_cores=use_cores,
        memory=mem,
        use_memory=use_memory,
        status=status
    ))
    return df


def jobs_status():
    """
    Get a pandas DataFrame object all of the current jobs on ChemFarm scheduler.
    _______
    Returns
    -------
        jobs_DataFrame: :obj:pandas.DataFrame
            DataFram with all the jobs and they status.
            column: job_id
                PBS ID number of the job
            column: user:
                User name of the user how submit the job
            column: status:
                The PBS status of the job: R - running, Q - waiting in the queue, S - suspended
    """
    process = sp.Popen(['qstat'], stdout=sp.PIPE, stderr=sp.PIPE, shell=True)
    out, err = process.communicate()
    job_number, status, user = [], [], []
    start = False
    for l in out.splitlines():
        l = l.decode('utf-8')
        if '--' in l:
            start = True
            continue
        if start:
            part = l.split()
            job_number.append(part[0])
            user.append(part[2])
            status.append(part[4])

    df = pd.DataFrame(dict(
        job_id=job_number,
        user=user,
        status=status,
    ))

    return df


def pbs_file(execute_line, **pbs_kwargs):
    """
    Create job submission file and return the name of the file.
    ----------
        execute_line : str
            The line to run inside the jobs
        pbs_kwargs : dict
            Keyword for the configuration of the PBS job
            Keyword:
                path: str
                    the path which the file is going to be made.
                name: str
                    name of the job
                resources: dict
                    dictionary with of the information of the requested resources for the job.
                    resources keys:
                        select: int:
                            number of node for the jobs
                        cpu: int:
                            number of cpu for a given node.
                        mem: str:
                            size of the RAM for each node.
                        node_type: str:
                            the type/name of the node.
                        ngpus: str:
                            number of GPUs for the job
                walltime: str:
                    the maximum running time of the job
                queue: str:
                    the name of the job queue
                mail: str:
                    mail address to send the PBS report
                log: str:
                    name of the log file to save the output of the script.
    Returns
    -------
        script path: string
            Path of the submission file.
    """
    path = pbs_kwargs['path'] if 'path' in pbs_kwargs.keys() else ''
    name = pbs_kwargs['name'] if 'name' in pbs_kwargs.keys() else unique_name(2, 4)
    resources = pbs_kwargs['resources'] if 'resources' in pbs_kwargs.keys() else dict(select=1, ncpus=1, mem='10gb')
    resources = ':'.join([f'{k}={v}' for k, v in resources.items()])
    queue = pbs_kwargs['queue'] if 'queue' in pbs_kwargs.keys() else 'sleep'
    mail = pbs_kwargs['mail'] if 'mail' in pbs_kwargs.keys() else None
    log_file = pbs_kwargs['log'] if 'log' in pbs_kwargs.keys() else name + '.log'
    file_text = '#!/bin/bash\n' \
                f'#PBS -N "{name}"\n' \
                f'#PBS -l {resources}\n' \
                f'#PBS -q {queue}\n'
    if mail is not None:
        file_text += f'#PBS -M {mail}\n' \
                     f'#PBS -m ae\n'
    if 'walltime' in pbs_kwargs.keys():
        walltime = pbs_kwargs['walltime']
        file_text += f'PBS -l walltime={walltime}\n'

    file_text += 'ulimit -s unlimited\n' \
                 'cd $PBS_O_WORKDIR\n' \
                 'set -e\n' \
                 'JOBID=$( echo $PBS_JOBID | sed \'s/\.pbs01//\' )\n' \
                 'JOBID=${JOBID%?};\n' \
                 'JOBID=${JOBID%?};\n' \
                 'JOBID=${JOBID%?};\n' \
                 'JOBID=${JOBID%?};\n' \
                 'ulimit -s\n' \
                 'source ~/.bash_profile\n'
    file_text += execute_line + '>>' + log_file + '\n'
    file_text += f'echo 1 > {name}.fin\n'
    full_name = path + name + '.pbs'
    with open(full_name, 'w') as f:
        f.write(file_text)
    return full_name


def submit_static_job(execute_line, **pbs_kwargs):
    """
    Get execute_line and pbs_kwargs and submit the jobs.
    ----------
    execute_line : str
        The line to run inside the jobs
    pbs_kwargs : dict
        Keyword for the configuration of the PBS job
        Keyword:
            path: str
                the path which the file is going to be made.
            name: str
                name of the job
            resources: dict
                dictionary with of the information of the requested resources for the job.
                resources keys:
                    select: int:
                        number of node for the jobs
                    cpu: int:
                        number of cpu for a given node.
                    mem: str:
                        size of the RAM for each node.
                    node_type: str:
                        the type/name of the node.
                    ngpus: str:
                        number of GPUs for the job
            walltime: str:
                the maximum running time of the job
            queue: str:
                the name of the job queue
            mail: str:
                mail address to send the PBS report
            log: str:
                name of the log file to save the output of the script.
    Returns
    -------
        job ID: string
            The job submission ID
    """

    run_me = pbs_file(execute_line, **pbs_kwargs)
    print('running: {}'.format(run_me))
    process = sp.Popen([f'qsub {run_me}'], stdout=sp.PIPE, stderr=sp.PIPE, shell=True)
    out, err = process.communicate()
    return out.decode('utf-8').strip()


def submit_static_job_df(df: pd.Series):
    """
    Get pandas.Series with execute_line and pbs_kwargs and submit a job.
    ----------
    df: :obj:pandas.Series
        A line contain all the information for a submission of a single jobs submission
        columns:
            execute_line : str
                The line to run inside the jobs
            pbs_kwargs : dict
                Keyword for the configuration of the PBS job
                Keyword:
                    path: str
                        the path which the file is going to be made.
                    name: str
                        name of the job
                    resources: dict
                        dictionary with of the information of the requested resources for the job.
                        resources keys:
                            select: int:
                                number of node for the jobs
                            cpu: int:
                                number of cpu for a given node.
                            mem: str:
                                size of the RAM for each node.
                            node_type: str:
                                the type/name of the node.
                            ngpus: str:
                                number of GPUs for the job
                    walltime: str:
                        the maximum running time of the job
                    queue: str:
                        the name of the job queue
                    mail: str:
                        mail address to send the PBS report
                    log: str:
                        name of the log file to save the output of the script.
    Returns
    -------
        job ID: string
            The job submission ID
    """

    execute_line = df['execute_lines']
    parser_kwargs = df['parser_kwargs'] if type(df['parser_kwargs']) == dict else {}
    for k, v in parser_kwargs.items():
        execute_line += f' --{k}={v}'
    pbs_kwargs = df['pbs_kwargs'] if type(df['pbs_kwargs']) == dict else {}
    run_me = pbs_file(execute_line, **pbs_kwargs)
    print('running: {}'.format(run_me))
    process = sp.Popen([f'qsub {run_me}'], stdout=sp.PIPE, stderr=sp.PIPE, shell=True)
    out, err = process.communicate()
    return out.decode('utf-8').strip()


def jobs_dataframe(execute_lines, pbs_kwargs, parser_kwargs=None):
    """
    Create a pandas.DataFrame from a list of execute_lines, pbs_kwargs and parser_kwargs (optional)
    ----------
    execute_lines: list of str or str
        execution lines for all the jobs to be submitted.
    pbs_kwargs: list of dict or dict
        key words for the submission file of the jobs.
        for more details look at submit_static_job().
    parser_kwargs: list of dict or dict (optional)
        A list of dictionaries for a python parser argument.
        Each attribute in the dictionary will bet added to the execution line in the following format:
            --key=parser_kwargs[key]
    Returns
    -------
        jobs DataFrame: :obj: pandas.DataFrame
            jobs DataFrame ready for submission.
    """
    if type(execute_lines) == str:
        if 'name' not in pbs_kwargs.keys():
            names = [unique_name()]
            pbs_kwargs['name'] = names
            job_id, status = [''], ['']
            execute_lines = [execute_lines]
            pbs_kwargs = [pbs_kwargs]
            parser_kwargs = [parser_kwargs]
            if parser_kwargs is None:
                parser_kwargs = [dict()]
        else:
            names = [pbs_kwargs['name']]
    if type(execute_lines) == list:
        names = []
        for i in range(len(execute_lines)):
            if 'name' not in pbs_kwargs[i].keys():
                names.append(unique_name())
                pbs_kwargs[i]['name'] = names[-1]
            else:
                names.append(pbs_kwargs[i]['name'])
        if parser_kwargs is None:
            parser_kwargs = [dict() for _ in range(len(execute_lines))]
    job_id = [''] * len(execute_lines)
    status = [''] * len(execute_lines)
    reset = [0] * len(execute_lines)

    return pd.DataFrame(dict(
        name=names,
        execute_lines=execute_lines,
        pbs_kwargs=pbs_kwargs,
        parser_kwargs=parser_kwargs,
        job_id=job_id,
        status=status))


def update_jobs_dataframe(job_df):
    """
    Update the jobs status of each entry in the pandas.DataFrame.
    Each line is update according to it PBS job ID.
    ----------
    jobs DataFrame: :obj: pandas.DataFrame
            jobs DataFrame with some of the line with job ID
    Returns
    -------
        jobs DataFrame: :obj: pandas.DataFrame
            The update pandas.DataFrame with updated status.
    """

    status_df = jobs_status()
    for i in range(len(job_df)):
        if len(status_df.loc[status_df['job_id'] == job_df.iloc[i]['job_id'], 'status'].values) > 0:
            job_df.loc[i, 'status'] = status_df.loc[status_df['job_id'] == job_df.iloc[i]['job_id'], 'status'].values[0]
        elif len(status_df.loc[status_df['job_id'] == job_df.iloc[i]['job_id'], 'status'].values) == 0:
            if job_df.loc[i, 'status'] == 'P':
                continue
            if job_df.loc[i, 'name'] + '.fin' in os.listdir():
                job_df.loc[i, 'status'] = 'E'
            if job_df.loc[i, 'name'] + '.fin' not in os.listdir():
                if job_df.loc[i, 'status'] == 'E':
                    pass
                else:
                    job_df.loc[i, 'status'] = 'F'
    return job_df


def submit_over_df(df, max_queue=3, max_time=1000, wait_time=5, restart=0, dump=False, reset_callback=None):
    """
    Submit jobs from a pandas.DataFrame while keeping number of jobs waiting in the queue smaller then max_queue*@
    ----------
    jobs DataFrame: :obj: pandas.DataFrame
        jobs subbmishing  DataFrame
    max_queue: int (optional)
        max number of jobs to submitted at the same time.
    max_time: int (optional)
        A global time limiter for the submisstion loop.
        Time in hours.
    wait_time: int (optional)
        minimum time to wait between two sequential submission loop.
        Time in seconds.
    wait_time: int (optional)
        number of time to restart a failed job.
        Need the ".fin" flag for monitoring.
    dump: str (optional)
        If a string is provided a .json and .csv files are created which update for the jobs DataFrame.
    reset_callback: function (optional)
        a callback function that take the jobs Dataframe and job row index from update/print
        information after a the job is reset.
    """

    t0 = time.time()
    max_time *= 60 * 60
    for i in range(len(df)):
        df.loc[i, 'status'] = 'P'
        df.loc[i, 'pbs_kwargs']['path'] += df.loc[i, 'pbs_kwargs']['name'] + '/'
    if restart > 0:
        df.insert(len(df.columns) - 1, 'reset', [int(0)] * len(df))
    while time.time() - t0 < max_time:
        # %%
        # Get current inforamtion about the jobs
        df = update_jobs_dataframe(df)
        n_Q = np.sum(df['status'].values == 'Q')
        n_P = np.sum(df['status'].values == 'P')
        n_R = np.sum(df['status'].values == 'R')
        n_E = np.sum(df['status'].values == 'E')
        n_F = np.sum(df['status'].values == 'F')
        print(f'jobs status: P:{n_P} Q:{n_Q} R:{n_R} E:{n_E} F:{n_F}')
        # %%
        # loop while more jobs in the queue then allowed
        while len(df.loc[df['status'] == 'Q']) >= max_queue:
            time.sleep(wait_time)
            df = update_jobs_dataframe(df)
        # dump the jobs datafram to readable file
        df = update_jobs_dataframe(df)
        if isinstance(dump, str):
            df.to_csv(f'{dump}.csv')
            df.to_json(f'{dump}.json')

        # clean file of finished / failed jobs
        time.sleep(wait_time)
        for i, row in df.loc[df['status'] == 'E', :].iterrows():
            try:
                clean_up(row)
            except Exception as e:
                print(e)
        for i, row in df.loc[df['status'] == 'F', :].iterrows():
            try:
                clean_up(row)
            except Exception as e:
                print(e)
        # move fails jobs to pending state if needed
        if restart > 0:
            reset_fails(df, n=restart, reset_callback=reset_callback)
        # %%
        # check if submission ended
        n_Q = np.sum(df['status'].values == 'Q')
        n_P = np.sum(df['status'].values == 'P')
        n_R = np.sum(df['status'].values == 'R')
        n_S = np.sum(df['status'].values == 'S')
        if n_Q + n_P + n_R + n_S == 0:
            break

        # %%
        # submit new pending jobs
        df_submission = df.loc[df['status'] == 'P', :][:max_queue]
        for i, row in df_submission.iterrows():
            try:
                os.makedirs(row['pbs_kwargs']['path'])
            except FileExistsError:
                pass

            id = submit_static_job_df(row)
            print(f'job ID:{id}')
            df_submission.loc[df_submission['name'] == row['name'], 'job_id'] = id
        # add job id to the jobs dataframe
        df.update(df_submission)
        time.sleep(wait_time)
        df = update_jobs_dataframe(df)


def clean_up(df: pd.Series):
    path = df['pbs_kwargs']['path']
    name = df['pbs_kwargs']['name']
    for file in os.listdir():
        if name in file:
            if '.e' in file or '.o' in file or '.fin' in file or '.log' in file:
                shutil.move(file, path + file)


def reset_fails(df, n=3, reset_callback=None):
    for i in range(len(df)):
        if df.loc[i, 'status'] == 'F' and df.iloc[i]['reset'] < n:
            df.loc[i, 'reset'] += 1
            df.loc[i, 'status'] = 'P'
            if reset_callback is not None:
                df = reset_callback(df, i)
            name = df.loc[i, 'name']
            print(f'restring: {name}')

