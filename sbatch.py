

'''For exporting individual python3 functions to a cluster for faster computation.

Assumptions:

1.) your cluster has the same modules as your local computer.

2.) your cluster uses SLURM to manage jobs.

3.) the objects you want passed into your function are picklable.
    - See https://docs.python.org/3/library/pickle.html#pickle-picklable about picklability.

Usage:

1.) Edit the values at the top of this file so that this function can ssh / scp into your cluster of choice.

2.) place this file in the same folder as the main program and import it with "import sbatch".

3.) in whatever file holds your functions, call sbatch.load("function_name", [list, of, arguments]).

4.) when you've finished loading up function calls and you want to send them all out, call sbatch.launch().

5.) drink a smoothie, if you want to!

Limitations:

1.) Commands can only be sent to one cluster at a time. All commands will be sent to run on the cluster specified in
the last call to load(), or to this module's datafields (whichever one has been updated more recently).

2.) If you're using a package supported by Conda / Anaconda, then you'll either need to have internet access on the
cluster (this script will pull all of the packages in the script that sourced it and include them in the conda build if
use_conda == True, use_anaconda == False, and _conda_package has a valid command to load conda; if
use_anaconda == use_conda == True, it will try to open an anaconda environment with the name in _anaconda_env and will
not do anything intelligent with import / package management beyond running your code in an anaconda environment. If you
don't have use_conda set to true, this script won't look for it and will assume that your cluster has all of the
packages necessary to run your scripts.

Dominic Burkart / dominicburkart.com
'''

#all libraries imported to sbatch will also need to be available on the cluster; use only standard libs
import os
import pickle
import types
from time import time, sleep

#for managing imports from the source file
import __main__
from __main__ import * #for imports()
# all libraries imported to sbatch will also need to be available on the cluster; use only standard libs
import os
import pickle
import types
from time import time, sleep

# for managing imports from the source file
import __main__
from __main__ import *  # for imports()

#set to false if all necessary packages are loaded into the standard python3 environment of the cluster.
use_conda = False
use_anaconda = False #only used if use_conda == True

# Note: use the cluster's filepath formatting for locations on the cluster, and the local formatting for local ones.
_sbatch_file_directory = "local/path/on/your/machine/autopy" # directory of things to be sent to the cluster
_archive_directory = "/local/path/autopy_archive" # for things that have been sent to the cluster
_ssh_username = "cluster_username" # cluster username
_ssh_key = "/path/to_shh_key" # cluster ssh key
_host = "della5.princeton.edu" # cluster hostname
_workdir = "/tigress/cluster_username/" # where on the cluster we should make a copy of _sbatch_file_directory
_path_to_python3 = "intel-python/3.5/20150803_185146" # python3 path on the cluster (for "module load _path_to_python3")
_mail = "email@princeton.edu" # email for SLURM messages
_mail_type = "end,fail" # when to email
_conda_package = "module load anaconda3"  # any package loading command that allows you to use conda (only used if conda == True)
_anaconda_env = "autopy_anaconda" #name of the anaconda environment to run the code in if use_anaconda == True
_active_batches = []  # will be populated by load() and emptied by launch()

def imports():
    '''
    returns the names of all non-local imported libraries. May include repeated values.
    '''
    # from https://web.archive.org/web/20180118172759/https://stackoverflow.com/questions/4858100/how-to-list-imported-modules
    for name, val in globals().items():
        if isinstance(val, types.ModuleType):
            yield val.__name__.split(".")[0]


def str_set(inp):
    ''' casts input into a set and then returns a string of all values in the input, split by one space between each value.
    :return: string'''
    s = set(inp)
    o = ""
    for x in s:
        o = o + x + " "
    return o


def launch():
    '''connects to the cluster and starts our sbatch jobs.'''
    import shutil, subprocess, pipes

    def send(to, local_folder=_sbatch_file_directory, k=_ssh_key):
        e = "\nsee http://geoweb.princeton.edu/it/sshkey.html for instructions of SSH keygen."
        if local_folder.endswith("/"):
            local_folder = local_folder[:len(local_folder) - 1]
        cmd = ['scp -i "' + k + '" -r "' + local_folder + '" "' + to + '"', "exit"]
        try:
            open(k).close()
        except FileNotFoundError:
            raise FileNotFoundError("Invalid SSH key filepath passed to sbatch.send(): " + k + e)
        c = subprocess.run(cmd, shell=True)
        if c.returncode != 0:
            print("scp return code of " + str(c.returncode) + ". retrying after 10 seconds.")
            sleep(10)
            c = subprocess.run(cmd, shell=True)
            if c.returncode == 0:
                print("scp successful!")
                return
            raise Exception("SCP Error. Return code: " + str(c.returncode))

    assert len(_active_batches) > 0  # don't contact the cluster if we have no reason to do so.

    if len(_active_batches) > 1:
        print("preparing to launch " + str(len(_active_batches)) + " sbatch jobs.")
    else:
        print("preparing to launch one sbatch job.")

    # copy __main__ into the local folder
    shutil.copy(__main__.__file__, _sbatch_file_directory)
    print("main copied to the local directory.")

    # send necessary files to cluster
    send(_ssh_username + "@" + _host + ":" + _workdir)

    # ssh into the cluster and sbatch the programs
    cmdl = [['cd', _workdir + "/" + _sbatch_file_directory.split("/")[-1]]]
    cmdl.extend([['sbatch', ac] for ac in _active_batches])
    cmdl.append(['exit'])
    cmd = ""
    for l in cmdl:
        cmd = cmd + ' '.join(pipes.quote(com) for com in l) + "\n"
    c = subprocess.run(['ssh', '-i', _ssh_key, _ssh_username + '@' + _host, cmd])
    if c.returncode != 0:
        raise Exception("sbatch unsuccessful. Returncode: " + str(c.returncode))
    else:
        print("sbatch successful.")

    # archive the files we just sent, since they'll delete themselves on the cluster after running
    sub = os.path.join(_archive_directory, "launched" + str(time()).replace(".", "_"))
    os.mkdir(sub)
    for f in os.listdir(_sbatch_file_directory):
        if os.path.isfile(os.path.join(_sbatch_file_directory, f)):
            shutil.move(os.path.join(_sbatch_file_directory, f), sub)

    _active_batches.clear()  # we did it kids! The rest is up to the cluster.


def load(function_name, list_of_params=[], ssh_username=_ssh_username,
         ssh_key=_ssh_key, maxtime="72:00:00",
         nodes=1, cores=20, host=_host, workdir=_workdir,
         name="batch" + str(time()).replace(".", "_"),
         path_to_python3=_path_to_python3,
         mail=_mail, mail_type=_mail_type,
         local=_sbatch_file_directory):
    '''Generates necessary helper scripts to run any individual function available in the program
    that calls this function on a cluster with python3 and SLURM.

    TODO: allow a bool to be passed on whether the whole directory should be sent to the cluster instead of just
    the file that called sbatch.
    '''

    # filenames for everything generated by this function
    tf_names = ["sbatch_tmp" + str(time()).replace(".", "_"), "wrapper" + str(time()).replace(".", "_") + ".py"]
    for i in range(len(list_of_params)):
        tf_names.append(name + '_' + str(time()).replace(".", "_") + '_' + str(
            i) + '.obj')  # pickle object files for each parameter

    # pickle all parameters so we can send them to the cluster
    n_bytes = 2 ** 31
    max_bytes = 2 ** 31 - 1
    for i in range(len(list_of_params)):
        bytes_out = pickle.dumps(list_of_params[i])
        with open(os.path.join(local, tf_names[i + 2]), 'wb') as f_out:
            for idx in range(0, n_bytes, max_bytes):
                f_out.write(bytes_out[idx:idx + max_bytes])

    # generate sbatch file
    sb = "\n#SBATCH "
    pars = ["-N " + str(nodes),
            "-c " + str(cores),
            "-t " + str(maxtime),
            "--workdir=" + os.path.join(workdir, _sbatch_file_directory.split("/")[-1]),
            "--mail-user=" + mail,
            "--mail-type=" + mail_type]
    bash_script = "#!/bin/bash"
    for p in pars:
        bash_script = bash_script + sb + p

    # load python3
    bash_script = bash_script + "\n\nmodule load " + path_to_python3

    # load conda (if relevant)
    if use_conda:
        env_name = "autopy" + str(time()).replace(".", "_") if not use_anaconda else _anaconda_env
        bash_script = bash_script + "\n" + _conda_package
        if use_anaconda:
            print("Assuming that the environment " + _anaconda_env +
                  " has been preconfigured in the working directory of the cluster.")
        else:
            bash_script = bash_script + "\nconda create --name " + env_name + " -y python=3 " + str_set(imports())
        bash_script = bash_script + "\nsource activate " + env_name

    # add content (python)
    bash_script = bash_script + "\npython3 " + tf_names[1]

    for rm in ["\nrm " + n for n in tf_names]:
        bash_script = bash_script + rm
        # ^ will remove unncessary run files on the cluster after the function is called (local files are kept).

    # delete conda environment after use (unless anaconda was used)
    if use_conda and not use_anaconda:
        bash_script = bash_script + "\nconda remove --name " + env_name + " --all"

    f = open(os.path.join(local, tf_names[0]), "w")
    f.write(bash_script)
    f.close()

    # generate python file
    pwrap = "from " + os.path.basename(__main__.__file__).split(".")[0] + " import *\nimport os, pickle"
    # ^ tells the cluster to import all functions from the file that called sbatch()
    pwrap = pwrap + '''
def l(file_path):
    n_bytes = 2**31
    max_bytes = 2**31 - 1
    bytes_in = bytearray(0)
    input_size = os.path.getsize(file_path)
    with open(file_path, 'rb') as f_in:
        for _ in range(0, input_size, max_bytes):
            bytes_in += f_in.read(max_bytes)
    return pickle.loads(bytes_in)

'''  # add unpickling function to each file

    if list_of_params != []:
        pars = "(" + " l('" + str(tf_names[2]) + "')"
        for i in range(3, len(list_of_params) + 2):
            pars = pars + ", l('" + tf_names[i] + "')"
        pars = pars + ")\n"
    else:
        pars = "()\n"

    pwrap = pwrap + function_name + pars

    f = open(os.path.join(local, tf_names[1]), "w")
    f.write(pwrap)
    f.close()

    _active_batches.append(tf_names[0])  # ready to be sent to the cluster + run by launch()!
