'''For exporting individual python3 functions to a cluster for faster computation.

Assumptions:

1.) your cluster has the same modules as your local computer, and both are running the same version of python.

2.) your cluster uses SLURM to manage jobs.

3.) the objects you want passed into your function are picklable.
    - See https://docs.python.org/3/library/pickle.html#pickle-picklable about picklability.

Usage:

1.) Edit the values at the top of this file so that this function can ssh / scp into your cluster of choice.

2.) place this file in the same folder as the main program and import it with "import sbatch".

3.) in whatever file holds your functions, call sbatch.load("function_name", [list, of, arguments]).

4.) when you've finished loading up function calls and you want to send them all out, call sbatch.launch().

5.) drink a smoothie, if you want to!


FYI:
You'll need to set up your cluster conda environment (or find an alternative) with any relevant packages if you
aren't able to use the packages built into anaconda. Syntactic differences between python versions aren't accounted for.

Dominic Burkart / dominicburkart.com
'''

#all libraries imported to sbatch will also need to be available on the cluster; use only standard libs
import os
import pickle
import types
from time import time, sleep
from functools import lru_cache

#for managing imports from the source file
import __main__

# ~~~~~~~~~~~~~~~~~~ beginning of the code that a user is expected to edit

#set to false if all necessary packages are loaded into the standard python3 environment of the cluster.
use_anaconda = False

default_cluster = "cluster"

# Note: use the cluster's filepath formatting for locations on the cluster, and the local formatting for local ones.

@lru_cache(maxsize=4)
def cluster_info(c = default_cluster):
    if c == "cluster1":
        _sbatch_file_directory = "local path – unique to cluster"  # directory of things to be sent to the cluster
        _archive_directory = "local path"  # for things that have been sent to the cluster
        _ssh_username = "cluster"  # cluster username
        _ssh_key = "key on local machine"  # cluster ssh key
        _host = "della5.princeton.edu"  # cluster hostname
        _workdir = "/tigress/username/"  # where on the cluster we should make a copy of _sbatch_file_directory.
        _path_to_python3 = "intel-python/3.5/20150803_185146"  # python3 path on the cluster (for "module load _path_to...")
        # leave _path_to_python3 blank if the version of python3 you want is loaded by default on the cluster.
        _mail = "email@website.edu"  # email for SLURM messages
        _mail_type = "end,fail"  # when to email
        _conda_package = "module load anaconda3"  # any package loading command that allows you to use conda (only used if use_anaconda == True)
        _anaconda_env = "autopy_anaconda"  # name of the anaconda environment to run the code in if use_anaconda == True
    elif c == "cluster2":
        _sbatch_file_directory = "local path – unique to cluster"
        _archive_directory = "local path"
        _ssh_username = "cluster"
        _ssh_key = "key on local machine"
        _host = "della5.princeton.edu"
        _workdir = "/tigress/username/"
        _path_to_python3 = "intel-python/3.5/20150803_185146"
        _mail = "email@website.edu"
        _mail_type = "end,fail"
        _conda_package = "module load anaconda3"
        _anaconda_env = "autopy_anaconda"
    #define your own cluster info here using the same format as above!
# ~~~~~~~~~~~~~~~~~~ end of the code that a user is expected to edit.
    else:
        raise UnknownClusterException("The following unrecognized value was passed as the cluster name: " +
                                      str(c)+"\ntype: "+str(type(c)))
    return locals()

_active_batches = []  # will be populated by load() and emptied by launch()
_active_clusters = [] # will also be populated by load() and emptied by launch()

class UnknownClusterException(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)


def launch():
    '''connects to the cluster and starts our sbatch jobs.'''
    import shutil, subprocess, pipes

    def send(to, local_folder, k):
        '''
        Uploads a folder to a given non-local address.

        :param to: username@hostname:/path/on/remote/system
        :param local_folder: local folder to send
        :param k: SSH key file
        :return: nothing
        '''
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

    for c in _active_clusters:
        clud = cluster_info(c=c)

        rel_jobs = [j[0] for j in _active_batches if j[1] == c]

        assert len(rel_jobs) > 0  # don't contact the cluster if we have no reason to do so.

        if len(rel_jobs) > 1:
            print("preparing to launch " + str(len(rel_jobs)) + " sbatch jobs " + "to "+c)
        else:
            print("preparing to launch one sbatch job.")

        # copy __main__ into the local folder
        shutil.copy(__main__.__file__, clud["_sbatch_file_directory"])
        print("main copied to the local directory.")

        # send necessary files to cluster
        send(clud["_ssh_username"] + "@" + clud["_host"] + ":" + clud["_workdir"], clud["_sbatch_file_directory"],
             clud["_ssh_key"])

        # ssh into the cluster and sbatch the programs
        cmdl = [['cd', clud["_workdir"] + "/" + clud["_sbatch_file_directory"].split("/")[-1]]]
        cmdl.extend([['sbatch', ac] for ac in rel_jobs])
        cmdl.append(['exit'])
        cmd = ""
        for l in cmdl:
            cmd = cmd + ' '.join(pipes.quote(com) for com in l) + "\n"
        c = subprocess.run(['ssh', '-i', clud["_ssh_key"], clud["_ssh_username"] + '@' + clud["_host"], cmd])
        if c.returncode != 0:
            raise Exception("sbatch unsuccessful. Return code: " + str(c.returncode))
        else:
            print("sbatch successful.")

        # archive the files we just sent, since they'll delete themselves on the cluster after running
        sub = os.path.join(clud["_archive_directory"], "launched" + str(time()).replace(".", "_"))
        os.mkdir(sub)
        for f in os.listdir(clud["_sbatch_file_directory"]):
            if os.path.isfile(os.path.join(clud["_sbatch_file_directory"], f)):
                shutil.move(os.path.join(clud["_sbatch_file_directory"], f), sub)

    _active_batches.clear()  # we did it kids! The rest is up to the cluster.
    _active_clusters.clear()


def load(function_name, list_of_params=[], cluster = default_cluster, maxtime="72:00:00",
         nodes=1, cores=20, workdir=None,
         name="batch" + str(time()).replace(".", "_"),
         mail=None, mail_type=None,
         local=None):
    '''Generates necessary helper scripts to run any individual function available in the program
    that calls this function on a cluster with python3 and SLURM.

    TODO: allow a bool to be passed on whether the whole directory should be sent to the cluster instead of just
    the file that called sbatch.
    '''

    workdir = cluster_info(cluster)["_workdir"] if workdir is None else workdir
    local = cluster_info(cluster)["_sbatch_file_directory"] if local is None else local
    mail = cluster_info(cluster)["_mail"] if mail is None else mail
    mail_type = cluster_info(cluster)["_mail_type"] if mail_type is None else mail_type

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
            "--workdir=" + os.path.join(workdir, cluster_info(cluster)["_sbatch_file_directory"].split("/")[-1]),
            "--mail-user=" + mail,
            "--mail-type=" + mail_type]
    bash_script = "#!/bin/bash"
    for p in pars:
        bash_script = bash_script + sb + p

    # load python3
    if cluster_info(cluster)["_path_to_python3"] != "" and cluster_info(cluster)["_path_to_python3"] != None:
        bash_script = bash_script + "\n\nmodule load " + cluster_info(cluster)["_path_to_python3"]

    if use_anaconda:
        bash_script = bash_script + "\n" + cluster_info(cluster)["_conda_package"] + "\nsource activate " + cluster_info(cluster)["_anaconda_env"]

    # add content (python)
    bash_script = bash_script + "\npython3 " + tf_names[1]

    for rm in ["\nrm " + n for n in tf_names]:
        bash_script = bash_script + rm
        # ^ will remove unnecessary run files on the cluster after the function is called (local files are kept).

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

    _active_batches.append((tf_names[0], cluster))  # ready to be sent to the cluster + run by launch()!
    if cluster not in _active_clusters:
        _active_clusters.append(cluster)

