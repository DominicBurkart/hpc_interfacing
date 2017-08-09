'''For exporting individual python3 functions to a cluster for faster computation.

Assumptions:

1.) your local computer and your cluster are use linux or unix (e.g. macOS).

2.) your cluster has the same modules as your local computer.

3.) your cluster uses SLURM to manage jobs.

4.) the objects you want passed into your function are picklable.
See https://docs.python.org/3/library/pickle.html#pickle-picklable about picklability.

Usage:

1.) Edit the values below so that this function can ssh / scp into your cluster of choice.

2.) place this file in the same folder as the main program and import it with "import sbatch".

3.) in whatever file holds your functions, call sbatch.load("function_name", [list, of, arguments]).

4.) when you've finished loading up function calls and you want to send them all out, call sbatch.load().

5.) drink a smoothie, if you want to!

Dominic Burkart / dominicburkart.com
'''
from time import time, sleep
import pickle, __main__

_sbatch_file_directory = "local/path/on/your/machine/autopy"
_ssh_username = "cluster_username"
_ssh_key = "/path/to_shh_key"
_host = "della5.princeton.edu"
_workdir = "/tigress/cluster_username/"
_path_to_python3 = "intel-python/3.5/20150803_185146" #della default
_mail = "email@princeton.edu"
_mail_type = "end,fail"
_active_batches = []

def launch():
    import shutil, subprocess, pipes
    def send(to, local_folder = _sbatch_file_directory, k = _ssh_key):
        e = "\nsee http://geoweb.princeton.edu/it/sshkey.html for instructions of SSH keygen."
        if local_folder.endswith("/"):
            local_folder = local_folder[:len(local_folder)-1]
        cmd = ['scp -i "' + k + '" -r "' + local_folder + '" "'+ to+ '"', "exit"]
        try:
            open(k).close()
        except FileNotFoundError:
            raise FileNotFoundError("Invalid SSH key filepath passed to sbatch.send(): "+ k + e)
        c = subprocess.run(cmd, shell=True)
        if c.returncode != 0:
            print("scp return code of "+str(c.returncode)+". retrying after 10 seconds.")
            sleep(10)
            c = subprocess.run(cmd, shell=True)
            if c.returncode == 0:
                print("scp successful!")
                return
            raise Exception("SCP Error. Return code: "+str(c.returncode))
        return

    print("preparing to launch "+str(len(_active_batches))+" sbatch jobs.")

    assert len(_active_batches) > 0

    print(_active_batches)

    #copy __main__ into the local folder
    shutil.copy(__main__.__file__, _sbatch_file_directory)
    print("main copied to the local directory.")

    #send necessary files to cluster
    send( _ssh_username + "@" + _host + ":" + _workdir )

    #ssh into the cluster and sbatch the programs
    cmdl = [['cd', _workdir + "/" + _sbatch_file_directory.split("/")[-1]]]
    cmdl.extend([['sbatch', ac] for ac in _active_batches])
    cmdl.append(['exit'])
    cmd = ""
    for l in cmdl:
        cmd = cmd + ' '.join(pipes.quote(com) for com in l) + "\n"
    c = subprocess.run(['ssh', '-i', _ssh_key, _ssh_username + '@' + _host, cmd])
    if c.returncode != 0:
        raise Exception("sbatch unsuccessful. Returncode: "+str(c.returncode))
    else:
        print("sbatch successful.")

def load(function_name, list_of_params = [], ssh_username = _ssh_username,
           ssh_key = _ssh_key, t= r"72:00:00",
           n = 1, c = 20, host = _host, workdir = _workdir,
           name = "batch"+str(time()).replace(".","_"),
           path_to_python3 = _path_to_python3,
           mail= _mail, mail_type = _mail_type,
         local = _sbatch_file_directory ):
    '''Generates necessary helper scripts to run any individual function available in the program
    that calls this function on a cluster with python3 and SLURM. Default values are specific to
    Della5 at Princeton.

    TODO: allow a bool to be passed on whether the whole directory should be sent to the cluster instead of just
    the file that called sbatch.
    '''
    import os

    previous_dir = os.curdir

    os.chdir(local)

    #filenames for everything generated by this function
    tf_names = ["sbatch_tmp"+str(time()).replace(".","_"), "wrapper"+str(time()).replace(".","_")+".py"]
    for i in range(len(list_of_params)):
        tf_names.append(name+'_'+str(time()).replace(".","_")+'_'+str(i)+'.obj') #pickle object files for each parameter

    #pickle all parameters so we can send them to the cluster
    n_bytes = 2**31
    max_bytes = 2**31 - 1
    for i in range(len(list_of_params)):
        bytes_out = pickle.dumps(list_of_params[i])
        with open(tf_names[i+2], 'wb') as f_out:
            for idx in range(0, n_bytes, max_bytes):
                f_out.write(bytes_out[idx:idx+max_bytes])

    #generate sbatch file
    sb = "\n#SBATCH "
    pars = ["-N " + str(n),
            "-t " + str(t),
            "--workdir=" + str(workdir) + "/" + _sbatch_file_directory.split("/")[-1],
            "--mail-user=" + mail,
            "--mail-type=" + mail_type]
    bash_script ="#!/bin/bash"
    for p in pars:
        bash_script = bash_script + sb + p

    bash_script = bash_script + "\n\nmodule load " + path_to_python3 + "\npython3 " + tf_names[1]

    for rm in ["\nrm "+ n for n in tf_names]:
        bash_script = bash_script + rm #will remove unncessary run files on the cluster (local files are kept).

    f = open(tf_names[0], "w")
    f.write(bash_script)
    f.close()

    _active_batches.append(tf_names[0])

    #generate python file
    pwrap = "from "+__main__.__file__.split("/")[-1].split(".")[0]+" import *\nimport pickle\nimport os"
    #^ tells the cluster to import all functions from the file that called sbatch()
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

'''

    if list_of_params != []:
        pars =  "("+" l('"+str(tf_names[2])+"')"
        for i in range(3,len(list_of_params)+2):
            pars = pars + ", l('"+tf_names[i]+"')"
        pars = pars + ")\n"
    else:
        pars = "()\n"
    
    pwrap = pwrap + function_name + pars

    f = open(tf_names[1], "w")
    f.write(pwrap)
    f.close()

    os.chdir(previous_dir)

    
