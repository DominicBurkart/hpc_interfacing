# hpc_interfacing
Easy-to-use tools for network computing

## sbatch.py
sbatch.py is a simple python 3 module that allows you to call whatever function you want, but have the calculations performed on your SLURM-based linux cluster.

Assumptions:

1.) your local computer and your cluster both use linux or unix (e.g. macOS).

2.) your cluster has the same modules as your local computer.

3.) your cluster uses SLURM to manage jobs.

4.) the objects you want passed into your function are picklable.
See https://docs.python.org/3/library/pickle.html#pickle-picklable about picklability.

Usage:

1.) Edit the values at the top of sbatch.py so that this function can ssh / scp into your cluster of choice.

2.) place this file in the same folder as the main program and import it with "import sbatch".

3.) in whatever file holds your functions, call sbatch.load("function_name", [list, of, arguments]).

4.) when you've finished loading up function calls and you want to send them all out, call sbatch.launch().

5.) drink a smoothie, if you want to!
