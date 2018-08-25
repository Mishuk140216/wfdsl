import os
from os import path

from ...exechelper import func_exec_run
from ...fileop import PosixFileSystem
from ....util import Utility
from ...ssh import ssh_hadoop_command, scp_get, scp_put

cluster = '206.12.102.75'
user = 'hadoop'
password = 'spark#2018'

python_ex = path.join(path.abspath(path.dirname(__file__)), path.join('lib', 'venv', 'bin', 'python'))
spark_submit_app = 'spark-submit'

def run_apachebeam(*args, **kwargs):
    return func_exec_run(python_ex, args)     

def count_words(*args, **kwargs):
    
    paramindex = 0
    if 'input' in kwargs.keys():
        inputfile = kwargs['input']
    else:
        if len(args) == paramindex:
            raise ValueError("Argument 'input' missing.")
        inputfile = args[paramindex]
        paramindex +=1
    
    if 'output' in kwargs.keys():
        output = kwargs['output']
    else:
        if len(args) == paramindex:
            raise ValueError("Argument 'output' missing.")
        output = args[paramindex]
        paramindex +=1
    
    inputfile = Utility.get_normalized_path(inputfile)
    output = Utility.get_normalized_path(output)
    
    args = ['-m', 'apache_beam.examples.wordcount', inputfile, output]
    
    _err, _ = run_apachebeam(args)
    
    fs = PosixFileSystem(Utility.get_rootdir(2))
    stripped_path = fs.strip_root(output)
    if not os.path.exists(output):
        raise ValueError("CountWords could not generate the file " + stripped_path + " due to error: " + _err)
    
    return stripped_path

def copy_posix_file_to_cluster(data):
    # copy the data to cluster if it is a local file. HDFS file will be accessed by cluster directly
    if (Utility.fs_type_by_prefix(data) == 'posix'):
        data = Utility.get_normalized_path(data)
        remotepath = os.path.join('/home/phenodoop/phenoproc/storage/public/', os.path.basename(data))
        scp_put(cluster, user, password, data, remotepath)
        data = remotepath
    return data
        
def run_beam_quality(*args, **kwargs):
    
    paramindex = 0
    if 'data' in kwargs.keys():
        data = kwargs['data']
    else:
        if len(args) == paramindex:
            raise ValueError("Argument missing error in FastQC.")
        data = args[paramindex]
        paramindex +=1
 
    data = copy_posix_file_to_cluster(data);
    
    outdir = ''
    if 'outdir' in kwargs.keys():
        outdir = kwargs['outdir']
    else:
        if len(args) > paramindex:
            outdir = args[paramindex]
            paramindex +=1
        
    runner = 'spark'
    if 'runner' in kwargs.keys():
        runner = kwargs['runner']
    else:
        if len(args) > paramindex:
            runner = args[paramindex]
            paramindex +=1

    ssh_cmd = ''
    cmd_outdir = ''
    if outdir and Utility.fs_type_by_prefix(data) != 'posix':
        cmd_outdir = "--outDir={0}".format(outdir)
    if runner == 'spark':
        ssh_cmd = "spark-submit --class edu.usask.srlab.biowl.beam.CheckQ --master yarn --deploy-mode cluster --driver-memory 4g --executor-memory 4g --executor-cores 4 /home/phenodoop/phenoproc/app/biowl/libraries/apachebeam/lib/beamflows-bundled-spark.jar --inputFile={0} {1} --runner=SparkRunner".format(data, cmd_outdir)
    else:
        ssh_cmd = "mvn compile exec:java -Dexec.mainClass=edu.usask.srlab.biowl.beam.CheckQ -Dexec.args='--inputFile={0} {1}' -Pdirect-runner".format(data, cmd_outdir)
    
    outpath = ssh_hadoop_command(cluster, user, password, ssh_cmd)
    
    if (Utility.fs_type_by_prefix(data) == 'posix'):
        if outdir:
            outdir = Utility.get_normalized_path(outdir)
        else:
            outdir = path.dirname(data)

        if not os.path.exists(outdir):
            os.makedirs(outdir)

        scp_get(cluster, user, password, outpath, outdir)
    
        fs = Utility.fs_by_prefix(outpath)
        stripped_path = fs.strip_root(outpath)
        if not os.path.exists(outpath):
            raise ValueError("FastQC could not generate the file " + stripped_path)
    
    return outpath


def run_beam_alignment(*args, **kwargs):
    
    paramindex = 0
    ref = ''
    if 'ref' in kwargs.keys():
        ref = kwargs['ref']
    else:
        if len(args) == paramindex:
            raise ValueError("Argument error")
        ref = args[paramindex]
        paramindex +=1
    
    ref = copy_posix_file_to_cluster(ref)
    
    data1 = ''
    if 'data1' in kwargs.keys():
        data1 = kwargs['data1']
    else:
        if len(args) == paramindex:
            raise ValueError("Argument error")
        data1 = args[paramindex]
        paramindex +=1
    
    data1 = copy_posix_file_to_cluster(data1)
    
    data2 = ''
    if 'data2' in kwargs.keys():
        data2 = kwargs['data2']
    else:
        if len(args) > paramindex:
            data2 = args[paramindex]
            paramindex +=1
    
    if data2:
        data2 = copy_posix_file_to_cluster(data2)
    
    output = ''    
    if 'output' in kwargs.keys():
        output = kwargs['output']
    else:
        if len(args) > paramindex:
            output = args[paramindex]
            paramindex +=1

    runner = 'spark'
    if 'runner' in kwargs.keys():
        runner = kwargs['runner']
    else:
        if len(args) > paramindex:
            runner = args[paramindex]
            paramindex +=1

    ssh_cmd = ''
    if output and Utility.fs_type_by_prefix(data1) != 'posix':
        output = "--output={0}".format(output)
        
    if runner == 'spark':
        ssh_cmd = "spark-submit --class edu.usask.srlab.biowl.beam.Alignment --master yarn --deploy-mode cluster --driver-memory 4g --executor-memory 4g --executor-cores 4 /home/phenodoop/phenoproc/app/biowl/libraries/apachebeam/lib/beamflows-bundled-spark.jar --inputFile={0} {1} --runner=SparkRunner".format(ref, data1, data2, output)
    else:
        ssh_cmd = "mvn compile exec:java -Dexec.mainClass=edu.usask.srlab.biowl.beam.Alignment -Dexec.args='--inputFile={0} {1}' -Pdirect-runner".format(ref, data1, data2, output)
    
    outpath = ssh_hadoop_command(cluster, user, password, ssh_cmd)
    
    if Utility.fs_type_by_prefix(data1) == 'posix':
        if output:
            outdir = path.dirname(Utility.get_normalized_path(output))
        else:
            outdir = path.dirname(data1)

        if not os.path.exists(outdir):
            os.makedirs(outdir)

        scp_get(cluster, user, password, outpath, outdir)
    
        fs = Utility.fs_by_prefix(outpath)
        stripped_path = fs.strip_root(outpath)
        if not os.path.exists(outpath):
            raise ValueError("FastQC could not generate the file " + stripped_path)
    
    return outpath