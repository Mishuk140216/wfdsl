from os import path
import os
from pathlib import Path

from ...exechelper import func_exec_run
from ...argshelper import get_posix_data_args, get_posix_output_folder_args

fastqc = path.join(path.abspath(path.dirname(__file__)), path.join('lib', 'fastqc'))

def prepare_args(fs, data, outdir, paramindex, *args):
    cmdargs = [data, "--outdir=" + outdir]
                           
    for arg in args[paramindex:]:
        cmdargs.append(arg)
    
    outpath = Path(data).stem + "_fastqc.html"
    outpath = os.path.join(outdir, os.path.basename(outpath))
    if fs.exists(outpath):
        outpath = fs.unique_filename(outdir, Path(outpath).stem, Path(outpath).suffix)
        
    return cmdargs, outpath
            
def run_fastqc(context, *args, **kwargs):
    
    paramindex, data, fs = get_posix_data_args(0, 'data', context, *args, **kwargs)
    outdir = get_posix_output_folder_args(paramindex, 'outdir', fs, context, *args, **kwargs)

    outpath = outdir
    err = ''
    if fs.isfile(data):
        cmdargs, outpath = prepare_args(fs, data, outdir, paramindex, *args)
        _,err = func_exec_run(fastqc, *cmdargs)
    else:
        for r, _, f in os.walk(data):
            for datafile in [os.path.join(r, file) for file in f if file.endswith(".fastq") or file.endswith(".fq")]:
                try:
                    cmdargs, _ = prepare_args(fs, datafile, outdir, paramindex, *args)
                    _,err = func_exec_run(fastqc, *cmdargs)
                except Exception as err:
                    context.err.append(str(err))
                        
    stripped_path = fs.strip_root(outpath)
    if not fs.exists(outpath):
        raise ValueError("FastQC could not generate the file " + stripped_path + " due to error " + err)
    
    return stripped_path