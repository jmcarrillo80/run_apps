import subprocess


class RunTask():

    def __init__(self, subprocess_run_args:str, cwd:str, stdout_file:str, stderr_file:str):
        self.subprocess_run_args = subprocess_run_args
        self.cwd = cwd
        self.stdout_file = stdout_file
        self.stderr_file = stderr_file

    
    def subprocess_run(self):
        result =  subprocess.Popen(self.subprocess_run_args, cwd=self.cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, text=True)
        print('Subprocesses running ...')
        return result
           
    def subprocess_logfiles(self, result:subprocess.Popen):
        result.wait()
        (out,err) = result.communicate()
        with open(self.stdout_file, "w") as f_stdout:
            f_stdout.write(out)
        with open(self.stderr_file, "w") as f_stderr:
            f_stderr.write(err)
        return result.returncode    

