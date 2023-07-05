import subprocess
import time

class RunTask():

    def __init__(self, subprocess_run_args, cwd, stdout_file, stderr_file):
        self.subprocess_run_args = subprocess_run_args
        self.cwd = cwd
        self.stdout_file = stdout_file
        self.stderr_file = stderr_file

    
    def subprocess_run(self):
        result =  subprocess.Popen(self.subprocess_run_args, cwd=self.cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, text=True)
        out = ''
        err = ''
        while result.poll() == None:
            for line in result.stdout:
                print(line)
                out += line+'\n'
            for line in result.stderr:
                print(line)
                err += line+'\n'
            time.sleep(1)
        if out != '':
            with open(self.stdout_file, "w") as f_stdout:
                f_stdout.write(out)
        if err != '':
            with open(self.stderr_file, "w") as f_stderr:
                f_stderr.write(err)
        return result           
