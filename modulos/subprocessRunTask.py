import subprocess


class RunTask():

    def __init__(self, subprocess_run_args:str, cwd:str, stdout_file:str, stderr_file:str):
        self.subprocess_run_args = subprocess_run_args
        self.cwd = cwd
        self.stdout_file = stdout_file
        self.stderr_file = stderr_file

    
    def subprocess_run(self):
        f_stdout = open(self.stdout_file, "w")
        f_stderr = open(self.stderr_file, "w")
        result = subprocess.run(self.subprocess_run_args, cwd=self.cwd, stdout=f_stdout, stderr=f_stderr, shell=True, text=True, check=True)
        print(f"\nreturncode:\n{result.returncode}")

        