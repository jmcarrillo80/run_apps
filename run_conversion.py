from modulos.subprocessRunTask import RunTask
from pathlib import Path

cwd = Path(r'C:\Users\Jorge.Carrillo1\Documents\DESARROLLO\VS CODE\DB_CONVERSION_REPO\db_conversion')
script_file = 'app.py'
directory_databases = r'"C:\Users\Jorge.Carrillo1\Desktop\S3D CAXPERTS"'
db_fileFormat = 'mdb'
output_fileformat = 'parquet'
stdout_file = 'stdout_conversion.txt'
stderr_file = 'stderr_conversion.txt'

results = []
subprocess_run_args = f"venv\Scripts\python {script_file} {directory_databases} {db_fileFormat} {output_fileformat}"
taskObj = RunTask(subprocess_run_args=subprocess_run_args, cwd=cwd, stdout_file=stdout_file, stderr_file=stderr_file)
result = taskObj.subprocess_run()
results.append((taskObj, result))
for taskObj, result in results:
    returnCode = taskObj.subprocess_logfiles(result)
    print(returnCode)