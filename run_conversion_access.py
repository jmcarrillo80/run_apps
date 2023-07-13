from modules.subprocessRunTask import RunTask
from pathlib import Path
import json


cwd = Path(r'C:\Users\Jorge.Carrillo1\Documents\DESARROLLO\VS CODE\DB_CONVERSION_REPO\db_conversion')
script_file = 'app.py'
directory_databases = r'"C:\Users\Jorge.Carrillo1\Desktop\S3D CAXPERTS"'
db_name = 'IPP_SPPID_PipingComponents'
db_fileFormat = 'mdb'
tables = ["InlineComp_Prun", "Eqp_Nozzle", "Instruments", "PID"]
#tables = json.dumps(tables).replace(str('"'), str("'"))
tables = str(tables).replace(str('"'), str("'"))
tables = f'"{tables}"'
directory_output = r'"C:\Users\Jorge.Carrillo1\Desktop\S3D CAXPERTS\test"'
output_fileformat = 'parquet'
stdout_file = 'stdout_conversion.txt'
stderr_file = 'stderr_conversion.txt'


subprocess_run_args = f'venv\Scripts\python {script_file} {directory_databases} {db_name} {db_fileFormat} {tables} {directory_output} {output_fileformat}'
print(subprocess_run_args)
taskObj = RunTask(subprocess_run_args=subprocess_run_args, cwd=cwd, stdout_file=stdout_file, stderr_file=stderr_file)
result = taskObj.subprocess_run()

