from modules.subprocessRunTask import RunTask
from pathlib import Path
import json


cwd = Path(r'C:\Users\Jorge.Carrillo1\Documents\DESARROLLO\VS CODE\DB_CONVERSION_REPO\db_conversion')
script_file = 'app.py'
directory_databases = r'C:\Users\Jorge.Carrillo1\Desktop\S3D CAXPERTS'  #r'"C:\Users\Jorge.Carrillo1\Desktop\S3D CAXPERTS"' --> popen arg string
db_name = 'Equipment'
db_fileFormat = 'db'
tables = ["[Equipment&Furnishing]"]
#tables = json.dumps(tables).replace(str('"'), str("'"))
#tables = str(tables).replace(str('"'), str("'"))
#tables = f'"{tables}"'
directory_output = r'C:\Users\Jorge.Carrillo1\Desktop\S3D CAXPERTS\test'   #r'"C:\Users\Jorge.Carrillo1\Desktop\S3D CAXPERTS\test"' --> popen arg string
output_fileformat = 'parquet'
stdout_file = 'stdout_conversion.txt'
stderr_file = 'stderr_conversion.txt'

conversion_run_args = []
print(f'venv\Scripts\python {script_file} {directory_databases} {db_name} {db_fileFormat} {tables} {directory_output} {output_fileformat}')
conversion_run_args.append(r'C:\Users\Jorge.Carrillo1\Documents\DESARROLLO\VS CODE\DB_CONVERSION_REPO\db_conversion\venv\Scripts\python')
conversion_run_args.append('app.py')
conversion_run_args.append(directory_databases)
conversion_run_args.append(db_name)
conversion_run_args.append(db_fileFormat)
conversion_run_args.append(json.dumps(tables))
conversion_run_args.append(directory_output)
conversion_run_args.append(output_fileformat)
# conversion_run_args = f'venv\Scripts\python {script_file} {directory_databases} {db_name} {db_fileFormat} {tables} {directory_output} {output_fileformat}'
print(conversion_run_args)
taskObj = RunTask(subprocess_run_args=conversion_run_args, cwd=cwd, stdout_file=stdout_file, stderr_file=stderr_file)
result = taskObj.subprocess_run()
