from modulos.subprocessRunTask import RunTask


cwd = r'C:\Users\Jorge.Carrillo1\Documents\DESARROLLO\VS CODE\DB_CONVERSION_REPO\db_conversion'
script_file = 'app.py'
directory_databases = r'"C:\Users\Jorge.Carrillo1\Desktop\S3D CAXPERTS"'
db_fileFormat = 'mdb'
output_fileformat = 'parquet'
stdout_file = 'stdout_conversion.txt'
stderr_file = 'stderr_conversion.txt'

subprocess_run_args = f"venv\Scripts\python {script_file} {directory_databases} {db_fileFormat} {output_fileformat}"
taskObj = RunTask(subprocess_run_args=subprocess_run_args, cwd=cwd, stdout_file=stdout_file, stderr_file=stderr_file)
taskObj.subprocess_run()