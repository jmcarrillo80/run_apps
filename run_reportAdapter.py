from modulos.subprocessRunTask import RunTask
from modulos.subprocessParameters import ProjectParameters
import json
from pathlib import Path
import os


ROOT_DIRECTORY = Path(r'C:\PROJECTS')
APP_DIRECTORY = ROOT_DIRECTORY/'_app'


with open('projects.json') as f:
    project_dict = json.load(f)

with open('report_adapter_s3d_shared_config.json') as f:
    s3d_shared_config_dict = json.load(f)

parameters = ProjectParameters(project_dict=project_dict, report_adapter_config_dict=s3d_shared_config_dict)
project_s3d_parameters_dict = parameters.getParameters()

print(project_s3d_parameters_dict)

results = []
for project_id, project_params in project_s3d_parameters_dict.items():
    if not os.path.exists(ROOT_DIRECTORY/project_id):
        os.makedirs(ROOT_DIRECTORY/project_id)
    for asset_type, asset_type_config in project_params.items():
        plant = asset_type_config['s3d_plant_name']
        directory_ini = ROOT_DIRECTORY/project_id/'ini_s3d'
        if not os.path.exists(directory_ini):
            os.makedirs(directory_ini)
        config_file = asset_type_config['s3d_adapter_ini_file']
        filter = asset_type_config['filter']
        directory_output = ROOT_DIRECTORY/project_id/'Output_s3d'
        if not os.path.exists(directory_output):
            os.makedirs(directory_output)        
        database_file = asset_type_config['output_database']
        directory_logfiles = ROOT_DIRECTORY/project_id/'logfiles_s3d'
        if not os.path.exists(directory_logfiles):
            os.makedirs(directory_logfiles)        
        stdout_file = directory_logfiles/f"{asset_type}_stdout.txt"
        stderr_file = directory_logfiles/f"{asset_type}_stderr.txt"
        tables = asset_type_config['table_name']
        print(f"\nplant:{plant}\ndirectory_ini:{directory_ini}\nconfig_file:{config_file}\nfilter:{filter}\ndirectory_output:{directory_output}\ndatabase_file:{database_file}\nstdout:{stdout_file}\nstderr:{stderr_file}")
        
        subprocess_run_args = f'"C:\Program Files (x86)\CAXperts\\3D ReportAdapter\\3D ReportAdapter.exe" -plant:{plant} -config:{directory_ini}\{config_file} -filter:{filter} \
                                -output:{directory_output}\{database_file} -cleanrules -forceexit:1'
        taskObj = RunTask(subprocess_run_args=subprocess_run_args, cwd=Path(r'C:\Program Files (x86)\CAXperts\\3D ReportAdapter'), stdout_file=stdout_file, stderr_file=stderr_file)
        result =  taskObj.subprocess_run()
        results.append((taskObj, result, tables))

for taskObj, result, tables in results:
    returnCode = taskObj.subprocess_logfiles(result)
    print(returnCode)