from modulos.subprocessRunTask import RunTask
from modulos.subprocessParameters import ProjectParameters
import json
import os


with open('projects.json') as f:
    project_dict = json.load(f)

with open('report_adapter_s3d_shared_config.json') as f:
    s3d_shared_config_dict = json.load(f)

parameters = ProjectParameters(project_dict=project_dict, report_adapter_config_dict=s3d_shared_config_dict)
project_s3d_parameters_dict = parameters.getParameters()

print(project_s3d_parameters_dict)

root_directory = r'\\azrcarawd001\PROJECTS'
cwd = r'C:\Program Files (x86)\CAXperts\3D ReportAdapter'

for project in project_s3d_parameters_dict:
    for asset_type in project_s3d_parameters_dict[project]:
        plant = project_s3d_parameters_dict[project][asset_type]['s3d_plant_name']
        directory_ini = os.path.join(root_directory, project, 'ini_s3d')
        config_file = project_s3d_parameters_dict[project][asset_type]['s3d_conf_file']
        filter = project_s3d_parameters_dict[project][asset_type]['filter']
        directory_output = os.path.join(root_directory, project, 'Output_s3d')
        database_file = project_s3d_parameters_dict[project][asset_type]['output_database']
        stdout_file = f"stdout_{project}__{asset_type}.txt"
        stderr_file = f"stderr_{project}__{asset_type}.txt"
        print(f"\nplant:{plant}\ndirectory_ini:{directory_ini}\nconfig_file:{config_file}\nfilter:{filter}\ndirectory_output:{directory_output}\ndatabase_file:{database_file}\nstdout:{stdout_file}\nstderr:{stderr_file}")

        # subprocess_run_args = f'"3D ReportAdapter.exe" -plant:{plant} -config:{directory_ini}\{config_file} -filter:{filter} \
        #                        -output:{directory_output}\{database_file} -cleanrules -forceexit:1'
        # taskObj = RunTask(subprocess_run_args=subprocess_run_args, cwd=cwd, stdout_file=stdout_file, stderr_file=stderr_file)
        # taskObj.subprocess_run()