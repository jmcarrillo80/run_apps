from modules.subprocessRunTask import RunTask
from modules.subprocessParameters import ProjectsParameters
from pathlib import Path
import json
import os


ROOT_DIRECTORY = Path(r'C:\PROJECTS')
APP_DIRECTORY = ROOT_DIRECTORY/'_app'
INI_FILES_DIRECTORY = ROOT_DIRECTORY/'_template'

def s3d_extractions():
    with open('projects.json') as f:
        project_dict = json.load(f)

    with open('report_adapter_s3d_shared_config.json') as f:
        s3d_shared_config_dict = json.load(f)

    parameters = ProjectsParameters(project_dict=project_dict, report_adapter_config_dict=s3d_shared_config_dict)
    project_s3d_parameters_dict = parameters.get_s3d_parameters()

    for project_id, project_params in project_s3d_parameters_dict.items():
        root_project = ROOT_DIRECTORY/project_id
        root_s3d_ini_files = INI_FILES_DIRECTORY/'ini_s3d'
        if not os.path.exists(root_project):
            os.makedirs(root_project)
        subdirectory_project = ['ini_s3d', 'output_s3d', 'logFilesExtractions_s3d', 'logFilesConversions_s3d', 'parquetFiles_s3d']
        directories_project = ProjectsParameters.createDirectory({'root':root_project, 'subdirectories':subdirectory_project})
        for asset_type, asset_type_config in project_params.items():
            extraction_params = ProjectsParameters.get_s3d_extractionParameters(directories_project, asset_type, asset_type_config)
            conversion_params = ProjectsParameters.get_s3d_conversionParameters(directories_project, asset_type_config, extraction_params)
            ProjectsParameters.create_s3d_configFile(s3d_template_ini_file=Path(root_s3d_ini_files/extraction_params["config_file"]), s3d_project_ini_file=Path(directories_project["ini_s3d"]/extraction_params["config_file"]), s3d_database_outputfile=str(directories_project["output_s3d"]/extraction_params["database_file"]))
            extraction_run_args = []
            extraction_run_args.append(f'C:\Program Files (x86)\CAXperts\\3D ReportAdapter\\3D ReportAdapter.exe')
            extraction_run_args.append(f'-plant:{extraction_params["plant"]}')
            extraction_run_args.append(f'-config:{directories_project["ini_s3d"]}\{extraction_params["config_file"]}')
            extraction_run_args.append(f'-filter:{extraction_params["filter"]}')
            extraction_run_args.append(f'-output:{directories_project["output_s3d"]}\{extraction_params["database_file"]}')
            extraction_run_args.append(f'-permissiongroup:{extraction_params["permissiongroup"]}')
            extraction_run_args.append("-cleanrules")
            extraction_run_args.append("-forceexit:1")

            # extraction_run_args = f'"C:\Program Files (x86)\CAXperts\\3D ReportAdapter\\3D ReportAdapter.exe" '\
            #                       f'-plant:"{extraction_params["plant"]}" '\
            #                       f'-config:"{directories_project["ini_s3d"]}\{extraction_params["config_file"]}" '\
            #                       f'-filter:"{extraction_params["filter"]}" '\
            #                       f'-output:"{directories_project["output_s3d"]}\{extraction_params["database_file"]}" '\
            #                       f'-permissiongroup:"{extraction_params["permissiongroup"]}" '\
            #                       f'-cleanrules -forceexit:1'

            extractionTaskObj = RunTask(subprocess_run_args=extraction_run_args, cwd=Path(r'C:\Program Files (x86)\CAXperts\\3D ReportAdapter'), stdout_file=extraction_params['stdout_file'], stderr_file=extraction_params['stderr_file'])
            print('Subprocess running ...')
            print(f'plant: {extraction_params["plant"]}\npermissiongroup: {extraction_params["permissiongroup"]}\ndirectory_ini: {directories_project["ini_s3d"]}\nini_file: {extraction_params["config_file"]}\nfilter: {extraction_params["filter"]}\ndirectory_database: {directories_project["output_s3d"]}\ndatabase_file: {extraction_params["database_file"]}')            
            result =  extractionTaskObj.subprocess_run()
            if result.returncode == 0:
                print('subprocess ID: {} --> executed successfully\n'.format(result.pid))
                conversion_run_args = []
                conversion_run_args.append(APP_DIRECTORY/'DatabaseTablesToParquet/venv/Scripts/python')
                conversion_run_args.append('app.py')
                conversion_run_args.append(conversion_params["directory_db"])
                conversion_run_args.append(conversion_params["db_name"])
                conversion_run_args.append(conversion_params["db_fileformat"])
                conversion_run_args.append(json.dumps(conversion_params["tables"]))
                conversion_run_args.append(conversion_params["directory_output"])
                conversion_run_args.append(conversion_params["output_fileFormat"])

                # conversion_run_args = f'venv\Scripts\python app.py "{conversion_params["directory_db"]}" {conversion_params["db_name"]} {conversion_params["db_fileformat"]} '\
                #                       f'"{conversion_params["tables"]}" "{conversion_params["directory_output"]}" {conversion_params["output_fileFormat"]}'

                conversionTaskObj = RunTask(subprocess_run_args=conversion_run_args, cwd=APP_DIRECTORY/'DatabaseTablesToParquet', stdout_file=conversion_params['stdout_file'], stderr_file=conversion_params['stderr_file'])
                print('Tables Conversion:')
                print(f'parent subprocess ID: {result.pid}\ndatabase: {conversion_params["db_name"]}\ntables: {conversion_params["tables"]}')
                resultConversion = conversionTaskObj.subprocess_run()
                if resultConversion.returncode == 0:
                    print('subprocess ID: {} --> executed successfully\n'.format(resultConversion.pid))
                else:
                    print('subprocess ID: {} --> NOT executed successfully\n'.format(resultConversion.pid))
            else:
                print('subprocess ID: {} --> NOT executed successfully\n'.format(result.pid))

