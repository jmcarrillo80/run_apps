from modules.subprocessRunTask import RunTask
from modules.subprocessParameters import ProjectsParameters
from modules.KeyVault import get_sas_token, get_storage_account_name
from modules.dataLake import initialize_storage_account_sas, create_directory, get_directory, upload_file_to_directory_bulk
from datetime import datetime
from pathlib import Path
import json
import os


ROOT_DIRECTORY = Path(r'C:\PROJECTS')
APP_DIRECTORY = ROOT_DIRECTORY/'_app'
INI_FILES_DIRECTORY = ROOT_DIRECTORY/'_template'
CONTAINER = 'landing'
SAS_TOKEN = get_sas_token()
STORAGE_ACCOUNT_NAME = get_storage_account_name()

def sppid_extractions():
    now_date = datetime.utcnow()
    service_client = initialize_storage_account_sas(storage_account_name=STORAGE_ACCOUNT_NAME, sas_token=SAS_TOKEN)
    with open('projects.json') as f:
        project_dict = json.load(f)

    with open('report_adapter_sppid_shared_config.json') as f:
        sppid_shared_config_dict = json.load(f)

    parameters = ProjectsParameters(project_dict=project_dict, report_adapter_config_dict=sppid_shared_config_dict)
    project_sppid_parameters_dict = parameters.get_sppid_parameters()

    for project_id, project_params in project_sppid_parameters_dict.items():
        root_project_datalake = create_directory(service_client=service_client, file_system=CONTAINER, directory=f'test_vscode/{project_id}')
        app_name_datalake_directory = create_directory(service_client=service_client, file_system=CONTAINER, directory=f'{root_project_datalake.path_name}/SPPID')
        root_project = ROOT_DIRECTORY/project_id
        root_sppid_ini_files = INI_FILES_DIRECTORY/'ini_sppid'
        if not os.path.exists(root_project):
            os.makedirs(root_project)
        subdirectory_project = ['ini_sppid', 'output_sppid', 'logFilesExtractions_sppid', 'logFilesConversions_sppid', 'parquetFiles_sppid']
        directories_project = ProjectsParameters.createDirectory({'root':root_project, 'subdirectories':subdirectory_project})
        for asset_type, asset_type_config in project_params.items():
            extraction_params = ProjectsParameters.get_sppid_extractionParameters(directories_project, asset_type, asset_type_config)
            conversion_params = ProjectsParameters.get_sppid_conversionParameters(directories_project, asset_type_config, extraction_params)
            ProjectsParameters.create_sppid_configFile(sppid_template_ini_file=Path(root_sppid_ini_files/extraction_params["config_file"]), sppid_project_ini_file=Path(directories_project["ini_sppid"]/extraction_params["config_file"]), sppid_database_outputfile=str(directories_project["output_sppid"]/extraction_params["database_file"]))
            extraction_run_args = []
            extraction_run_args.append(f'C:\Program Files (x86)\CAXperts\PID ReportAdapter\PID ReportAdapter.exe')
            extraction_run_args.append('/output')
            extraction_run_args.append(f'{directories_project["output_sppid"]}\{extraction_params["database_file"]}')
            extraction_run_args.append('/site')
            extraction_run_args.append(f'{extraction_params["site"]}')
            extraction_run_args.append('/plant')
            extraction_run_args.append(f'{extraction_params["plant"]}')
            extraction_run_args.append('/config')
            extraction_run_args.append(f'{directories_project["ini_sppid"]}\{extraction_params["config_file"]}')
            extraction_run_args.append('/log')
            extraction_run_args.append(f'{extraction_params["stdout_file"]}')

            # extraction_run_args = f'"C:\Program Files (x86)\CAXperts\PID ReportAdapter\PID ReportAdapter.exe" '\
            #                       f'/output "{directories_project["output_sppid"]}\{extraction_params["database_file"]}" '\
            #                       f'/site "{extraction_params["site"]}" '\
            #                       f'/plant "{extraction_params["plant"]}" '\
            #                       f'/config "{directories_project["ini_sppid"]}\{extraction_params["config_file"]}" '\
            #                       f'/log "{extraction_params["stdout_file"]}"'

            extractionTaskObj = RunTask(subprocess_run_args=extraction_run_args, cwd=Path(r'C:\Program Files (x86)\CAXperts\PID ReportAdapter'), stdout_file=extraction_params['stdout_file'], stderr_file=extraction_params['stderr_file'])
            print('Subprocess running ...')
            print(f'plant: {extraction_params["plant"]}\ndirectory_ini: {directories_project["ini_sppid"]}\nini_file: {extraction_params["config_file"]}\nsite: {extraction_params["site"]}\ndirectory_database: {directories_project["output_sppid"]}\ndatabase_file: {extraction_params["database_file"]}')            
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
                    for table in conversion_params["tables"]:
                        table_name = f'{conversion_params["db_name"]}_{table}'
                        table_name_datalake_directory = create_directory(service_client=service_client, file_system=CONTAINER, directory=f'{app_name_datalake_directory.path_name}/{table_name}')
                        _pdwetl_year_datalake_directory = create_directory(service_client=service_client, file_system=CONTAINER, directory=f'{table_name_datalake_directory.path_name}/_pdwetl_year={now_date.year}')
                        _pdwetl_month_datalake_directory = create_directory(service_client=service_client, file_system=CONTAINER, directory=f'{_pdwetl_year_datalake_directory.path_name}/_pdwetl_month={now_date.month}')
                        _pdwetl_day_datalake_directory = create_directory(service_client=service_client, file_system=CONTAINER, directory=f'{_pdwetl_month_datalake_directory.path_name}/_pdwetl_day={now_date.day}')
                        upload_datalake_directory = get_directory(service_client=service_client, file_system=CONTAINER, directory=_pdwetl_day_datalake_directory.path_name)
                        file_name_datalake = f"{now_date.strftime('%Y-%m-%dT%H:%M:%SZ')}.parquet"
                        local_file_path = f'{conversion_params["directory_output"]}\{table_name}.parquet'
                        upload_file_to_directory_bulk(directory_client=upload_datalake_directory, file_name_client=file_name_datalake, local_file_path=local_file_path)
                else:
                    print('subprocess ID: {} --> NOT executed successfully\n'.format(resultConversion.pid))
            else:
                print('subprocess ID: {} --> NOT executed successfully\n'.format(result.pid))

