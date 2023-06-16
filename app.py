from modulos.subprocessRunTask import RunTask
from modulos.subprocessParameters import ProjectParameters
import json
from pathlib import Path
import os


ROOT_DIRECTORY = Path(r'C:\Users\Jorge.Carrillo1\Desktop\S3D CAXPERTS\test')  #Path(r'C:\PROJECTS')
APP_DIRECTORY = ROOT_DIRECTORY/'_app'

def main():
    with open('projects.json') as f:
        project_dict = json.load(f)

    with open('report_adapter_s3d_shared_config.json') as f:
        s3d_shared_config_dict = json.load(f)

    parameters = ProjectParameters(project_dict=project_dict, report_adapter_config_dict=s3d_shared_config_dict)
    project_s3d_parameters_dict = parameters.getParameters()    

    results = []
    for project_id, project_params in project_s3d_parameters_dict.items():        
        root_project = ROOT_DIRECTORY/project_id
        if not os.path.exists(root_project):
            os.makedirs(root_project)        
        subprocess_directories = ProjectParameters.createDirectory({'root':root_project, 'subdirectories':['ini_s3d', 'output_s3d', 'logFilesExtractions_s3d', 'logFilesConversions_s3d', 'parquetFiles_s3d']})
        for asset_type, asset_type_config in project_params.items():
            conversion_params = {}
            plant = asset_type_config['s3d_plant_name']
            config_file = asset_type_config['s3d_adapter_ini_file']
            filter = asset_type_config['filter']
            database_file = asset_type_config['output_database']
            stdout_file = subprocess_directories['logFilesExtractions_s3d']/f"{asset_type}_stdout.txt"
            stderr_file = subprocess_directories['logFilesExtractions_s3d']/f"{asset_type}_stderr.txt"
            conversion_params['directory_db'] = f'"{subprocess_directories["output_s3d"]}"'
            conversion_params['db_name'] = database_file.split('.')[0]
            conversion_params['db_fileformat'] = database_file.split('.')[1]
            conversion_params['tables'] = asset_type_config['table_name']
            conversion_params['directory_parquet'] = f'"{subprocess_directories["parquetFiles_s3d"]}"'
            conversion_params['stdout_file'] = subprocess_directories['logFilesConversions_s3d']/f"{database_file.split('.')[0]}_stdout.txt"
            conversion_params['stderr_file'] = subprocess_directories['logFilesConversions_s3d']/f"{database_file.split('.')[0]}_stderr.txt"

            subprocess_run_args = f'"C:\Program Files (x86)\CAXperts\\3D ReportAdapter\\3D ReportAdapter.exe" -plant:{plant} -config:{subprocess_directories["ini_s3d"]}\{config_file}\n \
            -filter:{filter}\n \
            -output:{subprocess_directories["output_s3d"]}\{database_file} -cleanrules\n \
            -forceexit:1'   
            
            taskObj = RunTask(subprocess_run_args=subprocess_run_args, cwd=Path(r'C:\Program Files (x86)\CAXperts\\3D ReportAdapter'), stdout_file=stdout_file, stderr_file=stderr_file)
            result =  taskObj.subprocess_run()
            results.append((taskObj, result, conversion_params))
            print(f'subprocess ID: {result.pid}\nplant: {plant}\ndirectory_ini: {subprocess_directories["ini_s3d"]}\nini_file: {config_file}\nfilter: {filter}\ndirectory_database: {subprocess_directories["output_s3d"]}\ndatabase_file: {database_file}\n')


    for taskObj, result, conversion_params in results:
        returnCode = taskObj.subprocess_logfiles(result)
        if returnCode == 0:
            print('subprocess ID: {} --> executed successfully\n'.format(result.pid))
            subprocess_conversion_run_args = f'venv\Scripts\python app.py {conversion_params["directory_db"]} {conversion_params["db_name"]} {conversion_params["db_fileformat"]} {conversion_params["tables"]} {conversion_params["directory_parquet"]} parquet'
            conversionTaskObj = RunTask(subprocess_run_args=subprocess_conversion_run_args, cwd=APP_DIRECTORY/'DatabaseTablesToParquet', stdout_file=conversion_params['stdout_file'], stderr_file=conversion_params['stderr_file'])
            resultConversion = conversionTaskObj.subprocess_run()
            print('Tables Conversion:')
            print(f'subprocess ID: {resultConversion.pid}\nparent subprocess ID: {result.pid}\ndatabase: {conversion_params["db_name"]}\ntables: {conversion_params["tables"]}\n')
            returnCodeConversion = conversionTaskObj.subprocess_logfiles(resultConversion)
            if returnCodeConversion == 0:
                print('subprocess ID: {} --> executed successfully\n'.format(resultConversion.pid))
            else:
                print('subprocess ID: {} --> NOT executed successfully\n'.format(resultConversion.pid))
        else:
            print('subprocess ID: {} --> NOT executed successfully\n'.format(result.pid))





if __name__== "__main__":
    main()
