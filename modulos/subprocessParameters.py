from datetime import datetime
from pytz import timezone
import os


class ProjectsParameters():

    def __init__(self, project_dict, report_adapter_config_dict):
        self.project_dict = project_dict
        self.report_adapter_config_dict = report_adapter_config_dict

    def getParameters(self):
        project_report_adapter_dict = {}
        for project in self.project_dict:
            project_dict_keys = self.project_dict[project]
            project_report_adapter_config_dict = self.report_adapter_config_dict.copy()
            if 's3d_report_adapter' in project_dict_keys:
                s3d_report_adapter_dict = project_dict_keys['s3d_report_adapter']
                for asset_type in s3d_report_adapter_dict['blacklist']:
                    if asset_type in project_report_adapter_config_dict:
                        project_report_adapter_config_dict.pop(asset_type)
                if len(project_report_adapter_config_dict) > 0:
                    for asset_type in project_report_adapter_config_dict:
                        project_report_adapter_config_dict[asset_type]['s3d_plant_name'] = s3d_report_adapter_dict['s3d_plant_name']
                        if asset_type in s3d_report_adapter_dict['overrides']:
                            project_report_adapter_config_dict[asset_type]['filter'] = s3d_report_adapter_dict['overrides'][asset_type]['filter']
                    project_report_adapter_dict[project] = project_report_adapter_config_dict
        return project_report_adapter_dict
    
    @staticmethod
    def getExtractionParameters(subprocess_directories, asset_type, asset_type_config):
        now_date = datetime.now(timezone('America/Chicago')).strftime('%Y-%m-%d')
        extraction_params = {}        
        extraction_params['plant'] = asset_type_config['s3d_plant_name']
        extraction_params['config_file'] = asset_type_config['s3d_adapter_ini_file']
        extraction_params['filter'] = asset_type_config['filter']
        extraction_params['permissiongroup'] = asset_type_config['permissiongroup']
        extraction_params['database_file'] = asset_type_config['output_database']        
        extraction_params['stdout_file'] = subprocess_directories['logFilesExtractions_s3d']/f"{now_date}__{asset_type}_stdout.txt"
        extraction_params['stderr_file'] = subprocess_directories['logFilesExtractions_s3d']/f"{now_date}__{asset_type}_stderr.txt"
        return extraction_params
    
    @staticmethod
    def getConversionParameters(subprocess_directories, asset_type_config, extraction_params):
        now_date = datetime.now(timezone('America/Chicago')).strftime('%Y-%m-%d')
        conversion_params = {}
        conversion_params['directory_db'] = f'"{subprocess_directories["output_s3d"]}"'
        conversion_params['db_name'] = extraction_params['database_file'].split('.')[0]
        conversion_params['db_fileformat'] = extraction_params['database_file'].split('.')[1]
        conversion_params['tables'] = asset_type_config['table_name']
        conversion_params['directory_output'] = f'"{subprocess_directories["parquetFiles_s3d"]}"'
        conversion_params['output_fileFormat'] = 'parquet'
        conversion_params['stdout_file'] = subprocess_directories['logFilesConversions_s3d']/f"{now_date}__{extraction_params['database_file'].split('.')[0]}_stdout.txt"
        conversion_params['stderr_file'] = subprocess_directories['logFilesConversions_s3d']/f"{now_date}__{extraction_params['database_file'].split('.')[0]}_stderr.txt"
        return conversion_params
    
    @staticmethod
    def createDirectory(directories):
        subprocess_directories = {}
        root = directories['root']
        if not os.path.exists(root):
            os.makedirs(root)
        for subdirectory in directories['subdirectories']:
            if not os.path.exists(root/subdirectory):
                os.makedirs(root/subdirectory)
            subprocess_directories[subdirectory] = root/subdirectory
        return subprocess_directories
    