from datetime import datetime
from pytz import timezone
import shutil
import os


class ProjectsParameters():

    def __init__(self, project_dict, report_adapter_config_dict):
        self.project_dict = project_dict
        self.report_adapter_config_dict = report_adapter_config_dict

    def get_s3d_parameters(self):
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
    
    def get_sppid_parameters(self):
        project_report_adapter_dict = {}
        for project in self.project_dict:
            project_dict_keys = self.project_dict[project]
            project_report_adapter_config_dict = self.report_adapter_config_dict.copy()
            if 'sppid_report_adapter' in project_dict_keys:
                sppid_report_adapter_dict = project_dict_keys['sppid_report_adapter']
                for asset_type in sppid_report_adapter_dict['blacklist']:
                    if asset_type in project_report_adapter_config_dict:
                        project_report_adapter_config_dict.pop(asset_type)
                if len(project_report_adapter_config_dict) > 0:
                    for asset_type in project_report_adapter_config_dict:
                        project_report_adapter_config_dict[asset_type]['sppid_plant_name'] = sppid_report_adapter_dict['sppid_plant_name']
                        # if asset_type in sppid_report_adapter_dict['overrides']:
                        #     project_report_adapter_config_dict[asset_type]['filter'] = sppid_report_adapter_dict['overrides'][asset_type]['filter']
                    project_report_adapter_dict[project] = project_report_adapter_config_dict
        return project_report_adapter_dict
    
    @staticmethod
    def get_s3d_extractionParameters(directories_project, asset_type, asset_type_config):
        now_date = datetime.now(timezone('America/Chicago')).strftime('%Y-%m-%dT%H_%M_%S')
        extraction_params = {}
        extraction_params['plant'] = asset_type_config['s3d_plant_name']
        extraction_params['config_file'] = asset_type_config['s3d_adapter_ini_file']
        extraction_params['filter'] = asset_type_config['filter']
        extraction_params['permissiongroup'] = asset_type_config['permissiongroup']
        extraction_params['database_file'] = asset_type_config['output_database']        
        extraction_params['stdout_file'] = directories_project['logFilesExtractions_s3d']/f"{now_date}__{asset_type}_stdout.txt"
        extraction_params['stderr_file'] = directories_project['logFilesExtractions_s3d']/f"{now_date}__{asset_type}_stderr.txt"
        return extraction_params
    
    @staticmethod
    def get_sppid_extractionParameters(directories_project, asset_type, asset_type_config):
        now_date = datetime.now(timezone('America/Chicago')).strftime('%Y-%m-%dT%H_%M_%S')
        extraction_params = {}
        extraction_params['plant'] = asset_type_config['sppid_plant_name']
        extraction_params['config_file'] = asset_type_config['sppid_adapter_ini_file']
        extraction_params['site'] = asset_type_config['site']
        extraction_params['database_file'] = asset_type_config['output_database']        
        extraction_params['stdout_file'] = directories_project['logFilesExtractions_sppid']/f"{now_date}__{asset_type}_stdout.txt"
        extraction_params['stderr_file'] = directories_project['logFilesExtractions_sppid']/f"{now_date}__{asset_type}_stderr.txt"
        return extraction_params
    
    @staticmethod
    def get_s3d_conversionParameters(directories_project, asset_type_config, extraction_params):
        now_date = datetime.now(timezone('America/Chicago')).strftime('%Y-%m-%dT%H_%M_%S')
        conversion_params = {}
        conversion_params['directory_db'] = directories_project["output_s3d"]
        conversion_params['db_name'] = extraction_params['database_file'].split('.')[0]
        conversion_params['db_fileformat'] = extraction_params['database_file'].split('.')[1]
        conversion_params['tables'] = asset_type_config['table_name']
        conversion_params['directory_output'] = directories_project["parquetFiles_s3d"]
        conversion_params['output_fileFormat'] = 'parquet'
        conversion_params['stdout_file'] = directories_project['logFilesConversions_s3d']/f"{now_date}__{extraction_params['database_file'].split('.')[0]}_stdout.txt"
        conversion_params['stderr_file'] = directories_project['logFilesConversions_s3d']/f"{now_date}__{extraction_params['database_file'].split('.')[0]}_stderr.txt"
        return conversion_params
    
    @staticmethod
    def get_sppid_conversionParameters(directories_project, asset_type_config, extraction_params):
        now_date = datetime.now(timezone('America/Chicago')).strftime('%Y-%m-%dT%H_%M_%S')
        conversion_params = {}
        conversion_params['directory_db'] = directories_project["output_sppid"]
        conversion_params['db_name'] = extraction_params['database_file'].split('.')[0]
        conversion_params['db_fileformat'] = extraction_params['database_file'].split('.')[1]
        conversion_params['tables'] = asset_type_config['table_name']
        conversion_params['directory_output'] = directories_project["parquetFiles_sppid"]
        conversion_params['output_fileFormat'] = 'parquet'
        conversion_params['stdout_file'] = directories_project['logFilesConversions_sppid']/f"{now_date}__{extraction_params['database_file'].split('.')[0]}_stdout.txt"
        conversion_params['stderr_file'] = directories_project['logFilesConversions_sppid']/f"{now_date}__{extraction_params['database_file'].split('.')[0]}_stderr.txt"
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

    @staticmethod
    def create_s3d_configFile(s3d_template_ini_file, s3d_project_ini_file, s3d_database_outputfile):
        try:
            shutil.copy(s3d_template_ini_file, s3d_project_ini_file)
            with open(s3d_project_ini_file) as f:
                s = f.read().replace('[s3d_database_outputfile]', s3d_database_outputfile)
            with open(s3d_project_ini_file, 'w') as f:
                f.write(s)
        except Exception as e:
            print(e)

    @staticmethod
    def create_sppid_configFile(sppid_template_ini_file, sppid_project_ini_file, sppid_database_outputfile):
        try:
            shutil.copy(sppid_template_ini_file, sppid_project_ini_file)
            # with open(sppid_project_ini_file) as f:
            #     s = f.read().replace('[sppid_database_outputfile]', sppid_database_outputfile)
            # with open(sppid_project_ini_file, 'w') as f:
            #     f.write(s)
        except Exception as e:
            print(e)