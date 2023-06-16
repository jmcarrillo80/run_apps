import os


class ProjectParameters():

    def __init__(self, project_dict:dict, report_adapter_config_dict:dict):
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
    def createDirectory(directories:dict):
        subprocess_directories = {}
        root = directories['root']
        if not os.path.exists(root):
            os.makedirs(root)
        for subdirectory in directories['subdirectories']:
            if not os.path.exists(root/subdirectory):
                os.makedirs(root/subdirectory)
            subprocess_directories[subdirectory] = root/subdirectory
        return subprocess_directories
    