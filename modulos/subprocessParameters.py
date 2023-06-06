
class ProjectParameters():

    def __init__(self, project_dict:dict, report_adapter_config_dict:dict):
        self.project_dict = project_dict
        self.report_adapter_config_dict = report_adapter_config_dict

    def getParameters(self):
        project_report_adapter_dict = {}
        for project in self.project_dict:
            project_attributes = self.project_dict[project]
            project_report_adapter_config_dict = self.report_adapter_config_dict.copy()
            if 's3d_reporter_adapter_asset_type_blacklist' in project_attributes.keys():
                for asset_type in project_attributes['s3d_reporter_adapter_asset_type_blacklist']:
                    if asset_type in project_report_adapter_config_dict:
                        project_report_adapter_config_dict.pop(asset_type)
                if len(project_report_adapter_config_dict) > 0:
                    project_report_adapter_dict[project] = project_report_adapter_config_dict
        return project_report_adapter_dict            