import unittest
import subprocess
from pathlib import Path
import pandas as pd
import json

ROOT_DIRECTORY = Path(r'C:\PROJECTS')
APP_DIRECTORY = ROOT_DIRECTORY/'_app'

class ParquetFilesTestCase(unittest.TestCase):

    def setUp(self):
        self.batch_files_path = ROOT_DIRECTORY/'DEV_D_IPP_New'/'Scripts'
        self.app_conversion_path = APP_DIRECTORY/'DatabaseTablesToParquet'
        self.interpreter = APP_DIRECTORY/'DatabaseTablesToParquet/venv/Scripts/python'
        self.app_name = 'app.py'
        self.directory_db = ROOT_DIRECTORY/'DEV_D_IPP_New'/'Output'
        self.db_fileformat = 'db'
        self.output_path = APP_DIRECTORY/'ReportAdapterExtractions'/'tests'/'batch_parquet'
        self.output_fileformat = 'parquet'
        self.app_parquet_path = Path(r'C:\PROJECTS\105185_ipp_intermountain_power\parquetFiles_s3d')


    def test_BoltUp(self):
            batch_file = 'DEV_D_IPP_New_BoltUp.cmd'
            print("running batch file: ", batch_file)
            result_batch_run = subprocess.run([batch_file], cwd=self.batch_files_path, shell=True, capture_output=True, text=True)
            print(result_batch_run.stdout)
            print(result_batch_run.stderr)
            db_name = 'DEV_D_IPP_New_BoltUp'
            parquet_file_app = 'BoltUp__BoltUp.parquet'
            tables = ["BoltUp"]            
            conversion_run_args = []
            conversion_run_args.append(self.interpreter)
            conversion_run_args.append(self.app_name)
            conversion_run_args.append(self.directory_db)
            conversion_run_args.append(db_name)
            conversion_run_args.append(self.db_fileformat)
            conversion_run_args.append(json.dumps(tables))
            conversion_run_args.append(self.output_path)
            conversion_run_args.append(self.output_fileformat)
            parquet_file_batch = self.output_path/f'{db_name}__{tables[0]}.parquet'
            parquet_file_batch.unlink(missing_ok=True)
            result_batch_db_conversion = subprocess.run(conversion_run_args, cwd=self.app_conversion_path, shell=True, capture_output=True, text=True)
            print(result_batch_db_conversion.stdout)
            print(result_batch_db_conversion.stderr)
            df_parquet_batch = pd.read_parquet(parquet_file_batch, engine='pyarrow')
            df_parquet_app = pd.read_parquet(self.app_parquet_path/parquet_file_app, engine='pyarrow')
            df = df_parquet_batch.compare(df_parquet_app)
            self.assertTrue(len(df) == 0)


    def test_Equipment(self):
            batch_file = 'DEV_D_IPP_New_Equipment.cmd'
            print("running batch file: ", batch_file)            
            result_batch_run = subprocess.run([batch_file], cwd=self.batch_files_path, shell=True, capture_output=True, text=True)
            print(result_batch_run.stdout)
            print(result_batch_run.stderr)
            db_name = 'DEV_D_IPP_New_Equipment'
            parquet_file_app = 'Equipment__[Equipment&Furnishing].parquet'
            tables = ["[Equipment&Furnishing]"]            
            conversion_run_args = []
            conversion_run_args.append(self.interpreter)
            conversion_run_args.append(self.app_name)
            conversion_run_args.append(self.directory_db)
            conversion_run_args.append(db_name)
            conversion_run_args.append(self.db_fileformat)
            conversion_run_args.append(json.dumps(tables))
            conversion_run_args.append(self.output_path)
            conversion_run_args.append(self.output_fileformat)
            parquet_file_batch = self.output_path/f'{db_name}__{tables[0]}.parquet'
            parquet_file_batch.unlink(missing_ok=True)
            result_batch_db_conversion = subprocess.run(conversion_run_args, cwd=self.app_conversion_path, shell=True, capture_output=True, text=True)
            print(result_batch_db_conversion.stdout)
            print(result_batch_db_conversion.stderr)
            df_parquet_batch = pd.read_parquet(parquet_file_batch, engine='pyarrow')
            df_parquet_app = pd.read_parquet(self.app_parquet_path/parquet_file_app, engine='pyarrow')
            df = df_parquet_batch.compare(df_parquet_app)
            self.assertTrue(len(df) == 0)


    def test_Pipe(self):
            batch_file = 'DEV_D_IPP_New_Pipe.cmd'
            print("running batch file: ", batch_file)            
            result_batch_run = subprocess.run([batch_file], cwd=self.batch_files_path, shell=True, capture_output=True, text=True)
            print(result_batch_run.stdout)
            print(result_batch_run.stderr)
            db_name = 'DEV_D_IPP_New_Pipe'
            parquet_file_app = 'Pipe__Pipe.parquet'
            tables = ["Pipe"]            
            conversion_run_args = []
            conversion_run_args.append(self.interpreter)
            conversion_run_args.append(self.app_name)
            conversion_run_args.append(self.directory_db)
            conversion_run_args.append(db_name)
            conversion_run_args.append(self.db_fileformat)
            conversion_run_args.append(json.dumps(tables))
            conversion_run_args.append(self.output_path)
            conversion_run_args.append(self.output_fileformat)
            parquet_file_batch = self.output_path/f'{db_name}__{tables[0]}.parquet'
            parquet_file_batch.unlink(missing_ok=True)
            result_batch_db_conversion = subprocess.run(conversion_run_args, cwd=self.app_conversion_path, shell=True, capture_output=True, text=True)
            print(result_batch_db_conversion.stdout)
            print(result_batch_db_conversion.stderr)
            df_parquet_batch = pd.read_parquet(parquet_file_batch, engine='pyarrow')
            df_parquet_app = pd.read_parquet(self.app_parquet_path/parquet_file_app, engine='pyarrow')
            df = df_parquet_batch.compare(df_parquet_app)
            self.assertTrue(len(df) == 0)


    def test_PipeBolt(self):
            batch_file = 'DEV_D_IPP_New_PipeBolt.cmd'
            print("running batch file: ", batch_file)            
            result_batch_run = subprocess.run([batch_file], cwd=self.batch_files_path, shell=True, capture_output=True, text=True)
            print(result_batch_run.stdout)
            print(result_batch_run.stderr)
            db_name = 'DEV_D_IPP_New_PipeBolt'
            parquet_file_app = 'PipeBolt__PipeBolt.parquet'
            tables = ["PipeBolt"]            
            conversion_run_args = []
            conversion_run_args.append(self.interpreter)
            conversion_run_args.append(self.app_name)
            conversion_run_args.append(self.directory_db)
            conversion_run_args.append(db_name)
            conversion_run_args.append(self.db_fileformat)
            conversion_run_args.append(json.dumps(tables))
            conversion_run_args.append(self.output_path)
            conversion_run_args.append(self.output_fileformat)
            parquet_file_batch = self.output_path/f'{db_name}__{tables[0]}.parquet'
            parquet_file_batch.unlink(missing_ok=True)
            result_batch_db_conversion = subprocess.run(conversion_run_args, cwd=self.app_conversion_path, shell=True, capture_output=True, text=True)
            print(result_batch_db_conversion.stdout)
            print(result_batch_db_conversion.stderr)
            df_parquet_batch = pd.read_parquet(parquet_file_batch, engine='pyarrow')
            df_parquet_app = pd.read_parquet(self.app_parquet_path/parquet_file_app, engine='pyarrow')
            df = df_parquet_batch.compare(df_parquet_app)
            self.assertTrue(len(df) == 0)


    def test_PipeGasket(self):
            batch_file = 'DEV_D_IPP_New_PipeGasket.cmd'
            print("running batch file: ", batch_file)            
            result_batch_run = subprocess.run([batch_file], cwd=self.batch_files_path, shell=True, capture_output=True, text=True)
            print(result_batch_run.stdout)
            print(result_batch_run.stderr)
            db_name = 'DEV_D_IPP_New_PipeGasket'
            parquet_file_app = 'PipeGasket__PipeGasket.parquet'
            tables = ["PipeGasket"]            
            conversion_run_args = []
            conversion_run_args.append(self.interpreter)
            conversion_run_args.append(self.app_name)
            conversion_run_args.append(self.directory_db)
            conversion_run_args.append(db_name)
            conversion_run_args.append(self.db_fileformat)
            conversion_run_args.append(json.dumps(tables))
            conversion_run_args.append(self.output_path)
            conversion_run_args.append(self.output_fileformat)
            parquet_file_batch = self.output_path/f'{db_name}__{tables[0]}.parquet'
            parquet_file_batch.unlink(missing_ok=True)
            result_batch_db_conversion = subprocess.run(conversion_run_args, cwd=self.app_conversion_path, shell=True, capture_output=True, text=True)
            print(result_batch_db_conversion.stdout)
            print(result_batch_db_conversion.stderr)
            df_parquet_batch = pd.read_parquet(parquet_file_batch, engine='pyarrow')
            df_parquet_app = pd.read_parquet(self.app_parquet_path/parquet_file_app, engine='pyarrow')
            df = df_parquet_batch.compare(df_parquet_app)
            self.assertTrue(len(df) == 0)


    def test_PipeInstrument(self):
            batch_file = 'DEV_D_IPP_New_PipeInstrument.cmd'
            print("running batch file: ", batch_file)            
            result_batch_run = subprocess.run([batch_file], cwd=self.batch_files_path, shell=True, capture_output=True, text=True)
            print(result_batch_run.stdout)
            print(result_batch_run.stderr)
            db_name = 'DEV_D_IPP_New_PipeInstrument'
            parquet_file_app = 'PipeInstrument__PipeInstruments.parquet'
            tables = ["PipeInstruments"]            
            conversion_run_args = []
            conversion_run_args.append(self.interpreter)
            conversion_run_args.append(self.app_name)
            conversion_run_args.append(self.directory_db)
            conversion_run_args.append(db_name)
            conversion_run_args.append(self.db_fileformat)
            conversion_run_args.append(json.dumps(tables))
            conversion_run_args.append(self.output_path)
            conversion_run_args.append(self.output_fileformat)
            parquet_file_batch = self.output_path/f'{db_name}__{tables[0]}.parquet'
            parquet_file_batch.unlink(missing_ok=True)
            result_batch_db_conversion = subprocess.run(conversion_run_args, cwd=self.app_conversion_path, shell=True, capture_output=True, text=True)
            print(result_batch_db_conversion.stdout)
            print(result_batch_db_conversion.stderr)
            df_parquet_batch = pd.read_parquet(parquet_file_batch, engine='pyarrow')
            df_parquet_app = pd.read_parquet(self.app_parquet_path/parquet_file_app, engine='pyarrow')
            df = df_parquet_batch.compare(df_parquet_app)
            self.assertTrue(len(df) == 0)


    def test_Pipeline(self):
            batch_file = 'DEV_D_IPP_New_Pipeline.cmd'
            print("running batch file: ", batch_file)            
            result_batch_run = subprocess.run([batch_file], cwd=self.batch_files_path, shell=True, capture_output=True, text=True)
            print(result_batch_run.stdout)
            print(result_batch_run.stderr)
            db_name = 'DEV_D_IPP_New_Pipeline'
            parquet_file_app = 'Pipeline__Pipeline.parquet'
            tables = ["Pipeline"]            
            conversion_run_args = []
            conversion_run_args.append(self.interpreter)
            conversion_run_args.append(self.app_name)
            conversion_run_args.append(self.directory_db)
            conversion_run_args.append(db_name)
            conversion_run_args.append(self.db_fileformat)
            conversion_run_args.append(json.dumps(tables))
            conversion_run_args.append(self.output_path)
            conversion_run_args.append(self.output_fileformat)
            parquet_file_batch = self.output_path/f'{db_name}__{tables[0]}.parquet'
            parquet_file_batch.unlink(missing_ok=True)
            result_batch_db_conversion = subprocess.run(conversion_run_args, cwd=self.app_conversion_path, shell=True, capture_output=True, text=True)
            print(result_batch_db_conversion.stdout)
            print(result_batch_db_conversion.stderr)
            df_parquet_batch = pd.read_parquet(parquet_file_batch, engine='pyarrow')
            df_parquet_app = pd.read_parquet(self.app_parquet_path/parquet_file_app, engine='pyarrow')
            df = df_parquet_batch.compare(df_parquet_app)
            self.assertTrue(len(df) == 0)


    def test_PipePart(self):
            batch_file = 'DEV_D_IPP_New_PipePart.cmd'
            print("running batch file: ", batch_file)            
            result_batch_run = subprocess.run([batch_file], cwd=self.batch_files_path, shell=True, capture_output=True, text=True)
            print(result_batch_run.stdout)
            print(result_batch_run.stderr)
            db_name = 'DEV_D_IPP_New_PipePart'
            parquet_file_app = 'PipePart__PipePart.parquet'
            tables = ["PipePart"]            
            conversion_run_args = []
            conversion_run_args.append(self.interpreter)
            conversion_run_args.append(self.app_name)
            conversion_run_args.append(self.directory_db)
            conversion_run_args.append(db_name)
            conversion_run_args.append(self.db_fileformat)
            conversion_run_args.append(json.dumps(tables))
            conversion_run_args.append(self.output_path)
            conversion_run_args.append(self.output_fileformat)
            parquet_file_batch = self.output_path/f'{db_name}__{tables[0]}.parquet'
            parquet_file_batch.unlink(missing_ok=True)
            result_batch_db_conversion = subprocess.run(conversion_run_args, cwd=self.app_conversion_path, shell=True, capture_output=True, text=True)
            print(result_batch_db_conversion.stdout)
            print(result_batch_db_conversion.stderr)
            df_parquet_batch = pd.read_parquet(parquet_file_batch, engine='pyarrow')
            df_parquet_app = pd.read_parquet(self.app_parquet_path/parquet_file_app, engine='pyarrow')
            df = df_parquet_batch.compare(df_parquet_app)
            self.assertTrue(len(df) == 0)


    def test_Piperun(self):
            batch_file = 'DEV_D_IPP_New_Piperun.cmd'
            print("running batch file: ", batch_file)            
            result_batch_run = subprocess.run([batch_file], cwd=self.batch_files_path, shell=True, capture_output=True, text=True)
            print(result_batch_run.stdout)
            print(result_batch_run.stderr)
            db_name = 'DEV_D_IPP_New_Piperun'
            parquet_file_app = 'Piperun__Piperun.parquet'
            tables = ["Piperun"]            
            conversion_run_args = []
            conversion_run_args.append(self.interpreter)
            conversion_run_args.append(self.app_name)
            conversion_run_args.append(self.directory_db)
            conversion_run_args.append(db_name)
            conversion_run_args.append(self.db_fileformat)
            conversion_run_args.append(json.dumps(tables))
            conversion_run_args.append(self.output_path)
            conversion_run_args.append(self.output_fileformat)
            parquet_file_batch = self.output_path/f'{db_name}__{tables[0]}.parquet'
            parquet_file_batch.unlink(missing_ok=True)
            result_batch_db_conversion = subprocess.run(conversion_run_args, cwd=self.app_conversion_path, shell=True, capture_output=True, text=True)
            print(result_batch_db_conversion.stdout)
            print(result_batch_db_conversion.stderr)
            df_parquet_batch = pd.read_parquet(parquet_file_batch, engine='pyarrow')
            df_parquet_app = pd.read_parquet(self.app_parquet_path/parquet_file_app, engine='pyarrow')
            df = df_parquet_batch.compare(df_parquet_app)
            self.assertTrue(len(df) == 0)


    def test_PipeSPItems(self):
            batch_file = 'DEV_D_IPP_New_PipeSPItems.cmd'
            print("running batch file: ", batch_file)            
            result_batch_run = subprocess.run([batch_file], cwd=self.batch_files_path, shell=True, capture_output=True, text=True)
            print(result_batch_run.stdout)
            print(result_batch_run.stderr)
            db_name = 'DEV_D_IPP_New_PipeSPItems'
            parquet_file_app = 'PipeSPItems__PipeSPItems.parquet'
            tables = ["PipeSPItems"]            
            conversion_run_args = []
            conversion_run_args.append(self.interpreter)
            conversion_run_args.append(self.app_name)
            conversion_run_args.append(self.directory_db)
            conversion_run_args.append(db_name)
            conversion_run_args.append(self.db_fileformat)
            conversion_run_args.append(json.dumps(tables))
            conversion_run_args.append(self.output_path)
            conversion_run_args.append(self.output_fileformat)
            parquet_file_batch = self.output_path/f'{db_name}__{tables[0]}.parquet'
            parquet_file_batch.unlink(missing_ok=True)
            result_batch_db_conversion = subprocess.run(conversion_run_args, cwd=self.app_conversion_path, shell=True, capture_output=True, text=True)
            print(result_batch_db_conversion.stdout)
            print(result_batch_db_conversion.stderr)
            df_parquet_batch = pd.read_parquet(parquet_file_batch, engine='pyarrow')
            df_parquet_app = pd.read_parquet(self.app_parquet_path/parquet_file_app, engine='pyarrow')
            df = df_parquet_batch.compare(df_parquet_app)
            self.assertTrue(len(df) == 0)


    def test_PipeSpool(self):
            batch_file = 'DEV_D_IPP_New_PipeSpool.cmd'
            print("running batch file: ", batch_file)            
            result_batch_run = subprocess.run([batch_file], cwd=self.batch_files_path, shell=True, capture_output=True, text=True)
            print(result_batch_run.stdout)
            print(result_batch_run.stderr)
            db_name = 'DEV_D_IPP_New_PipeSpool'
            parquet_file_app = 'PipeSpool__PipeSpool.parquet'
            tables = ["PipeSpool"]            
            conversion_run_args = []
            conversion_run_args.append(self.interpreter)
            conversion_run_args.append(self.app_name)
            conversion_run_args.append(self.directory_db)
            conversion_run_args.append(db_name)
            conversion_run_args.append(self.db_fileformat)
            conversion_run_args.append(json.dumps(tables))
            conversion_run_args.append(self.output_path)
            conversion_run_args.append(self.output_fileformat)
            parquet_file_batch = self.output_path/f'{db_name}__{tables[0]}.parquet'
            parquet_file_batch.unlink(missing_ok=True)
            result_batch_db_conversion = subprocess.run(conversion_run_args, cwd=self.app_conversion_path, shell=True, capture_output=True, text=True)
            print(result_batch_db_conversion.stdout)
            print(result_batch_db_conversion.stderr)
            df_parquet_batch = pd.read_parquet(parquet_file_batch, engine='pyarrow')
            df_parquet_app = pd.read_parquet(self.app_parquet_path/parquet_file_app, engine='pyarrow')
            df = df_parquet_batch.compare(df_parquet_app)
            self.assertTrue(len(df) == 0)


    def test_PipeWeld(self):
            batch_file = 'DEV_D_IPP_New_PipeWeld.cmd'
            print("running batch file: ", batch_file)            
            result_batch_run = subprocess.run([batch_file], cwd=self.batch_files_path, shell=True, capture_output=True, text=True)
            print(result_batch_run.stdout)
            print(result_batch_run.stderr)
            db_name = 'DEV_D_IPP_New_PipeWeld'
            parquet_file_app = 'PipeWeld__PipeWeld.parquet'
            tables = ["PipeWeld"]            
            conversion_run_args = []
            conversion_run_args.append(self.interpreter)
            conversion_run_args.append(self.app_name)
            conversion_run_args.append(self.directory_db)
            conversion_run_args.append(db_name)
            conversion_run_args.append(self.db_fileformat)
            conversion_run_args.append(json.dumps(tables))
            conversion_run_args.append(self.output_path)
            conversion_run_args.append(self.output_fileformat)
            parquet_file_batch = self.output_path/f'{db_name}__{tables[0]}.parquet'
            parquet_file_batch.unlink(missing_ok=True)
            result_batch_db_conversion = subprocess.run(conversion_run_args, cwd=self.app_conversion_path, shell=True, capture_output=True, text=True)
            print(result_batch_db_conversion.stdout)
            print(result_batch_db_conversion.stderr)
            df_parquet_batch = pd.read_parquet(parquet_file_batch, engine='pyarrow')
            df_parquet_app = pd.read_parquet(self.app_parquet_path/parquet_file_app, engine='pyarrow')
            df = df_parquet_batch.compare(df_parquet_app)
            self.assertTrue(len(df) == 0)


    def test_Support(self):
            batch_file = 'DEV_D_IPP_New_Support.cmd'
            print("running batch file: ", batch_file)            
            result_batch_run = subprocess.run([batch_file], cwd=self.batch_files_path, shell=True, capture_output=True, text=True)
            print(result_batch_run.stdout)
            print(result_batch_run.stderr)
            db_name = 'DEV_D_IPP_New_Support'
            parquet_file_app = 'Support__SupportAssembly.parquet'
            tables = ["SupportAssembly"]            
            conversion_run_args = []
            conversion_run_args.append(self.interpreter)
            conversion_run_args.append(self.app_name)
            conversion_run_args.append(self.directory_db)
            conversion_run_args.append(db_name)
            conversion_run_args.append(self.db_fileformat)
            conversion_run_args.append(json.dumps(tables))
            conversion_run_args.append(self.output_path)
            conversion_run_args.append(self.output_fileformat)
            parquet_file_batch = self.output_path/f'{db_name}__{tables[0]}.parquet'
            parquet_file_batch.unlink(missing_ok=True)
            result_batch_db_conversion = subprocess.run(conversion_run_args, cwd=self.app_conversion_path, shell=True, capture_output=True, text=True)
            print(result_batch_db_conversion.stdout)
            print(result_batch_db_conversion.stderr)
            df_parquet_batch = pd.read_parquet(parquet_file_batch, engine='pyarrow')
            df_parquet_app = pd.read_parquet(self.app_parquet_path/parquet_file_app, engine='pyarrow')
            df = df_parquet_batch.compare(df_parquet_app)
            self.assertTrue(len(df) == 0)


    def test_SupportComponent(self):
            batch_file = 'DEV_D_IPP_New_SupportComponent.cmd'
            print("running batch file: ", batch_file)            
            result_batch_run = subprocess.run([batch_file], cwd=self.batch_files_path, shell=True, capture_output=True, text=True)
            print(result_batch_run.stdout)
            print(result_batch_run.stderr)
            db_name = 'DEV_D_IPP_New_SupportComponent'
            parquet_file_app = 'SupportComponent__SupportComponent.parquet'
            tables = ["SupportComponent"]            
            conversion_run_args = []
            conversion_run_args.append(self.interpreter)
            conversion_run_args.append(self.app_name)
            conversion_run_args.append(self.directory_db)
            conversion_run_args.append(db_name)
            conversion_run_args.append(self.db_fileformat)
            conversion_run_args.append(json.dumps(tables))
            conversion_run_args.append(self.output_path)
            conversion_run_args.append(self.output_fileformat)
            parquet_file_batch = self.output_path/f'{db_name}__{tables[0]}.parquet'
            parquet_file_batch.unlink(missing_ok=True)
            result_batch_db_conversion = subprocess.run(conversion_run_args, cwd=self.app_conversion_path, shell=True, capture_output=True, text=True)
            print(result_batch_db_conversion.stdout)
            print(result_batch_db_conversion.stderr)
            df_parquet_batch = pd.read_parquet(parquet_file_batch, engine='pyarrow')
            df_parquet_app = pd.read_parquet(self.app_parquet_path/parquet_file_app, engine='pyarrow')
            df = df_parquet_batch.compare(df_parquet_app)
            self.assertTrue(len(df) == 0)