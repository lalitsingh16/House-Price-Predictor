from abc import ABC, abstractmethod
import zipfile
import os
import pandas as pd

# Defining abstract class for data Ingestor
class DataIngestor(ABC):
    @abstractmethod
    def ingest(self,file_path:str) -> pd.DataFrame:
        pass
# Implement a ZIPIngestion class
class ZipDataIngestor(DataIngestor):
    def ingest(self,file_path:str) -> pd.DataFrame:
        # ensure the file is a .zip file
        if not file_path.endswith('.zip'):
            raise ValueError("The file ingested is not a zip file")

        # extract the zip file
        with zipfile.ZipFile(file_path,'r') as z:
            z.extractall("extracted_data")

        # Find the extracted CSV file
        extracted_files = os.listdir("extracted_data")
        csv_files = [f for f in extracted_files if f.endswith('.csv')]

        if len(csv_files) == 0: raise ValueError("No csv files found in extracted_data")
        if len(csv_files)  > 1: raise ValueError("Multiple csv files found in extracted_data, Please choose one file")

        # Read the CSV into Dataframe
        csv_file_path = os.path.join(os.path.abspath("extracted_data"), csv_files[0])
        df = pd.read_csv(csv_file_path)

        return df

# Implement a Factory to create DataIngestors
class DataIngestorfactory:
    @staticmethod
    def get_data_ingestor(file_extension:str) -> DataIngestor:
        """Returns the appropriate DataIngestor based on the file extension"""
        if file_extension == '.zip':
            return ZipDataIngestor()
        else:
            raise ValueError("File extension must be .zip")

#Example:
if __name__ == '__main__':
    # File path
    file_path = r"C:\Users\Lalit\PycharmProjects\House_Price_Predictor-End-to-End_ML-Project\data\archive.zip"

    # file extension
    file_extension = os.path.splitext(file_path)[1]

    # GET the approapriate dataIngestor
    data_ingestor = DataIngestorfactory.get_data_ingestor(file_extension)

    # Ingest the data and load it into DF
    df = data_ingestor.ingest(file_path)

    print(df)