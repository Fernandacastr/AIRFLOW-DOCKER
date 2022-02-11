from kaggle.api.kaggle_api_extended import KaggleApi
from zipfile import ZipFile

def importando_zip():
    api = KaggleApi()
    api.authenticate()
    api.dataset_download_file('ealaxi/banksim1',
                              file_name='bs140513_032310.csv')
    print('Arquivo importado')

    test_file_name = "bs140513_032310.csv.zip"
    with ZipFile(test_file_name, 'r') as zip:
        zip.printdir()
        zip.extractall()
        print('Arquivo Descompactado')

