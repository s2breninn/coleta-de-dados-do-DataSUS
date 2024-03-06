import urllib.request
from tqdm import tqdm # lib mostra a progressão do download
from multiprocessing import Pool
import sys


# Importando função feita na pasta /lib/
# Função que gera um range de datas a partir de uma data inicial até a final setada pelo usuário
sys.path.insert(1, '../../lib/')
import dttools

# Função passando endereço url fdp e salvando os arquivos em um diretorio setado
def get_data_uf_ano_mes(uf, ano, mes):
    url = f'ftp://ftp.datasus.gov.br/dissemin/publicos/SIHSUS/200801_/Dados/RD{uf}{ano}{mes}.dbc'

    file = f'/home/scorpion777/Documentos/language-programmer/projetosreais/dataSUS/src/raw/datasus/rd/dbc/RD{uf}{ano}{mes}.dbc'

    resp = urllib.request.urlretrieve(url, file)

# Função para padronização de datas para salvar arquivos de forma dinâmica a partir das datas(mes, ano) e uf
def get_data_uf(uf, datas):
    for i in tqdm(datas):
        ano, mes, dia = i.split('-')
        ano = ano[-2:]
        get_data_uf_ano_mes(uf, ano, mes)

ufs = [
    'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MT', 'MS', 
    'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 
    'SP', 'SE', 'TO'
]

dt_start = '2022-01-01'
dt_stop = '2023-05-01'

datas = dttools.date_range(dt_start, dt_stop, monthly=True)

print(datas)

#to_downloads = [(uf, datas) for uf in ufs]

with Pool(8) as pool:
    pool.starmap(get_data_uf, to_downloads)