import csv
import logging
import time
import pandas
from datetime import datetime

import configkeys
from helpers import date_formatter
from helpers import parser_portos as pp
from helpers import set_dir_structure as ss
from helpers import handle_pandas
from helpers.scrapping_methods import Scrapping
from helpers.mysql_handler_rio_de_janeiro import dbHandler

# Set log


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
# create file handler and ser level to debug
fh = logging.FileHandler('log/scrapping_rio_de_janeiro.log')
fh.setLevel(logging.DEBUG)
# create formatter
formatter = logging.Formatter("%(asctime)s;%(levelname)s;%(message)s", "%Y-%m-%d %H:%M:%S")
fh.setFormatter(formatter)
# add ch to logger
logger.addHandler(fh)


class RioJaneiro(Scrapping):

    def __init__(self, url):

        super().__init__(url)
        self.guanabara_pd = pandas.DataFrame()
        self.sepetiba_angra_pd = pandas.DataFrame()
        self.acu_pd = pandas.DataFrame()
        self.forno_pd = pandas.DataFrame()

    def html_to_table(self):
        """
        Trada o html apropriadamente e salva na base de dados
        1. Guanabara
        2. Sepetiba e Angra
        3. Açu
        """
        if self.status_HTTP_request == 200:
            # Se a conexao e obtencao do html ocorreu com sucesso
            try:
                self.lista_publicacao_portos = pp.lista_portos(self.soup)

            except:
                pass

            for nome_porto in self.lista_publicacao_portos.keys():
                print(nome_porto)
                if "GUANABARA" in nome_porto:
                    self.guanabara_pd = self.lista_publicacao_portos[nome_porto]

                if "FORNO" in nome_porto:
                    self.forno_pd = self.lista_publicacao_portos[nome_porto]

                if "SEPETIBA" in nome_porto:
                    self.sepetiba_angra_pd = self.lista_publicacao_portos[nome_porto]

                if "AÇÚ" in nome_porto:
                    self.acu_pd = self.lista_publicacao_portos[nome_porto]


    def to_csv(self):
        """
        Obtem o csv dos do(s) porto(s) disponiveis na URL
            - Resolve o caso de nao ter o html atualizado na execucao
            - Reporta qualquer problema de execução via email

        Transforma html para csv
        :return:
        """

        # Define o header dos dados salvos no formato csv
        header_out = ['POB', 'NAVIO', 'CALADO', 'LOA', 'BOCA', 'GT', 'DWT', 'MANOBRA', 'DE', 'PARA', 'BRD',
                      'nome_porto', 'data_abertura', 'navio_info_TIPO DE NAVIO', 'navio_info_BANDEIRA',
                      'navio_info_NOME', 'navio_info_IMO', 'navio_info_PREFIXO', 'navio_info_MMSI']

        if self.status_HTTP_request is None:
            # atualizar as informacoes do html
            self.get_html_urllib()

        if self.status_HTTP_request == 200:
            # Tendo obtido sucesso na requisicao, inicia a estruturacao da informacao 
            self.html_to_table()

            # Pasta de saida do arquivo
            datetime_str = date_formatter.datetime_to_yyyymmdd_hhmm(self.datetime_extracao.isoformat())
            dir_out = "./dados_processados/" + datetime_str

            # Lista os arquivos que foram parseados
            lista_dataframes_to_csv = [self.acu_pd, self.guanabara_pd, self.sepetiba_angra_pd, self.forno_pd]
            nomes_dataframes_to_csv = ["Acu", "Guanabara", "Sepetiba_Angra", "Forno"]

            for dataframe, name in zip(lista_dataframes_to_csv, nomes_dataframes_to_csv):

                #print(name)
                if dataframe.empty:
                    logger.info("to_csv() - As informacoes referentes ao porto de {} estao vazias! - Pode ser que o site nao tenha informacoes referentes a esse porto ou o site mudou".format(name))

                else:
                    # Sabendo que o dataframe nao esta vazio, produzir o arquivo
                    try:
                        dataframe.to_csv("{}_{}.csv".format(dir_out, name), sep=";", encoding='latin-1',
                                             doublequote=True, quotechar='"', quoting=csv.QUOTE_ALL, columns=header_out)

                        logger.info("to_csv() - Arquivo produzido: {}_{}.csv".format(dir_out, name))

                    except:
                        logger.info("to_csv() - Falha ao produzir o arquivo: {}_{}.csv".format(dir_out, name))
                        loggier.debug("erro:")

        if self.status_HTTP_request >= 400:
            logger.info("to_csv() - Nao foi possivel obter as informacoes do site! problemas com a requisicao: {}".format(self.status_HTTP_request))

    def to_mysql(self):
        """
        Envia para o SQL as infomacoes obtidas
        Insere os dados em suas tabelsa especificas:
         - praticagem_previsao_guanabara
         - praticagem_previsao_sepetiba_angra
         - praticagem_previsao_acu

        :return:
        """

        if self.status_HTTP_request is None:
            # atualizar as informacoes do html
            self.get_html_urllib()

            # feita arequisicao para o site, chama to_mysql()
            #self.to_mysql()

        if self.status_HTTP_request == 200:
            # Tendo obtido sucesso na requisicao, inicia a estruturacao da informacao 
            #print("Funfou HTTP status")
            self.html_to_table()

            # Lista os arquivos que foram parseados
            lista_dataframes_to_csv = [self.acu_pd, self.guanabara_pd, self.sepetiba_angra_pd, self.forno_pd]
            nomes_dataframes_to_csv = ["Acu", "Guanabara", "Sepetiba_Angra", "Forno"]
            sql_table_names = ["praticagem_programado_acu", "praticagem_programado_guanabara", "praticagem_programado_sepetiba_angra", "praticagem_programado_forno"]

            # Define parametros de entrada
            # Header
            header_out_pandas = ['POB', 'NAVIO', 'CALADO', 'LOA', 'BOCA', 'GT', 'DWT', 'MANOBRA', 'DE', 'PARA', 'BRD', 
                            'nome_porto', 'navio_info_TIPO DE NAVIO', 'navio_info_PREFIXO', 
                            'navio_info_MMSI', 'navio_info_IMO', 'navio_info_BANDEIRA']

            # Chaves de acesso - Devem ficar guardadas no arquivo configkeys
            keys = configkeys.mysql_keys
            praticagem_db = dbHandler(host=keys["host"], database=keys["database"],
                                      user=keys["login"], password=keys["senha"])

            for dataframe, nome, sql_table_name in zip(lista_dataframes_to_csv, nomes_dataframes_to_csv, sql_table_names):

                #print(nome, sql_table_names)
                # Verifica se o dataframe nao esta vazio
                if dataframe.empty:
                    logger.info("to_mysql() - As informacoes referentes ao porto de {} estao vazias! - Pode ser que o site nao tenha informacoes referentes a esse porto ou o site mudou".format(nome))

                else:
                    logger.info("to_mysql() - Iniciado o processo para insercao dos dados do porto: {}".format(nome))
                    #print(nome, sql_table_name)
                    # Obtem as caracteristicas da tabela de atracacoes
                    sql_table_header = praticagem_db.get_header(sql_table_name)[1:]
                    sql_table_type = praticagem_db.get_columns_type(sql_table_name)[1:]
                    header_pandas_to_sql = dict(zip(header_out_pandas, sql_table_header))

                    # Inicia tratamento dos dados para fazer o upload - retira duplicados
                    distinct_pd = dataframe[header_out_pandas].drop_duplicates()
                    distinct_pd = distinct_pd.rename(columns=header_pandas_to_sql)

                    # Update informacoes de data com
                    distinct_pd["data_procedimento"] = distinct_pd.apply(lambda row: date_formatter.set_year_movimentacao(row["data_procedimento"]), axis=1)
                    distinct_pd = handle_pandas.format_praticagem_programado(distinct_pd)
                    #print(distinct_pd)
                    # Obtem somente o diferencial das informacoes entre o scrap atual e os dados que temos na base de dados
                    historico_pd = praticagem_db.get_select_top_100(sql_table_name)
                    historico_pd = historico_pd[list(sql_table_header)].drop_duplicates()
                    
                    # Inicia o processo de comparacao entre os dados correntes do site (distinct_pd) e os da base de dados (historico_pd)
                    # 1. Colocar ambos os dados no mesmo formato
                    historico_pd_chunk = []
                    for row in [tuple(x) for x in historico_pd.values]:    
                        historico_pd_chunk.append(tuple([str(x).replace('\xa0', 'None') for x in row]))

                    distinct_pd_chunk = []
                    for row in [tuple(x) for x in distinct_pd.values]:    
                        distinct_pd_chunk.append(tuple([str(x).replace('\xa0', 'None') for x in row]))

                    # Obtem o diferencial considerando somente as informacoes do site
                    pd_insert_target = set(distinct_pd_chunk)-set(historico_pd_chunk)

                    # Formata os dados para mandar para o SQL
                    chunk_pd_final = praticagem_db.chunk_to_data_type_filter(pd_insert_target, sql_table_type)
                    
                    # Insere os dados
                    if chunk_pd_final == []:
                        logger.info("to_mysql() - {}: Dados nao inseridos; Somente dados repetidos".format(nome))
                    else:
                        logger.info("to_mysql() - {}: foram inseridos {} linhas".format(nome, len(chunk_pd_final)))
                        praticagem_db.insert_chunk(sql_table_name, sql_table_header, chunk_pd_final)


if __name__ == "__main__":
    ss.make_dir()
    RJ = RioJaneiro(url="http://www.praticagem-rj.com.br/")
    RJ.to_mysql()
    RJ.to_csv()
