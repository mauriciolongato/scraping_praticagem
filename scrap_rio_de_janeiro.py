import csv
import logging
import time
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
        self.guanabara_pd = None
        self.sepetiba_angra_pd = None
        self.acu_pd = None

    def html_to_table(self):
        """
        Trada o html apropriadamente e salva na base de dados
        1. Guanabara
        2. Sepetiba e Angra
        3. Açu
        """

        try:
            self.guanabara_pd = pp.guanabara(self.soup)
        except Exception:
            logger.info("Tivemos problemas para parsear os dados do porto: Bahia de Guanabara")
            logger.exception("message")

        try:
            self.sepetiba_angra_pd = pp.sepetiba_angra(self.soup)
        except Exception:
            logger.info("Tivemos problemas para parsear os dados do porto: Sepetiba")
            logger.exception("message")

        try:
            self.acu_pd = pp.acu(self.soup)
        except Exception:
            logger.info("Tivemos problemas para parsear os dados do porto: Acu")
            logger.exception("message")

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

        if self.html is None:
            # atualizar as informacoes do html
            self.get_html_urllib()

            # Cria um timeout para get_html_urllib
            delta = datetime.utcnow() - self.datetime_extracao
            if delta.total_seconds() < 20:
                # Espere x segundos para fazer a proxima requisicao

                time.sleep(5)
                self.to_csv()
            else:
                logger.info("Timeout para obter o html do site")

            self.to_csv()

        else:
            # Caso o DataFrames ainda nao foram criados
            if self.acu_pd is None or self.guanabara_pd is None or self.sepetiba_angra_pd is None:
                # Inicia a contagem de tempo que estamos dispostos a ficar tentando obter html
                self.html_to_table()
                self.to_csv()

            else:
                delta = datetime.utcnow() - self.datetime_extracao
                if delta.total_seconds() < 40:
                    datetime_str = date_formatter.datetime_to_yyyymmdd_hhmm(self.datetime_extracao.isoformat())
                    dir_out = "./dados_processados/" + datetime_str

                    self.guanabara_pd.to_csv(dir_out + "_Guanabara.csv", sep=";", encoding='latin-1',
                                             doublequote=True, quotechar='"', quoting=csv.QUOTE_ALL, columns=header_out)

                    self.sepetiba_angra_pd.to_csv(dir_out + "_Sepetiba_Angra.csv", sep=";", encoding='latin-1',
                                                  doublequote=True, quotechar='"', quoting=csv.QUOTE_ALL,
                                                  columns=header_out)

                    self.acu_pd.to_csv(dir_out + "_Acu.csv", sep=";", encoding='latin-1', doublequote=True,
                                       quotechar='"', quoting=csv.QUOTE_ALL, columns=header_out)

                    logger.info("Arquivos produzidos: {}_Guanabara.csv, {}_Sepetiba_Angra.csv, {}_Acu".format(dir_out,
                                                                                                              dir_out,
                                                                                                              dir_out))
                else:
                    logger.info("Timeout para criar os DataFrames")

    def to_mysql(self):
        """
        Envia para o SQL as infomacoes obtidas
        Insere os dados em suas tabelsa especificas:
         - praticagem_previsao_guanabara
         - praticagem_previsao_sepetiba_angra
         - praticagem_previsao_acu

        :return:
        """
        # Chama o metodo para obtencao dos dados - chamamos o to_csv pois ele garante que teremos a base em csv tambem
        self.to_csv()

        # Define parametros de entrada
        # Header
        header_out_pandas = ['POB', 'NAVIO', 'CALADO', 'LOA', 'BOCA', 'GT', 'DWT', 'MANOBRA', 'DE', 'PARA', 'BRD', 
                        'nome_porto', 'navio_info_TIPO DE NAVIO', 'navio_info_PREFIXO', 
                        'navio_info_MMSI', 'navio_info_IMO', 'navio_info_BANDEIRA']

        # Chaves de acesso - Devem ficar guardadas no arquivo configkeys
        keys = configkeys.mysql_keys
        praticagem_db = dbHandler(host=keys["host"], database=keys["database"],
                                  user=keys["login"], password=keys["senha"])

        # Acu
        # ---
        sql_table_name = "praticagem_programado_acu"
        # Obtem as caracteristicas da tabela de atracacoes
        sql_table_header = praticagem_db.get_header(sql_table_name)[1:]
        sql_table_type = praticagem_db.get_columns_type(sql_table_name)[1:]
        header_pandas_to_sql = dict(zip(header_out_pandas, sql_table_header))

        # Inicia tratamento dos dados para fazer o upload - retira duplicados
        distinct_acu = self.acu_pd[header_out_pandas].drop_duplicates()
        distinct_acu = distinct_acu.rename(columns=header_pandas_to_sql)

        # Update informacoes de data com
        distinct_acu["data_procedimento"] = distinct_acu.apply(lambda row: date_formatter.set_year_movimentacao(row["data_procedimento"]), axis=1)
        distinct_acu = handle_pandas.format_praticagem_programado(distinct_acu)

        # Obtem somente o diferencial das informacoes entre o scrap atual e os dados que temos na base de dados
        historico_acu = praticagem_db.get_select_top_100(sql_table_name)
        historico_acu = historico_acu[list(sql_table_header)].drop_duplicates()
        acu_insert_target = handle_pandas.get_diff(historico_acu, distinct_acu)

        # Cria os chunks dos dados
        acu_chunk = [tuple(x) for x in acu_insert_target.values]
        acu_chunk_aux = []
        for row in acu_chunk:    
            acu_chunk_aux.append(tuple([str(x) for x in row]))

        # Trata chunk de dados
        chunk_acu_final = praticagem_db.chunk_to_data_type_filter(acu_chunk_aux, sql_table_type)

        # Insere os dados
        if chunk_acu_final == []:
            logger.info("Acu: Dados nao inseridos; Somente dados repetidos")
        else:
            praticagem_db.insert_chunk(sql_table_name, sql_table_header, chunk_acu_final)


        # Guanabara
        # ---------
        sql_table_name = "praticagem_programado_guanabara"
        # Obtem as caracteristicas da tabela de atracacoes
        sql_table_header = praticagem_db.get_header(sql_table_name)[1:]
        sql_table_type = praticagem_db.get_columns_type(sql_table_name)[1:]
        header_pandas_to_sql = dict(zip(header_out_pandas, sql_table_header))        
        
        # Inicia tratamento dos dados para fazer o upload - retira duplicados
        distinct_guanabara = self.guanabara_pd[header_out_pandas].drop_duplicates()
        distinct_guanabara = distinct_guanabara.rename(columns=header_pandas_to_sql)

        # Update informacoes de data
        distinct_guanabara["data_procedimento"] = distinct_guanabara.apply(lambda row: date_formatter.set_year_movimentacao(row["data_procedimento"]), axis=1)
        distinct_guanabara = handle_pandas.format_praticagem_programado(distinct_guanabara)

        # Obtem somente o diferencial das informacoes entre o scrap atual e os dados que temos na base de dados
        historico_guanabara = praticagem_db.get_select_top_100(sql_table_name)
        historico_guanabara = historico_guanabara[list(sql_table_header)].drop_duplicates()
        guanabara_insert_target = handle_pandas.get_diff(historico_guanabara, distinct_guanabara)

        # Cria os chunks dos dados
        guanabara_chunk = [tuple(x) for x in guanabara_insert_target.values]
        guanabara_chunk_aux = []
        for row in guanabara_chunk:    
            guanabara_chunk_aux.append(tuple([str(x) for x in row]))

        # Trata chunk de dados
        # OBS: O site nao fornece o ano da atracacao, sendo assim, adotamos o valor descrito na funcao helpers/date_formater/set_year_movimentacao
        #   isso pode causar desencontros na virada do ano
        chunk_guanabara_final = praticagem_db.chunk_to_data_type_filter(guanabara_chunk_aux, sql_table_type)
        #for row in chunk_guanabara_final:
        #    print(row)

        # Obtem o diferencial das informacoes ja existentes ja base de dados e o apresentado no site
        # Insere os dados
        if chunk_guanabara_final == []:
            logger.info("Guanabara: Dados nao inseridos; Somente dados repetidos")
        else:
            praticagem_db.insert_chunk(sql_table_name, sql_table_header, chunk_guanabara_final)


        # Sepetiba e angra
        # ----------------
        # nome da tabela que vai receber os dados
        sql_table_name = "praticagem_programado_sepetiba_angra"
        # Obtem as caracteristicas da tabela de atracacoes
        sql_table_header = praticagem_db.get_header(sql_table_name)[1:]
        sql_table_type = praticagem_db.get_columns_type(sql_table_name)[1:]
        header_pandas_to_sql = dict(zip(header_out_pandas, sql_table_header))        
        
        # Inicia tratamento dos dados para fazer o upload - retira duplicados
        distinct_sepetiba_angra = self.sepetiba_angra_pd[header_out_pandas].drop_duplicates()
        distinct_sepetiba_angra = distinct_sepetiba_angra.rename(columns=header_pandas_to_sql)

        # Update informacoes de data
        distinct_sepetiba_angra["data_procedimento"] = distinct_sepetiba_angra.apply(lambda row: date_formatter.set_year_movimentacao(row["data_procedimento"]), axis=1)
        distinct_sepetiba_angra = handle_pandas.format_praticagem_programado(distinct_sepetiba_angra)

        # Obtem somente o diferencial das informacoes entre o scrap atual e os dados que temos na base de dados
        historico_sepetiba_angra = praticagem_db.get_select_top_100(sql_table_name)
        historico_sepetiba_angra = historico_sepetiba_angra[list(sql_table_header)].drop_duplicates()
        sepetiba_angra_insert_target = handle_pandas.get_diff(historico_sepetiba_angra, distinct_sepetiba_angra)

        # Cria os chunks dos dados
        sepetiba_angra_chunk = [tuple(x) for x in sepetiba_angra_insert_target.values]
        sepetiba_angra_chunk_aux = []
        for row in sepetiba_angra_chunk:    
            sepetiba_angra_chunk_aux.append(tuple([str(x) for x in row]))

        # Trata chunk de dados
        # OBS: O site nao fornece o ano da atracacao, sendo assim, adotamos o valor descrito na funcao helpers/date_formater/set_year_movimentacao
        #   isso pode causar desencontros na virada do ano
        chunk_sepetiba_angra_final = praticagem_db.chunk_to_data_type_filter(sepetiba_angra_chunk_aux, sql_table_type)

        # Obtem o diferencial das informacoes ja existentes ja base de dados e o apresentado no site
        # Insere os dados
        if chunk_sepetiba_angra_final == []:
            logger.info("Sepetiba-Angra: Dados nao inseridos; Somente dados repetidos")
        else:
            praticagem_db.insert_chunk(sql_table_name, sql_table_header, chunk_sepetiba_angra_final)


if __name__ == "__main__":
    ss.make_dir()
    RJ = RioJaneiro(url="http://www.praticagem-rj.com.br/")
    RJ.to_mysql()
