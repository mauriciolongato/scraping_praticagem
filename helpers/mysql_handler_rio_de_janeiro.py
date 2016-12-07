import uuid
import logging
import pandas
import datetime
import mysql.connector
import csv


# import csv_handler

# Cria o logger e seta o nivel de debug
# se quiser saber mais sobre essa loggers:
#   http://docs.python-guide.org/en/latest/writing/logging/
#   https://fangpenlin.com/posts/2012/08/26/good-logging-practice-in-python/

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Cria arquivo para receber os logs e seta o nivel de informacao (nesse caso: DEBUG)
fh = logging.FileHandler('log/rio_de_janeiro_mysql_connector.log')
fh.setLevel(logging.DEBUG)

# Define o formato que o log sera escrito
formatter = logging.Formatter("%(asctime)s;%(levelname)s;%(message)s", "%Y-%m-%d %H:%M:%S")
fh.setFormatter(formatter)

# Adiciona essa instancia do log
logger.addHandler(fh)

def str_to_float(x):

    try:
        x = x.replace(",", ".")
        x = float(x)
        return x
    except:
        return x


def str_to_int(x):
    try:
        if "." in x:
            x = x.split(".")[0]
        else:
            pass

        x = int(x)
        return x
    except:
        return x


def data_type_csv_to_python(data_cell, type_):
    """
    Dado um celula de informacao do csv e o tipo de dado da tabela sql, trata a informacao

    :param data_cell:
    :param type:
    :return: informacao no tipo esperado
    """

    letters = "abcdefghijklmnopqrstuvxyz"
    type_ = "".join([x for x in str(type_[1]) if x in letters])

    if data_cell == None:
        return data_cell

    if data_cell == 'None':
        return None

    if data_cell.strip() == '':
        return None

    if type_ == 'int':
        data_cell = str_to_int(data_cell)
        return data_cell

    if type_ == 'varchar':
        data_cell = str(data_cell)
        return data_cell

    if type_ == 'float':
        data_cell = str_to_float(data_cell)
        return data_cell

    if type_ == 'date':
        format = '%Y-%m-%d'
        return datetime.datetime.strptime(data_cell, format).date()

    if type_ == 'time':
        format = '%H:%M:%S'
        return datetime.datetime.strptime(data_cell, format).time()

    if type_ == 'datetime':
        format = "%Y-%m-%d %H:%M:%S"
        date = datetime.datetime.strptime(data_cell, format)
        return date


class dbHandler:

    def __init__(self, host, database, user, password):
        """
        Instacia o objeto necessario para a conexao com o DB desejado

        :param host: IP do servidor em que o servico sql esta instanciado
        :param database: Nome da base de dados que quer acessar
        :param user: Seu usuario
        :param password: Sua senha
        """
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        # logger.debug('Status connexao do usuario {}: {}'.format(self.user, self.conn.is_connected()))

    def get_select_top_100(self, table_name):
        """
        Funcao retorna todas as informacoes da tabela desejada

        :param table_name: nome da tabela
        :return: retorna pandas DataFrame
        """

        query = "select * from {} order by id_procedimento desc limit 100;".format(table_name)
        try:

            conn = mysql.connector.connect(host=self.host, database=self.database, user=self.user,
                                           password=self.password, buffered=True)
            logger.debug('select_all - status connexao do usuario {}: {}'.format(self.user, conn.is_connected()))

            cursor = conn.cursor()
            cursor.execute(query)
            data = cursor.fetchall()
            header = [x[0] for x in cursor.description]

            cursor.close()
            conn.close()

            data_out = pandas.DataFrame.from_records(data=data, columns=header)

        except Exception as e:
            logger.debug('Erro para obter os dados')
            logger.exception('message')

        finally:
            if conn.is_connected() == True:
                cursor.close()
                conn.close()

        return data_out

    def get_header(self, table_name):
        """
        Funcao retorna o nome das colunas da tabela desejada

        :param table_name: nome da tabela
        :return: retorna tupla contendo os nomes das colunas em formato unicode
        """
        query = "select * from {} limit 1;".format(table_name)
        try:
            conn = mysql.connector.connect(host=self.host, database=self.database, user=self.user,
                                           password=self.password, buffered=True)

            logger.debug('get_header - status connexao do usuario {}: {}'.format(self.user, conn.is_connected()))

            cursor = conn.cursor()
            cursor.execute(query)
            header = [str(x[0]) for x in cursor.description]

            cursor.close()
            conn.close()

        except Exception as e:
            logger.debug('Erro para obter nome das colunas')
            logger.exception('message')

        finally:
            if conn.close() == True:
                conn.close()

        return tuple(header)

    def get_columns_type(self, table_name):
        """
        Funcao retorna o nome das colunas da tabela desejada

        :param table_name: nome da tabela
        :return: retorna tupla contendo os nomes das colunas em formato unicode
        """
        query = "show columns from {};".format(table_name)
        # query = "select * from {} limit 1;".format(table_name)
        try:
            conn = mysql.connector.connect(host=self.host, database=self.database, user=self.user,
                                           password=self.password, buffered=True)

            logger.debug('get_columns_type - status connexao do usuario {}: {}'.format(self.user, conn.is_connected()))

            cursor = conn.cursor()
            cursor.execute(query)
            # col_info = [unicode(x) for x in cursor.fetchall()]
            col_info = [tuple(x) for x in cursor.fetchall()]

            cursor.close()
            conn.close()

        except Exception as e:
            logger.debug('Erro para obter nome das colunas')
            logger.exception('message')

        finally:
            if conn.close() == True:
                conn.close()

        return list(col_info)

    def insert_chunk(self, table_name, header, chunk):

        placeholders = ", ".join(['%s' for x in range(len(chunk[0]))])
        query = "insert into {}({}) values({});".format(table_name, ", ".join(header), placeholders)

        try:
            conn = mysql.connector.connect(host=self.host, database=self.database, user=self.user,
                                           password=self.password, buffered=True)

            logger.debug('insert_chunk - status connexao do usuario {}: {}'.format(self.user, conn.is_connected()))

            cursor = conn.cursor()
            cursor.executemany(query, chunk)
            conn.commit()

        except Exception as e:
            # chave = format(datetime.datetime.utcnow(), "%Y%m%d%H%M")
            chave = uuid.uuid1()
            logger.debug(
                'Nao foi possivel inserir os dados do chunk. Dados guardados no arquivo: ./erros/{}_{}.csv'.format(
                    chave, table_name))
            logger.exception('message')
            conn.rollback()

            # Escreve os erros em um arquivo externo
            data_out = pandas.DataFrame.from_records(data=chunk, columns=header)
            data_out.to_csv("./erros/{}_{}.csv".format(chave, table_name), sep=';', doublequote=True, quotechar='"',
                            quoting=csv.QUOTE_ALL)

        finally:
            cursor.close()
            if conn.close() == True:
                conn.close()


    def chunk_to_data_type_filter(self, chunk, columns_type):
        """
        Envia o boloca da informacao e a especificacao da
        :param chunk:
        :param columns_type:
        :return: chunk tratado
        """

        chunk_out = []
        for row in chunk:
            row_aux = []
            for cell, type_ in zip(row, columns_type):
                row_aux.append(data_type_csv_to_python(cell, type_))
            chunk_out.append(tuple(row_aux))

        return chunk_out


if __name__ == '__main__':

    sql_table_name = "praticagem_rio_de_janeiro"
    dir_address = './dados_processados'

    #c = csv_handler.csv_file(file_name='atracacoes_top10.csv', address="./201611_db")

    #chunk = []
    #for row in c.csv_dict():
        # Exemplo de chunk
        # chunk.append(tuple([row[x].strip() if row[x] != '' else None for x in c.get_header()]))
        # chunk.append(tuple([row[x] if row[x] != "" else None for x in header]))

    # antac_db = dbHandler('127.0.0.1', 'ship_call', 'root', 'root')
    antac_db = dbHandler('127.0.0.1', 'praticagem', 'mauriciolongato', '123456')

    # Get table information
    sql_table_header = antac_db.get_header(sql_table_name)

    sql_table_type = antac_db.get_columns_type(sql_table_name)
    # Trata os dados contidos no chunk

    #chunk = antac_db.chunk_to_data_type_filter(chunk, sql_table_type)
    # Insere os dados
    #antac_db.insert_chunk(sql_table_name, sql_table_header, chunk)
