import pandas
import datetime
import math


def str_to_float(x):
    try:
        x = x.replace(",", ".")
        x = float(x)
        return x
    except:
        return x

def str_to_int(x):

    try:
        x = int(x)
        return x
    except:

        if math.isnan(x):
            return 0
        else:
            return x

def data_type_pandas(data_cell, type_):
    """
    Dado um celula de informacao do csv e o tipo de dado da tabela sql, trata a informacao

    :param data_cell:
    :param type:
    :return: informacao no tipo esperado
    """

    letters = "abcdefghijklmnopqrstuvxyz"
    type_ = "".join([x for x in str(type_) if x in letters])

    if data_cell == "Timestamp":
        return data_cell

    if data_cell == None:
        return 'None'

    if data_cell == 'nan':
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
        data_cell = str(data_cell)
        format = "%Y-%m-%d %H:%M:%S"
        date = datetime.datetime.strptime(data_cell, format)
        return date

    if data_cell.strip() == '':
        return 'None'

def get_diff(df1, df2):
    """
    Dado dois data frames, obtem as linhas diferentes
    :return: DataFrame
    """

    df = pandas.concat([df1, df2])
    df = df.reset_index(drop=True)
    #print(df.sort("data_procedimento"))
    
    df_gpby = df.groupby(list(df.columns))
    idx = [x[0] for x in df_gpby.groups.values() if len(x) == 1]

    df_out = df.reindex(idx)
    #print("\n")
    #print("get_diff")
    #print(df_out)
    return df_out

def format_praticagem_programado(dataframe):

        dataframe["data_procedimento"] = dataframe.apply(lambda row: data_type_pandas(row["data_procedimento"], "datetime"), axis=1)
        dataframe["nome_navio"] = dataframe.apply(lambda row: data_type_pandas(row["nome_navio"], "varchar(200)"), axis=1)
        dataframe["calado"] = dataframe.apply(lambda row: data_type_pandas(row["calado"], "float"), axis=1)
        dataframe["loa"] = dataframe.apply(lambda row: data_type_pandas(row["loa"], "float"), axis=1)
        dataframe["boca"] = dataframe.apply(lambda row: data_type_pandas(row["boca"], "float"), axis=1)
        dataframe["gt"] = dataframe.apply(lambda row: data_type_pandas(row["gt"], "float"), axis=1)
        dataframe["dwt"] = dataframe.apply(lambda row: data_type_pandas(row["dwt"], "float"), axis=1)
        dataframe["manobra"] = dataframe.apply(lambda row: data_type_pandas(row["manobra"], "varchar(50)"), axis=1)
        dataframe["de"] = dataframe.apply(lambda row: data_type_pandas(row["de"], "varchar(200)"), axis=1)
        dataframe["para"] = dataframe.apply(lambda row: data_type_pandas(row["para"], "varchar(200)"), axis=1)
        dataframe["brd"] = dataframe.apply(lambda row: data_type_pandas(row["brd"], "varchar(15)"), axis=1)
        dataframe["nome_porto"] = dataframe.apply(lambda row: data_type_pandas(row["nome_porto"], "varchar(200)"), axis=1)
        dataframe["tipo_navio"] = dataframe.apply(lambda row: data_type_pandas(row["tipo_navio"], "varchar(200)"), axis=1)
        dataframe["prefixo"] = dataframe.apply(lambda row: data_type_pandas(row["prefixo"], "varchar(50)"), axis=1)
        dataframe["mmsi"] = dataframe.apply(lambda row: data_type_pandas(row["mmsi"], "int(11)"), axis=1)
        dataframe["imo"] = dataframe.apply(lambda row: data_type_pandas(row["imo"], "int(11)"), axis=1)
        dataframe["bandeira"] = dataframe.apply(lambda row: data_type_pandas(row["bandeira"], "varchar(200)"), axis=1) 


        return dataframe


if __name__ == "__main__":
    import configkeys
    from mysql_handler_rio_de_janeiro import dbHandler

    keys = configkeys.mysql_keys
    praticagem_db = dbHandler(host=keys["host"], database=keys["database"],
                            user=keys["login"], password=keys["senha"])

    sql_table_name = "praticagem_programado_sepetiba_angra"
    historico_pd = praticagem_db.get_select_top_100(sql_table_name)
    historico_pd["mmsi"] = historico_pd.apply(lambda row: data_type_pandas(row["mmsi"], "int(11)"), axis=1)
    #historico_pd = format_praticagem_programado(historico_pd)
    print(historico_pd)
    frame = historico_pd["mmsi"].astype('int')
    print(frame)
    #print(historico_pd["mmsi"])