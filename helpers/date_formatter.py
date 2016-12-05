from dateutil import parser
import datetime


def datetime_to_yyyymmdd_hhmm(value):
    """
    Recebe data isoformat e tranforma no nome padrao para nomear arquivos

    :param value: datetime.isoformat()
    :return: str
    """
    return parser.parse(value).strftime('%Y%m%d_%H%M%S')

def set_year_movimentacao(value):
    """
    Recebe data isoformat e acrescenta o ano em que o movimento aconteceu

    :param value: datetime.isoformat()
    :return: datetime.isoformat()
    """

    # Formata a data como datetime
    format = "%d/%m %H:%M"
    value = datetime.datetime.strptime(value, format)

    # Obtem o horario atual
    current_date = datetime.datetime.utcnow()
    current_year = current_date.year
    current_month = current_date.month

    # Inicia a Classificacao 
    if current_month == 12:
        if value.month < 12:
            data_final = value.replace(year=current_year+1)
        else:
            data_final = value.replace(year=current_year)
    else:
        if current_month == 1:
            if value.month == 12:
                data_final = value.replace(year=current_year-1)
            else:
                data_final = value.replace(year=current_year)                

    #print("Set_year: ", value, data_final)
    return str(data_final)


if __name__ == "__main__":
    datetime_to_yyyymmdd_hhmm("2016-12-01 15:49:19.950291")