import bs4
import pandas
import js2py

def parse_dados_navio(linha_info, navios):
    """
    Recebe a linha da informacao e um objeto java contendo as informacoes dos navios
    Funcao usada no site da praticagem do Rio de Janeiro

    :param linha_info: Integer
    :param navios: JavaScript OBJ
    :return: list
    """

    header = []
    data_navio = []

    html = navios[linha_info]
    soup = bs4.BeautifulSoup(html, "lxml")
    html_navio = [x.find("td", style="padding: 5px;") for x in soup.find_all("tr")]
    for info in html_navio:
        if info is None:
            pass
        else:
            header = ["navio_info_"+x.text for x in info.find_all("th")]
            data_navio = [x.text for x in info.find_all("td")][1:]

    return dict(zip(header, data_navio))


def lista_portos(soup):
    """
    Estutura a informacao da pagina: http://www.praticagem-rj.com.br/

    :param soup: BeautifulSoup obj
    :return: Pandas DataFrame ou False caso tenhamos problemas
    """
    # quadro = soup.find_all("td", class_="quadro")
    lista_publicacao_portos = {}
    for quadro in soup.find_all("td", class_="quadro"):

        try:
            # Parseia cada uma das informacoes
            header = [x.text for x in quadro.find_all("th")] + ["nome_porto", "data_abertura"]
            nome_porto = quadro.find("span").text.split(" - ")[0]
            data_abertura = quadro.find("span").text.split(" - ")[1]
            navios = js2py.eval_js("var " + soup.find_all("script", language="javascript")[0].text.split("var")[1])

            # Formata dados
            data_table = [x.find_all("td") for x in quadro.find_all("tr")[3:]]
            data_out = []
            for row in data_table:
                row_aux = {"nome_porto": nome_porto, "data_abertura": data_abertura}
                for key, value in zip(header, row):

                    if key == "NAVIO":
                        # Para obter a informacao do navio
                        navio_n = int("".join([s for s in str(value.a["onmouseover"]) if s.isdigit()]))
                        data_navio = parse_dados_navio(navio_n, navios)
                        header_navios = list(data_navio.keys())

                    row_aux[key] = value.text

                row_aux.update(data_navio)
                data_out.append(row_aux)

            header = header + header_navios
            lista_publicacao_portos[nome_porto] = pandas.DataFrame(data=data_out, columns=header)

        except Exception:
            pass

    return lista_publicacao_portos
