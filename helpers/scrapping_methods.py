import urllib.request
from datetime import datetime

import bs4


class Scrapping:
    def __init__(self, url):
        """
        Classe responsavel por fazer a requisicao e obter o html da pagina
        :param url:
        """
        self.url = url
        self.html = None
        self.soup = None
        self.datetime_extracao = None

    def get_html_urllib(self):
        """
        get html from the webpage
        :return:
        """
        self.datetime_extracao = datetime.utcnow()
        self.html = urllib.request.urlopen(self.url)

        # Caso tenhamos conseguido obter o html com sucesso
        if self.html:
            self.parse()

    def parse(self):
        self.soup = bs4.BeautifulSoup(self.html, "lxml")
