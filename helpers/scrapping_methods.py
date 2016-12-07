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
        self.status_HTTP_request = None

    def get_html_urllib(self, num_try=0):
        """
        get html from the webpage
        :return:
        """
        self.datetime_extracao = datetime.utcnow()
        self.html = urllib.request.urlopen(self.url)
        self.status_HTTP_request = self.html.status
        
        if self.html.status != 200 and num_try==0:
            time.sleep(30)
            self.get_html_urllib(num_try=1)

        # Caso tenhamos conseguido obter o html com sucesso
        if self.html:
            self.parse()

    def parse(self):
        self.soup = bs4.BeautifulSoup(self.html, "lxml")
