# Scraping - Praticagem Portos - Rio de Janeiro

Scrapping do site: http://www.praticagem-rj.com.br

Versão estável


# to_csv() & to_mysql()
 - Com check de status da requisição HTTP
 - Identifica os objetos da pagina que devem ser parseados

 - Cuidado: Caso ele não consiga pegar a informação de manobras de um porto, aplicação silencía o evento e só traz o que foi possível.
Então, sempre observe as mudanças no site

# crontab

*/30 * * * * cd /home/mauriciolongato/scraping_praticagem && /usr/bin/python3.5 ./scrap_rio_de_janeiro.py >> ./log/crontab_service 2>&1

# Instalação
(usando python3.5)

* pip3 install -r requirements.txt
* Crie uma pasta log no root da aplicação
* No arquivo configkeys.py, coloque suas credenciais do mysql
* command-line: python3.5 scrap_rio_de_janeiro.py
