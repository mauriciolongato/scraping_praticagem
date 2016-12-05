import os


def make_dir():
    # Set directories
    directories = ["./html",
                   "./log",
                   "./dados_processados",
                   "./erros"]

    for directory in directories:
        if not os.path.exists(directory):

            os.makedirs(directory)