-- tables
-- Table: praticagem_rio_de_janeiro
CREATE TABLE praticagem_programado_guanabara(
    id_procedimento int NOT NULL AUTO_INCREMENT,
    data_procedimento DATETIME NOT NULL,
    nome_navio VARCHAR(200),
    calado FLOAT,
    loa FLOAT,
    boca FLOAT,
    gt FLOAT,
    dwt FLOAT,
    manobra VARCHAR(50),
    de VARCHAR(200),
    para VARCHAR(200),
    brd VARCHAR(15),
    nome_porto VARCHAR(200),
    tipo_navio VARCHAR(200),
    prefixo VARCHAR(50),
    mmsi INT,
    imo INT,
    bandeira VARCHAR(200),
    CONSTRAINT praticagem_rio_de_janeiro_pk PRIMARY KEY (id_procedimento)
);

CREATE TABLE praticagem_programado_acu(
    id_procedimento int NOT NULL AUTO_INCREMENT,
    data_procedimento DATETIME NOT NULL,
    nome_navio VARCHAR(200),
    calado FLOAT,
    loa FLOAT,
    boca FLOAT,
    gt FLOAT,
    dwt FLOAT,
    manobra VARCHAR(50),
    de VARCHAR(200),
    para VARCHAR(200),
    brd VARCHAR(15),
    nome_porto VARCHAR(200),
    tipo_navio VARCHAR(200),
    prefixo VARCHAR(50),
    mmsi INT,
    imo INT,
    bandeira VARCHAR(200),
    CONSTRAINT praticagem_rio_de_janeiro_pk PRIMARY KEY (id_procedimento)
);

CREATE TABLE praticagem_programado_sepetiba_angra(
    id_procedimento int NOT NULL AUTO_INCREMENT,
    data_procedimento DATETIME NOT NULL,
    nome_navio VARCHAR(200),
    calado FLOAT,
    loa FLOAT,
    boca FLOAT,
    gt FLOAT,
    dwt FLOAT,
    manobra VARCHAR(50),
    de VARCHAR(200),
    para VARCHAR(200),
    brd VARCHAR(15),
    nome_porto VARCHAR(200),
    tipo_navio VARCHAR(200),
    prefixo VARCHAR(50),
    mmsi INT,
    imo INT,
    bandeira VARCHAR(200),
    CONSTRAINT praticagem_rio_de_janeiro_pk PRIMARY KEY (id_procedimento)
);

CREATE TABLE praticagem_programado_forno(
    id_procedimento int NOT NULL AUTO_INCREMENT,
    data_procedimento DATETIME NOT NULL,
    nome_navio VARCHAR(200),
    calado FLOAT,
    loa FLOAT,
    boca FLOAT,
    gt FLOAT,
    dwt FLOAT,
    manobra VARCHAR(50),
    de VARCHAR(200),
    para VARCHAR(200),
    brd VARCHAR(15),
    nome_porto VARCHAR(200),
    tipo_navio VARCHAR(200),
    prefixo VARCHAR(50),
    mmsi INT,
    imo INT,
    bandeira VARCHAR(200),
    CONSTRAINT praticagem_rio_de_janeiro_pk PRIMARY KEY (id_procedimento)
);