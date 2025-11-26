-- Criação do Banco de Dados
CREATE DATABASE filmes_db;
USE filmes_db;

-- ============================
-- 1. TABELA PRINCIPAL: FILMES
-- ============================

CREATE TABLE Filmes (
    id_filme INT AUTO_INCREMENT PRIMARY KEY,
    titulo VARCHAR(255) NOT NULL,
    link_imdb VARCHAR(500),
    ano_lancamento INT,
    duracao_minutos INT,
    classificacao_mpa VARCHAR(20),
    nota_imdb DECIMAL(3,1),
    votos_imdb INT,
    orcamento DECIMAL(15,2),
    bilheteria_mundial DECIMAL(15,2),
    bilheteria_eua_canada DECIMAL(15,2),
    bilheteria_abertura DECIMAL(15,2),
    vitorias_premios INT,
    nominacoes_premios INT,
    vitorias_oscar INT
) ENGINE=InnoDB;

-- ==================================
-- 2. TABELAS DE DIMENSÃO
-- ==================================

CREATE TABLE Pessoas (
    id_pessoa INT AUTO_INCREMENT PRIMARY KEY,
    nome_pessoa VARCHAR(255) NOT NULL
) ENGINE=InnoDB;

CREATE TABLE Generos (
    id_genero INT AUTO_INCREMENT PRIMARY KEY,
    nome_genero VARCHAR(100) NOT NULL
) ENGINE=InnoDB;

CREATE TABLE Paises (
    id_pais INT AUTO_INCREMENT PRIMARY KEY,
    nome_pais VARCHAR(150) NOT NULL
) ENGINE=InnoDB;

CREATE TABLE Empresas (
    id_empresa INT AUTO_INCREMENT PRIMARY KEY,
    nome_empresa VARCHAR(255) NOT NULL
) ENGINE=InnoDB;

CREATE TABLE Idiomas (
    id_idioma INT AUTO_INCREMENT PRIMARY KEY,
    nome_idioma VARCHAR(150) NOT NULL
) ENGINE=InnoDB;

-- ==================================
-- 3. TABELAS ASSOCIATIVAS
-- ==================================

-- Tabela para Estrelas/Atores principais
CREATE TABLE Filme_Estrela (
    id_filme INT,
    id_pessoa INT,
    ordem_credito INT DEFAULT NULL, -- Para ordenação dos atores principais
    PRIMARY KEY (id_filme, id_pessoa),
    FOREIGN KEY (id_filme) REFERENCES Filmes(id_filme) ON DELETE CASCADE,
    FOREIGN KEY (id_pessoa) REFERENCES Pessoas(id_pessoa) ON DELETE CASCADE
) ENGINE=InnoDB;

-- Tabela para Diretores
CREATE TABLE Filme_Diretor (
    id_filme INT,
    id_pessoa INT,
    PRIMARY KEY (id_filme, id_pessoa),
    FOREIGN KEY (id_filme) REFERENCES Filmes(id_filme) ON DELETE CASCADE,
    FOREIGN KEY (id_pessoa) REFERENCES Pessoas(id_pessoa) ON DELETE CASCADE
) ENGINE=InnoDB;

-- Tabela para Roteiristas
CREATE TABLE Filme_Roteirista (
    id_filme INT,
    id_pessoa INT,
    PRIMARY KEY (id_filme, id_pessoa),
    FOREIGN KEY (id_filme) REFERENCES Filmes(id_filme) ON DELETE CASCADE,
    FOREIGN KEY (id_pessoa) REFERENCES Pessoas(id_pessoa) ON DELETE CASCADE
) ENGINE=InnoDB;

CREATE TABLE Filme_Genero (
    id_filme INT,
    id_genero INT,
    PRIMARY KEY (id_filme, id_genero),
    FOREIGN KEY (id_filme) REFERENCES Filmes(id_filme) ON DELETE CASCADE,
    FOREIGN KEY (id_genero) REFERENCES Generos(id_genero) ON DELETE CASCADE
) ENGINE=InnoDB;

CREATE TABLE Filme_Pais_Origem (
    id_filme INT,
    id_pais INT,
    PRIMARY KEY (id_filme, id_pais),
    FOREIGN KEY (id_filme) REFERENCES Filmes(id_filme) ON DELETE CASCADE,
    FOREIGN KEY (id_pais) REFERENCES Paises(id_pais) ON DELETE CASCADE
) ENGINE=InnoDB;

CREATE TABLE Filme_Empresa_Producao (
    id_filme INT,
    id_empresa INT,
    PRIMARY KEY (id_filme, id_empresa),
    FOREIGN KEY (id_filme) REFERENCES Filmes(id_filme) ON DELETE CASCADE,
    FOREIGN KEY (id_empresa) REFERENCES Empresas(id_empresa) ON DELETE CASCADE
) ENGINE=InnoDB;

CREATE TABLE Filme_Idioma (
    id_filme INT,
    id_idioma INT,
    PRIMARY KEY (id_filme, id_idioma),
    FOREIGN KEY (id_filme) REFERENCES Filmes(id_filme) ON DELETE CASCADE,
    FOREIGN KEY (id_idioma) REFERENCES Idiomas(id_idioma) ON DELETE CASCADE
) ENGINE=InnoDB;

-- ==================================
-- ÍNDICES RECOMENDADOS
-- ==================================
CREATE INDEX idx_filmes_titulo ON Filmes(titulo);
CREATE INDEX idx_pessoas_nome ON Pessoas(nome_pessoa);
CREATE INDEX idx_generos_nome ON Generos(nome_genero);
CREATE INDEX idx_paises_nome ON Paises(nome_pais);
CREATE INDEX idx_empresas_nome ON Empresas(nome_empresa);
CREATE INDEX idx_idiomas_nome ON Idiomas(nome_idioma);