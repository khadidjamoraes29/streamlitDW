import pandas as pd
import pymysql
import re
import os

class FilmesNormalizer:
    def __init__(self, host='127.0.0.1', user='root', password='Khadidja', database='filmes_base'):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.connection = None
        self.cursor = None

    # ----------------------------
    # Conexão com o banco
    # ----------------------------
    def conectar_banco(self, criar_db=False):
        try:
            
            print("[INFO] Conectando ao banco...")
            self.connection = pymysql.connect(
                host=self.host,
                user=self.user,
                password='Khadidja',
                charset='utf8mb4',
                port=3306,
                autocommit=False
            )
            self.cursor = self.connection.cursor()
            if criar_db:
                self.cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.database}")
                print(f"[INFO] Banco '{self.database}' criado ou já existia.")
            self.cursor.execute(f"USE {self.database}")
            print(f"[INFO] Usando banco '{self.database}'")
            return True
        except Exception as e:
            print(f"[ERRO] Erro ao conectar/criar banco: {e}")
            return False

    def desconectar_banco(self):
        if self.connection:
            self.cursor.close()
            self.connection.close()
            print("[INFO] Conexão com o banco encerrada.")

    # ----------------------------
    # Criação das tabelas
    # ----------------------------
    def criar_tabelas(self):
        try:
            print("[INFO] Criando tabelas...")
            sqls = [
                # Tabela principal
                """
                CREATE TABLE IF NOT EXISTS Filmes (
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
                """,
                # Tabelas de dimensão
                """
                CREATE TABLE IF NOT EXISTS Pessoas (
                    id_pessoa INT AUTO_INCREMENT PRIMARY KEY,
                    nome_pessoa VARCHAR(255) NOT NULL
                ) ENGINE=InnoDB;
                """,
                """
                CREATE TABLE IF NOT EXISTS Generos (
                    id_genero INT AUTO_INCREMENT PRIMARY KEY,
                    nome_genero VARCHAR(100) NOT NULL
                ) ENGINE=InnoDB;
                """,
                """
                CREATE TABLE IF NOT EXISTS Paises (
                    id_pais INT AUTO_INCREMENT PRIMARY KEY,
                    nome_pais VARCHAR(150) NOT NULL
                ) ENGINE=InnoDB;
                """,
                """
                CREATE TABLE IF NOT EXISTS Empresas (
                    id_empresa INT AUTO_INCREMENT PRIMARY KEY,
                    nome_empresa VARCHAR(255) NOT NULL
                ) ENGINE=InnoDB;
                """,
                """
                CREATE TABLE IF NOT EXISTS Idiomas (
                    id_idioma INT AUTO_INCREMENT PRIMARY KEY,
                    nome_idioma VARCHAR(150) NOT NULL
                ) ENGINE=InnoDB;
                """,
                # Tabelas associativas
                """
                CREATE TABLE IF NOT EXISTS Filme_Estrela (
                    id_filme INT,
                    id_pessoa INT,
                    ordem_credito INT DEFAULT NULL,
                    PRIMARY KEY (id_filme, id_pessoa),
                    FOREIGN KEY (id_filme) REFERENCES Filmes(id_filme) ON DELETE CASCADE,
                    FOREIGN KEY (id_pessoa) REFERENCES Pessoas(id_pessoa) ON DELETE CASCADE
                ) ENGINE=InnoDB;
                """,
                """
                CREATE TABLE IF NOT EXISTS Filme_Diretor (
                    id_filme INT,
                    id_pessoa INT,
                    PRIMARY KEY (id_filme, id_pessoa),
                    FOREIGN KEY (id_filme) REFERENCES Filmes(id_filme) ON DELETE CASCADE,
                    FOREIGN KEY (id_pessoa) REFERENCES Pessoas(id_pessoa) ON DELETE CASCADE
                ) ENGINE=InnoDB;
                """,
                """
                CREATE TABLE IF NOT EXISTS Filme_Roteirista (
                    id_filme INT,
                    id_pessoa INT,
                    PRIMARY KEY (id_filme, id_pessoa),
                    FOREIGN KEY (id_filme) REFERENCES Filmes(id_filme) ON DELETE CASCADE,
                    FOREIGN KEY (id_pessoa) REFERENCES Pessoas(id_pessoa) ON DELETE CASCADE
                ) ENGINE=InnoDB;
                """,
                """
                CREATE TABLE IF NOT EXISTS Filme_Genero (
                    id_filme INT,
                    id_genero INT,
                    PRIMARY KEY (id_filme, id_genero),
                    FOREIGN KEY (id_filme) REFERENCES Filmes(id_filme) ON DELETE CASCADE,
                    FOREIGN KEY (id_genero) REFERENCES Generos(id_genero) ON DELETE CASCADE
                ) ENGINE=InnoDB;
                """,
                """
                CREATE TABLE IF NOT EXISTS Filme_Pais_Origem (
                    id_filme INT,
                    id_pais INT,
                    PRIMARY KEY (id_filme, id_pais),
                    FOREIGN KEY (id_filme) REFERENCES Filmes(id_filme) ON DELETE CASCADE,
                    FOREIGN KEY (id_pais) REFERENCES Paises(id_pais) ON DELETE CASCADE
                ) ENGINE=InnoDB;
                """,
                """
                CREATE TABLE IF NOT EXISTS Filme_Empresa_Producao (
                    id_filme INT,
                    id_empresa INT,
                    PRIMARY KEY (id_filme, id_empresa),
                    FOREIGN KEY (id_filme) REFERENCES Filmes(id_filme) ON DELETE CASCADE,
                    FOREIGN KEY (id_empresa) REFERENCES Empresas(id_empresa) ON DELETE CASCADE
                ) ENGINE=InnoDB;
                """,
                """
                CREATE TABLE IF NOT EXISTS Filme_Idioma (
                    id_filme INT,
                    id_idioma INT,
                    PRIMARY KEY (id_filme, id_idioma),
                    FOREIGN KEY (id_filme) REFERENCES Filmes(id_filme) ON DELETE CASCADE,
                    FOREIGN KEY (id_idioma) REFERENCES Idiomas(id_idioma) ON DELETE CASCADE
                ) ENGINE=InnoDB;
                """
            ]
            for sql in sqls:
                self.cursor.execute(sql)
            self.connection.commit()
            print("[INFO] Tabelas criadas com sucesso.")
        except Exception as e:
            print(f"[ERRO] Erro ao criar tabelas: {e}")
            self.connection.rollback()

    # ----------------------------
    # Inserção de dados
    # ----------------------------
    def inserir_ou_obter_id(self, tabela, campo_nome, valor, campo_id):
        if not valor or pd.isna(valor) or str(valor).strip() == '':
            return None
        valor = str(valor).strip()
        self.cursor.execute(f"SELECT {campo_id} FROM {tabela} WHERE {campo_nome} = %s", (valor,))
        resultado = self.cursor.fetchone()
        if resultado:
            return resultado[0]
        self.cursor.execute(f"INSERT INTO {tabela} ({campo_nome}) VALUES (%s)", (valor,))
        return self.cursor.lastrowid

    def processar_lista_valores(self, valores_str, separador=','):
        if not valores_str or pd.isna(valores_str):
            return []
        return [v.strip() for v in str(valores_str).split(separador) if v.strip()]

    def limpar_duracao(self, duracao_str):
        if not duracao_str or pd.isna(duracao_str):
            return None
        total_minutos = 0
        horas_match = re.search(r'(\d+)h', str(duracao_str))
        if horas_match:
            total_minutos += int(horas_match.group(1)) * 60
        minutos_match = re.search(r'(\d+)m', str(duracao_str))
        if minutos_match:
            total_minutos += int(minutos_match.group(1))
        return total_minutos if total_minutos > 0 else None

    def limpar_valor_numerico(self, valor):
        if pd.isna(valor) or valor == '' or valor is None:
            return None
        try:
            return float(valor)
        except (ValueError, TypeError):
            print(f"[ALERTA] Valor numérico inválido: {valor}")
            return None

    def processar_filme(self, row, numero_linha=None):
        try:
            titulo = None if pd.isna(row.get('title')) else row.get('title')
            if numero_linha:
                print(f"[INFO] Processando filme #{numero_linha}: {titulo}")
            query_filme = """
                INSERT INTO Filmes (
                    titulo, link_imdb, ano_lancamento, duracao_minutos, 
                    classificacao_mpa, nota_imdb, votos_imdb, orcamento,
                    bilheteria_mundial, bilheteria_eua_canada, bilheteria_abertura,
                    vitorias_premios, nominacoes_premios, vitorias_oscar
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            valores = (
                titulo,
                None if pd.isna(row.get('link')) else row.get('link'),
                int(row['year']) if pd.notna(row.get('year')) else None,
                self.limpar_duracao(row.get('duration')),
                None if pd.isna(row.get('rating_mpa')) else row.get('rating_mpa'),
                self.limpar_valor_numerico(row.get('rating_imdb')),
                self.limpar_valor_numerico(row.get('vote')),
                self.limpar_valor_numerico(row.get('budget')),
                self.limpar_valor_numerico(row.get('gross_world_wide')),
                self.limpar_valor_numerico(row.get('gross_us_canada')),
                self.limpar_valor_numerico(row.get('gross_opening_weekend')),
                self.limpar_valor_numerico(row.get('win')),
                self.limpar_valor_numerico(row.get('nomination')),
                self.limpar_valor_numerico(row.get('oscar'))
            )
            self.cursor.execute(query_filme, valores)
            id_filme = self.cursor.lastrowid

            dim_id_map = {
                'Generos': 'id_genero',
                'Paises': 'id_pais',
                'Empresas': 'id_empresa',
                'Idiomas': 'id_idioma',
                'Pessoas': 'id_pessoa'
            }

            def inserir_associativa(tabela_assoc, tabela_dim, campo_nome, coluna_csv):
                coluna_id = dim_id_map[tabela_dim]
                for v in self.processar_lista_valores(row.get(coluna_csv,'')):
                    id_dim = self.inserir_ou_obter_id(tabela_dim, campo_nome, v, coluna_id)
                    if id_dim:
                        self.cursor.execute(
                            f"INSERT IGNORE INTO {tabela_assoc} (id_filme, {coluna_id}) VALUES (%s,%s)",
                            (id_filme, id_dim)
                        )

            # Associativas
            for i, estrela in enumerate(self.processar_lista_valores(row.get('star',''))):
                id_pessoa = self.inserir_ou_obter_id('Pessoas','nome_pessoa', estrela, 'id_pessoa')
                if id_pessoa:
                    self.cursor.execute(
                        "INSERT IGNORE INTO Filme_Estrela (id_filme,id_pessoa,ordem_credito) VALUES (%s,%s,%s)",
                        (id_filme,id_pessoa,i+1)
                    )

            for diretor in self.processar_lista_valores(row.get('director','')):
                id_pessoa = self.inserir_ou_obter_id('Pessoas','nome_pessoa', diretor, 'id_pessoa')
                if id_pessoa:
                    self.cursor.execute(
                        "INSERT IGNORE INTO Filme_Diretor (id_filme,id_pessoa) VALUES (%s,%s)",
                        (id_filme,id_pessoa)
                    )

            for roteirista in self.processar_lista_valores(row.get('writer','')):
                id_pessoa = self.inserir_ou_obter_id('Pessoas','nome_pessoa', roteirista, 'id_pessoa')
                if id_pessoa:
                    self.cursor.execute(
                        "INSERT IGNORE INTO Filme_Roteirista (id_filme,id_pessoa) VALUES (%s,%s)",
                        (id_filme,id_pessoa)
                    )

            inserir_associativa('Filme_Genero','Generos','nome_genero','genre')
            inserir_associativa('Filme_Pais_Origem','Paises','nome_pais','country_origin')
            inserir_associativa('Filme_Empresa_Producao','Empresas','nome_empresa','production_company')
            inserir_associativa('Filme_Idioma','Idiomas','nome_idioma','language')

            self.connection.commit()  # commit por filme
            return True
        except Exception as e:
            print(f"[ERRO] Ao processar filme '{titulo}': {e}")
            self.connection.rollback()
            return False

    # ----------------------------
    # Normalização CSV com logs e chunks
    # ----------------------------
    def normalizar_csv(self, arquivo_csv):
        if not os.path.exists(arquivo_csv):
            print(f"[ERRO] Arquivo não encontrado: {arquivo_csv}")
            return
        
        print(f"[INFO] Iniciando leitura do CSV: {arquivo_csv}")
        chunk_size = 100
        total = 0
        for chunk in pd.read_csv(arquivo_csv, chunksize=chunk_size):
            print(f"[INFO] Processando linhas {total+1} até {total+len(chunk)}")
            for idx, row in chunk.iterrows():
                self.processar_filme(row, total+idx+1)
            total += len(chunk)
            print(f"[INFO] Total de filmes processados até agora: {total}")
        print(f"[INFO] Normalização concluída! Total de filmes processados: {total}")

# ----------------------------
# EXECUÇÃO
# ----------------------------
if __name__ == "__main__":
    arquivo_csv = "filmes_ingles_apos_2000.csv"
    normalizer = FilmesNormalizer(user='root', password='root123')
    if normalizer.conectar_banco(criar_db=True):
        normalizer.criar_tabelas()
        normalizer.normalizar_csv(arquivo_csv)
        normalizer.desconectar_banco()
        print("[INFO] Banco criado e dados inseridos com sucesso!")
