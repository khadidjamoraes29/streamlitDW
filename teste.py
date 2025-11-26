import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

# ==============================
# 0️⃣ Carregar CSV e limpar texto
# ==============================
df = pd.read_csv(r"base de dados nao normalizada\world_imdb_movies_top_movies_per_year.csv[1]", encoding='utf-8')

# Corrige textos para UTF-8 seguro
text_cols = df.select_dtypes(include="object").columns
for col in text_cols:
    df[col] = df[col].astype(str).apply(lambda x: x.encode("utf-8", errors="ignore").decode("utf-8"))

# Explode colunas de listas
movie_director = df[["id", "director"]].assign(director=df["director"].str.split(",\s*")).explode("director")
movie_writer   = df[["id", "writer"]].assign(writer=df["writer"].str.split(",\s*")).explode("writer")
movie_star     = df[["id", "star"]].assign(star=df["star"].str.split(",\s*")).explode("star")
movie_genre    = df[["id", "genre"]].assign(genre=df["genre"].str.split(",\s*")).explode("genre")
movie_language = df[["id", "language"]].assign(language=df["language"].str.split(",\s*")).explode("language")

movies = df.drop(columns=['director','writer','star','genre','language'])

# ==============================
# 1️⃣ Criar tabelas de dimensão
# ==============================
# Atores, Diretores, Escritores
atores = movie_star[['star']].drop_duplicates().rename(columns={'star':'nome_ator'})
diretores = movie_director[['director']].drop_duplicates().rename(columns={'director':'nome_diretor'})
escritores = movie_writer[['writer']].drop_duplicates().rename(columns={'writer':'nome_escritor'})

# Gêneros, Idiomas, Países, Localidades, Empresas
generos = movie_genre[['genre']].drop_duplicates().rename(columns={'genre':'nome_genero'}).reset_index(drop=True)
generos['id_genero'] = generos.index + 1

idiomas = movie_language[['language']].drop_duplicates().rename(columns={'language':'nome_idioma'}).reset_index(drop=True)
idiomas['id_idioma'] = idiomas.index + 1

paises = movies[['country_origin']].drop_duplicates().rename(columns={'country_origin':'nome_pais'}).reset_index(drop=True)
paises['id_pais'] = paises.index + 1

localidades = movies[['filming_location']].drop_duplicates().rename(columns={'filming_location':'nome_localidade'}).reset_index(drop=True)
localidades['id_localidade'] = localidades.index + 1

empresas = movies[['production_company']].drop_duplicates().rename(columns={'production_company':'nome_empresa'}).reset_index(drop=True)
empresas['id_empresa'] = empresas.index + 1

# ==============================
# 2️⃣ Criar tabelas associativas
# ==============================
filme_diretor = movie_director.merge(diretores, left_on="director", right_on="nome_diretor")[['id','nome_diretor']].rename(columns={'id':'id_filme'})
filme_escritor = movie_writer.merge(escritores, left_on="writer", right_on="nome_escritor")[['id','nome_escritor']].rename(columns={'id':'id_filme'})
filme_ator = movie_star.merge(atores, left_on='star', right_on='nome_ator')[['id','nome_ator']].rename(columns={'id':'id_filme'})

filme_genero = movie_genre.merge(generos, left_on='genre', right_on='nome_genero')[['id','id_genero']].rename(columns={'id':'id_filme'})
filme_idioma = movie_language.merge(idiomas, left_on='language', right_on='nome_idioma')[['id','id_idioma']].rename(columns={'id':'id_filme'})
filme_pais = movies.merge(paises, left_on='country_origin', right_on='nome_pais')[['id','id_pais']].rename(columns={'id':'id_filme'})
filme_localidade = movies.merge(localidades, left_on='filming_location', right_on='nome_localidade')[['id','id_localidade']].rename(columns={'id':'id_filme'})
filme_empresa = movies.merge(empresas, left_on='production_company', right_on='nome_empresa')[['id','id_empresa']].rename(columns={'id':'id_filme'})

# Equipe unificada
filme_equipe = pd.concat([
    filme_diretor.rename(columns={'nome_diretor':'id_pessoa'}).assign(papel='Diretor'),
    filme_escritor.rename(columns={'nome_escritor':'id_pessoa'}).assign(papel='Roteirista'),
    filme_ator.rename(columns={'nome_ator':'id_pessoa'}).assign(papel='Ator')
])

# ==============================
# 3️⃣ Configuração do banco
# ==============================
user = "user"
password = "user123"
password_enc = quote_plus(password)
host = "localhost"
port = 33016
database = "filmes"

engine = create_engine(f"mysql+pymysql://{user}:{password_enc}@{host}:{port}/{database}")
print("Conectado ao banco:", engine)

# ==============================
# 4️⃣ Criar tabelas no MySQL
# ==============================
with engine.begin() as conn:
    for table in ["filme_empresa","filme_localidade","filme_pais","filme_idioma","filme_genero",
                  "filme_equipe","filme_ator","filme_escritor","filme_diretor",
                  "empresas","localidades","paises","idiomas","generos","atores","escritores","diretores","movies"]:
        conn.execute(text(f"DROP TABLE IF EXISTS {table} CASCADE"))

    # Movies
    conn.execute(text("""
    CREATE TABLE movies (
        id VARCHAR(50) PRIMARY KEY,
        title VARCHAR(255),
        link VARCHAR(255),
        year INT,
        duration VARCHAR(50),
        rating_mpa VARCHAR(50),
        rating_imdb FLOAT,
        vote BIGINT,
        budget BIGINT,
        gross_world_wide BIGINT,
        gross_us_canada BIGINT,
        gross_opening_weekend BIGINT,
        country_origin VARCHAR(255),
        filming_location VARCHAR(255),
        production_company VARCHAR(255),
        win INT,
        nomination INT,
        oscar INT
    );"""))

    conn.execute(text("CREATE TABLE diretores (id_diretor INT AUTO_INCREMENT PRIMARY KEY, nome_diretor VARCHAR(255) UNIQUE);"))
    conn.execute(text("CREATE TABLE escritores (id_escritor INT AUTO_INCREMENT PRIMARY KEY, nome_escritor VARCHAR(255) UNIQUE);"))
    conn.execute(text("CREATE TABLE atores (id_ator INT AUTO_INCREMENT PRIMARY KEY, nome_ator VARCHAR(255) UNIQUE);"))
    conn.execute(text("CREATE TABLE generos (id_genero INT AUTO_INCREMENT PRIMARY KEY, nome_genero VARCHAR(255) UNIQUE);"))
    conn.execute(text("CREATE TABLE idiomas (id_idioma INT AUTO_INCREMENT PRIMARY KEY, nome_idioma VARCHAR(255) UNIQUE);"))
    conn.execute(text("CREATE TABLE paises (id_pais INT AUTO_INCREMENT PRIMARY KEY, nome_pais VARCHAR(255) UNIQUE);"))
    conn.execute(text("CREATE TABLE localidades (id_localidade INT AUTO_INCREMENT PRIMARY KEY, nome_localidade VARCHAR(255) UNIQUE);"))
    conn.execute(text("CREATE TABLE empresas (id_empresa INT AUTO_INCREMENT PRIMARY KEY, nome_empresa VARCHAR(255) UNIQUE);"))

    # Tabelas associativas
    conn.execute(text("CREATE TABLE filme_diretor (id_filme VARCHAR(50), nome_diretor VARCHAR(255), FOREIGN KEY(id_filme) REFERENCES movies(id));"))
    conn.execute(text("CREATE TABLE filme_escritor (id_filme VARCHAR(50), nome_escritor VARCHAR(255), FOREIGN KEY(id_filme) REFERENCES movies(id));"))
    conn.execute(text("CREATE TABLE filme_ator (id_filme VARCHAR(50), nome_ator VARCHAR(255), FOREIGN KEY(id_filme) REFERENCES movies(id));"))
    conn.execute(text("CREATE TABLE filme_genero (id_filme VARCHAR(50), id_genero INT, FOREIGN KEY(id_filme) REFERENCES movies(id), FOREIGN KEY(id_genero) REFERENCES generos(id_genero));"))
    conn.execute(text("CREATE TABLE filme_idioma (id_filme VARCHAR(50), id_idioma INT, FOREIGN KEY(id_filme) REFERENCES movies(id), FOREIGN KEY(id_idioma) REFERENCES idiomas(id_idioma));"))
    conn.execute(text("CREATE TABLE filme_pais (id_filme VARCHAR(50), id_pais INT, FOREIGN KEY(id_filme) REFERENCES movies(id), FOREIGN KEY(id_pais) REFERENCES paises(id_pais));"))
    conn.execute(text("CREATE TABLE filme_localidade (id_filme VARCHAR(50), id_localidade INT, FOREIGN KEY(id_filme) REFERENCES movies(id), FOREIGN KEY(id_localidade) REFERENCES localidades(id_localidade));"))
    conn.execute(text("CREATE TABLE filme_empresa (id_filme VARCHAR(50), id_empresa INT, FOREIGN KEY(id_filme) REFERENCES movies(id), FOREIGN KEY(id_empresa) REFERENCES empresas(id_empresa));"))
    conn.execute(text("CREATE TABLE filme_equipe (id_filme VARCHAR(50), id_pessoa VARCHAR(255), papel ENUM('Diretor','Roteirista','Ator'), FOREIGN KEY(id_filme) REFERENCES movies(id));"))

# ==============================
# 5️⃣ Inserir dados
# ==============================
df.to_sql("movies", engine, index=False, if_exists="append")
diretores.to_sql("diretores", engine, index=False, if_exists="append")
escritores.to_sql("escritores", engine, index=False, if_exists="append")
atores.to_sql("atores", engine, index=False, if_exists="append")
generos.to_sql("generos", engine, index=False, if_exists="append")
idiomas.to_sql("idiomas", engine, index=False, if_exists="append")
paises.to_sql("paises", engine, index=False, if_exists="append")
localidades.to_sql("localidades", engine, index=False, if_exists="append")
empresas.to_sql("empresas", engine, index=False, if_exists="append")

filme_diretor.to_sql("filme_diretor", engine, index=False, if_exists="append")
filme_escritor.to_sql("filme_escritor", engine, index=False, if_exists="append")
filme_ator.to_sql("filme_ator", engine, index=False, if_exists="append")
filme_genero.to_sql("filme_genero", engine, index=False, if_exists="append")
filme_idioma.to_sql("filme_idioma", engine, index=False, if_exists="append")
filme_pais.to_sql("filme_pais", engine, index=False, if_exists="append")
filme_localidade.to_sql("filme_localidade", engine, index=False, if_exists="append")
filme_empresa.to_sql("filme_empresa", engine, index=False, if_exists="append")
filme_equipe.to_sql("filme_equipe", engine, index=False, if_exists="append")

print("✅ Banco de dados populado com sucesso!")
