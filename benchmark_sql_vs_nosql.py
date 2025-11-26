"""
Script de Benchmark: MySQL vs CouchDB

Compara o desempenho de consultas equivalentes entre:
- MySQL (banco relacional normalizado)
- CouchDB (banco NoSQL desnormalizado)

Queries testadas:
1. Buscar filmes por ano
2. Buscar filmes por ator
3. Buscar filmes por gênero
4. Buscar top 10 filmes por bilheteria
5. Contar filmes por gênero (agregação)

Autor: Sistema de Modelagem de Dados
Data: 2025-10-19
"""

import pymysql
import requests
import time
from typing import Dict, List, Tuple, Any
from datetime import datetime
import statistics

# =====================
# Configurações MySQL
# =====================
MYSQL_CONFIG = {
    "user": "root",
    "password": "mdrzz.0301",
    "host": "localhost",
    "port": 3306,
    "database": "filmes_db",
    "charset": "utf8mb4"
}

# =====================
# Configurações CouchDB
# =====================
COUCHDB_HOST = "http://localhost:5984"
COUCHDB_USER = "admin"
COUCHDB_PASSWORD = "mdrzz.0301"
COUCHDB_DATABASE = "filmes_db"

# Número de repetições para média de tempo
REPETICOES = 5


class BenchmarkResult:
    """Armazena resultados de um benchmark."""
    
    def __init__(self, nome: str):
        self.nome = nome
        self.tempo_mysql = []
        self.tempo_couchdb = []
        self.resultados_mysql = None
        self.resultados_couchdb = None
    
    def add_tempo_mysql(self, tempo: float):
        self.tempo_mysql.append(tempo)
    
    def add_tempo_couchdb(self, tempo: float):
        self.tempo_couchdb.append(tempo)
    
    def get_media_mysql(self) -> float:
        return statistics.mean(self.tempo_mysql) if self.tempo_mysql else 0
    
    def get_media_couchdb(self) -> float:
        return statistics.mean(self.tempo_couchdb) if self.tempo_couchdb else 0
    
    def get_desvio_mysql(self) -> float:
        return statistics.stdev(self.tempo_mysql) if len(self.tempo_mysql) > 1 else 0
    
    def get_desvio_couchdb(self) -> float:
        return statistics.stdev(self.tempo_couchdb) if len(self.tempo_couchdb) > 1 else 0
    
    def get_vencedor(self) -> str:
        if self.get_media_mysql() < self.get_media_couchdb():
            return "MySQL"
        elif self.get_media_couchdb() < self.get_media_mysql():
            return "CouchDB"
        else:
            return "Empate"
    
    def get_diferenca_percentual(self) -> float:
        mysql_avg = self.get_media_mysql()
        couch_avg = self.get_media_couchdb()
        
        if mysql_avg == 0 or couch_avg == 0:
            return 0
        
        menor = min(mysql_avg, couch_avg)
        maior = max(mysql_avg, couch_avg)
        
        return ((maior - menor) / menor) * 100


def conectar_mysql() -> pymysql.Connection:
    """Conecta ao MySQL."""
    try:
        conn = pymysql.connect(**MYSQL_CONFIG)
        print(f"✅ MySQL conectado")
        return conn
    except Exception as e:
        print(f"❌ Erro ao conectar ao MySQL: {e}")
        raise


def conectar_couchdb() -> requests.Session:
    """Conecta ao CouchDB."""
    try:
        session = requests.Session()
        session.auth = (COUCHDB_USER, COUCHDB_PASSWORD)
        
        response = session.get(COUCHDB_HOST)
        response.raise_for_status()
        
        print(f"✅ CouchDB conectado")
        return session
    except Exception as e:
        print(f"❌ Erro ao conectar ao CouchDB: {e}")
        raise


# =====================================================
# QUERY 1: Buscar filmes por ano
# =====================================================

def query1_mysql(conn: pymysql.Connection, ano: int) -> List[Dict]:
    """MySQL: Busca filmes de um ano específico."""
    query = """
        SELECT id_filme, titulo, nota_imdb, bilheteria_mundial
        FROM Filmes
        WHERE ano_lancamento = %s
        ORDER BY nota_imdb DESC
    """
    
    with conn.cursor(pymysql.cursors.DictCursor) as cursor:
        cursor.execute(query, (ano,))
        return cursor.fetchall()


def query1_couchdb(session: requests.Session, ano: int) -> List[Dict]:
    """CouchDB: Busca filmes de um ano específico."""
    url = f"{COUCHDB_HOST}/{COUCHDB_DATABASE}/_design/queries/_view/por_ano"
    params = {"key": ano}
    
    response = session.get(url, params=params)
    response.raise_for_status()
    
    result = response.json()
    return [row['value'] for row in result.get('rows', [])]


# =====================================================
# QUERY 2: Buscar filmes por ator
# =====================================================

def query2_mysql(conn: pymysql.Connection, ator: str) -> List[Dict]:
    """MySQL: Busca filmes de um ator (com JOINs)."""
    query = """
        SELECT DISTINCT f.id_filme, f.titulo, f.ano_lancamento, f.nota_imdb
        FROM Filmes f
        JOIN Filme_Estrela fe ON fe.id_filme = f.id_filme
        JOIN Pessoas p ON p.id_pessoa = fe.id_pessoa
        WHERE p.nome_pessoa = %s
        ORDER BY f.ano_lancamento DESC
    """
    
    with conn.cursor(pymysql.cursors.DictCursor) as cursor:
        cursor.execute(query, (ator,))
        return cursor.fetchall()


def query2_couchdb(session: requests.Session, ator: str) -> List[Dict]:
    """CouchDB: Busca filmes de um ator."""
    url = f"{COUCHDB_HOST}/{COUCHDB_DATABASE}/_design/queries/_view/por_ator"
    params = {"key": f'"{ator}"'}  # CouchDB precisa de aspas para strings
    
    response = session.get(url, params=params)
    response.raise_for_status()
    
    result = response.json()
    return [row['value'] for row in result.get('rows', [])]


# =====================================================
# QUERY 3: Buscar filmes por gênero
# =====================================================

def query3_mysql(conn: pymysql.Connection, genero: str) -> List[Dict]:
    """MySQL: Busca filmes de um gênero (com JOINs)."""
    query = """
        SELECT DISTINCT f.id_filme, f.titulo, f.ano_lancamento, f.nota_imdb
        FROM Filmes f
        JOIN Filme_Genero fg ON fg.id_filme = f.id_filme
        JOIN Generos g ON g.id_genero = fg.id_genero
        WHERE g.nome_genero = %s
        ORDER BY f.nota_imdb DESC
        LIMIT 20
    """
    
    with conn.cursor(pymysql.cursors.DictCursor) as cursor:
        cursor.execute(query, (genero,))
        return cursor.fetchall()


def query3_couchdb(session: requests.Session, genero: str) -> List[Dict]:
    """CouchDB: Busca filmes de um gênero."""
    url = f"{COUCHDB_HOST}/{COUCHDB_DATABASE}/_design/queries/_view/por_genero"
    params = {"key": f'"{genero}"', "limit": 20}
    
    response = session.get(url, params=params)
    response.raise_for_status()
    
    result = response.json()
    return [row['value'] for row in result.get('rows', [])]


# =====================================================
# QUERY 4: Top 10 filmes por bilheteria
# =====================================================

def query4_mysql(conn: pymysql.Connection) -> List[Dict]:
    """MySQL: Top 10 filmes por bilheteria."""
    query = """
        SELECT id_filme, titulo, ano_lancamento, bilheteria_mundial
        FROM Filmes
        WHERE bilheteria_mundial IS NOT NULL
        ORDER BY bilheteria_mundial DESC
        LIMIT 10
    """
    
    with conn.cursor(pymysql.cursors.DictCursor) as cursor:
        cursor.execute(query)
        return cursor.fetchall()


def query4_couchdb(session: requests.Session) -> List[Dict]:
    """CouchDB: Top 10 filmes por bilheteria."""
    url = f"{COUCHDB_HOST}/{COUCHDB_DATABASE}/_design/queries/_view/top_bilheteria"
    params = {"descending": "true", "limit": 10}
    
    response = session.get(url, params=params)
    response.raise_for_status()
    
    result = response.json()
    return [row['value'] for row in result.get('rows', [])]


# =====================================================
# QUERY 5: Agregação - Contar filmes por gênero
# =====================================================

def query5_mysql(conn: pymysql.Connection) -> List[Dict]:
    """MySQL: Conta filmes por gênero (GROUP BY)."""
    query = """
        SELECT g.nome_genero, COUNT(DISTINCT fg.id_filme) as total_filmes
        FROM Generos g
        LEFT JOIN Filme_Genero fg ON fg.id_genero = g.id_genero
        GROUP BY g.id_genero, g.nome_genero
        ORDER BY total_filmes DESC
        LIMIT 10
    """
    
    with conn.cursor(pymysql.cursors.DictCursor) as cursor:
        cursor.execute(query)
        return cursor.fetchall()


def query5_couchdb(session: requests.Session) -> List[Dict]:
    """CouchDB: Conta filmes por gênero (map/reduce)."""
    url = f"{COUCHDB_HOST}/{COUCHDB_DATABASE}/_design/statistics/_view/filmes_por_genero"
    params = {"group": "true"}
    
    response = session.get(url, params=params)
    response.raise_for_status()
    
    result = response.json()
    rows = result.get('rows', [])
    
    # Formata resultado similar ao MySQL
    return [{"nome_genero": row['key'], "total_filmes": row['value']} for row in rows[:10]]


# =====================================================
# Funções de Benchmark
# =====================================================

def executar_benchmark(
    nome: str,
    func_mysql,
    func_couchdb,
    *args
) -> BenchmarkResult:
    """Executa benchmark de uma query."""
    
    print(f"\n{'='*70}")
    print(f"📊 BENCHMARK: {nome}")
    print(f"{'='*70}")
    
    benchmark = BenchmarkResult(nome)
    
    # Conecta aos bancos
    conn_mysql = conectar_mysql()
    session_couch = conectar_couchdb()
    
    # Executa MySQL várias vezes
    print(f"\n🔵 Executando MySQL ({REPETICOES} vezes)...")
    for i in range(REPETICOES):
        inicio = time.perf_counter()
        resultado = func_mysql(conn_mysql, *args)
        fim = time.perf_counter()
        
        tempo = (fim - inicio) * 1000  # Converte para ms
        benchmark.add_tempo_mysql(tempo)
        
        if i == 0:  # Guarda resultado da primeira execução
            benchmark.resultados_mysql = resultado
        
        print(f"   Execução {i+1}: {tempo:.2f}ms")
    
    # Executa CouchDB várias vezes
    print(f"\n🟢 Executando CouchDB ({REPETICOES} vezes)...")
    for i in range(REPETICOES):
        inicio = time.perf_counter()
        resultado = func_couchdb(session_couch, *args)
        fim = time.perf_counter()
        
        tempo = (fim - inicio) * 1000  # Converte para ms
        benchmark.add_tempo_couchdb(tempo)
        
        if i == 0:  # Guarda resultado da primeira execução
            benchmark.resultados_couchdb = resultado
        
        print(f"   Execução {i+1}: {tempo:.2f}ms")
    
    # Fecha conexões
    conn_mysql.close()
    
    return benchmark


def imprimir_resultados(benchmarks: List[BenchmarkResult]):
    """Imprime resultados consolidados de todos os benchmarks."""
    
    print("\n" + "="*70)
    print("📈 RESULTADOS CONSOLIDADOS")
    print("="*70)
    
    for bench in benchmarks:
        print(f"\n{'─'*70}")
        print(f"🔍 {bench.nome}")
        print(f"{'─'*70}")
        
        mysql_avg = bench.get_media_mysql()
        mysql_std = bench.get_desvio_mysql()
        couch_avg = bench.get_media_couchdb()
        couch_std = bench.get_desvio_couchdb()
        
        print(f"🔵 MySQL:   {mysql_avg:8.2f}ms (±{mysql_std:.2f}ms)")
        print(f"🟢 CouchDB: {couch_avg:8.2f}ms (±{couch_std:.2f}ms)")
        
        vencedor = bench.get_vencedor()
        diferenca = bench.get_diferenca_percentual()
        
        if vencedor == "MySQL":
            print(f"🏆 Vencedor: MySQL ({diferenca:.1f}% mais rápido)")
        elif vencedor == "CouchDB":
            print(f"🏆 Vencedor: CouchDB ({diferenca:.1f}% mais rápido)")
        else:
            print(f"🤝 Empate técnico")
        
        # Mostra amostra de resultados
        print(f"\n📄 Amostra de resultados (primeiras 3 linhas):")
        
        print(f"   MySQL ({len(bench.resultados_mysql or [])} resultados):")
        for i, item in enumerate((bench.resultados_mysql or [])[:3], 1):
            print(f"      {i}. {item}")
        
        print(f"   CouchDB ({len(bench.resultados_couchdb or [])} resultados):")
        for i, item in enumerate((bench.resultados_couchdb or [])[:3], 1):
            print(f"      {i}. {item}")
    
    # Resumo geral
    print(f"\n{'='*70}")
    print("📊 RESUMO GERAL")
    print(f"{'='*70}")
    
    vitorias_mysql = sum(1 for b in benchmarks if b.get_vencedor() == "MySQL")
    vitorias_couch = sum(1 for b in benchmarks if b.get_vencedor() == "CouchDB")
    empates = sum(1 for b in benchmarks if b.get_vencedor() == "Empate")
    
    print(f"🔵 Vitórias MySQL:   {vitorias_mysql}")
    print(f"🟢 Vitórias CouchDB: {vitorias_couch}")
    print(f"🤝 Empates:          {empates}")
    
    # Calcula média geral de todas as queries
    tempo_total_mysql = sum(b.get_media_mysql() for b in benchmarks)
    tempo_total_couch = sum(b.get_media_couchdb() for b in benchmarks)
    
    print(f"\n⏱️  Tempo total médio:")
    print(f"   MySQL:   {tempo_total_mysql:.2f}ms")
    print(f"   CouchDB: {tempo_total_couch:.2f}ms")
    
    if tempo_total_mysql < tempo_total_couch:
        diff = ((tempo_total_couch - tempo_total_mysql) / tempo_total_mysql) * 100
        print(f"\n🏆 VENCEDOR GERAL: MySQL ({diff:.1f}% mais rápido no conjunto)")
    elif tempo_total_couch < tempo_total_mysql:
        diff = ((tempo_total_mysql - tempo_total_couch) / tempo_total_couch) * 100
        print(f"\n🏆 VENCEDOR GERAL: CouchDB ({diff:.1f}% mais rápido no conjunto)")
    else:
        print(f"\n🤝 Empate técnico no desempenho geral")


def main():
    """Função principal de benchmark."""
    
    print("="*70)
    print("🚀 BENCHMARK: MySQL vs CouchDB")
    print("="*70)
    print(f"📅 Data: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🔁 Repetições por query: {REPETICOES}")
    print()
    
    benchmarks = []
    
    try:
        # Benchmark 1: Filmes por ano
        bench1 = executar_benchmark(
            "Query 1: Buscar filmes de 2020",
            query1_mysql,
            query1_couchdb,
            2020
        )
        benchmarks.append(bench1)
        
        # Benchmark 2: Filmes por ator
        bench2 = executar_benchmark(
            "Query 2: Buscar filmes com 'Tom Hanks'",
            query2_mysql,
            query2_couchdb,
            "Tom Hanks"
        )
        benchmarks.append(bench2)
        
        # Benchmark 3: Filmes por gênero
        bench3 = executar_benchmark(
            "Query 3: Buscar filmes de 'Drama'",
            query3_mysql,
            query3_couchdb,
            "Drama"
        )
        benchmarks.append(bench3)
        
        # Benchmark 4: Top bilheteria
        bench4 = executar_benchmark(
            "Query 4: Top 10 filmes por bilheteria",
            query4_mysql,
            query4_couchdb
        )
        benchmarks.append(bench4)
        
        # Benchmark 5: Agregação
        bench5 = executar_benchmark(
            "Query 5: Contar filmes por gênero (agregação)",
            query5_mysql,
            query5_couchdb
        )
        benchmarks.append(bench5)
        
        # Imprime resultados consolidados
        imprimir_resultados(benchmarks)
        
        print("\n" + "="*70)
        print("✅ BENCHMARK CONCLUÍDO COM SUCESSO!")
        print("="*70)
        print()
        
    except Exception as e:
        print(f"\n❌ ERRO durante benchmark: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())
