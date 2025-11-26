#!/usr/bin/env python3
"""
Exemplos de consultas e operações com CouchDB
Demonstra como usar o modelo de dados migrado
"""

import requests
import json
from datetime import datetime
from typing import List, Dict, Any

class CouchDBFilmesAPI:
    def __init__(self, couchdb_url: str = "http://localhost:5984", db_name: str = "filmes_db"):
        self.base_url = f"{couchdb_url}/{db_name}"
        self.couchdb_url = couchdb_url
        
    def get_filme_by_id(self, filme_id: str) -> Dict:
        """Busca filme por ID"""
        response = requests.get(f"{self.base_url}/{filme_id}")
        if response.status_code == 200:
            return response.json()
        return {}
    
    def search_filmes_by_title(self, titulo: str) -> List[Dict]:
        """Busca filmes por título (usando regex)"""
        query = {
            "selector": {
                "type": "filme",
                "titulo": {"$regex": f"(?i){titulo}"}
            },
            "fields": ["_id", "titulo", "ano_lancamento", "nota_imdb", "generos"]
        }
        
        response = requests.post(
            f"{self.base_url}/_find",
            json=query,
            headers={"Content-Type": "application/json"}
        )
        
        if response.status_code == 200:
            return response.json().get("docs", [])
        return []
    
    def get_filmes_by_year(self, ano: int) -> List[Dict]:
        """Busca filmes por ano usando view"""
        response = requests.get(f"{self.base_url}/_design/filmes/_view/por_ano?key={ano}")
        if response.status_code == 200:
            data = response.json()
            return [row["value"] for row in data.get("rows", [])]
        return []
    
    def get_filmes_by_genre(self, genero: str) -> List[Dict]:
        """Busca filmes por gênero usando view"""
        response = requests.get(f'{self.base_url}/_design/filmes/_view/por_genero?key="{genero}"')
        if response.status_code == 200:
            data = response.json()
            return [row["value"] for row in data.get("rows", [])]
        return []
    
    def get_top_rated_movies(self, limit: int = 10) -> List[Dict]:
        """Busca filmes mais bem avaliados"""
        query = {
            "selector": {
                "type": "filme",
                "nota_imdb": {"$gte": 8.0}
            },
            "sort": [{"nota_imdb": "desc"}],
            "limit": limit,
            "fields": ["titulo", "ano_lancamento", "nota_imdb", "generos", "pessoas.diretores"]
        }
        
        response = requests.post(
            f"{self.base_url}/_find",
            json=query,
            headers={"Content-Type": "application/json"}
        )
        
        if response.status_code == 200:
            return response.json().get("docs", [])
        return []
    
    def get_pessoa_filmografia(self, pessoa_id: str) -> Dict:
        """Busca filmografia completa de uma pessoa"""
        response = requests.get(f"{self.base_url}/{pessoa_id}")
        if response.status_code == 200:
            return response.json()
        return {}
    
    def search_pessoas_by_name(self, nome: str) -> List[Dict]:
        """Busca pessoas por nome"""
        query = {
            "selector": {
                "type": "pessoa",
                "nome": {"$regex": f"(?i){nome}"}
            },
            "fields": ["_id", "nome", "estatisticas"]
        }
        
        response = requests.post(
            f"{self.base_url}/_find",
            json=query,
            headers={"Content-Type": "application/json"}
        )
        
        if response.status_code == 200:
            return response.json().get("docs", [])
        return []
    
    def get_movies_by_decade(self, decada: int) -> List[Dict]:
        """Busca filmes por década (ex: 1990, 2000, 2010)"""
        start_year = decada
        end_year = decada + 9
        
        query = {
            "selector": {
                "type": "filme",
                "ano_lancamento": {
                    "$gte": start_year,
                    "$lte": end_year
                }
            },
            "sort": [{"nota_imdb": "desc"}],
            "fields": ["titulo", "ano_lancamento", "nota_imdb", "generos"]
        }
        
        response = requests.post(
            f"{self.base_url}/_find",
            json=query,
            headers={"Content-Type": "application/json"}
        )
        
        if response.status_code == 200:
            return response.json().get("docs", [])
        return []
    
    def get_box_office_leaders(self, min_bilheteria: float = 100000000) -> List[Dict]:
        """Busca filmes com maior bilheteria"""
        query = {
            "selector": {
                "type": "filme",
                "financeiro.bilheteria.mundial": {"$gte": min_bilheteria}
            },
            "sort": [{"financeiro.bilheteria.mundial": "desc"}],
            "fields": ["titulo", "ano_lancamento", "financeiro.bilheteria", "financeiro.orcamento"]
        }
        
        response = requests.post(
            f"{self.base_url}/_find",
            json=query,
            headers={"Content-Type": "application/json"}
        )
        
        if response.status_code == 200:
            return response.json().get("docs", [])
        return []
    
    def create_genre_statistics(self) -> Dict:
        """Cria estatísticas por gênero"""
        # Usar view para agrupar por gênero
        response = requests.get(f"{self.base_url}/_design/filmes/_view/por_genero?group=true")
        
        if response.status_code == 200:
            data = response.json()
            stats = {}
            
            for row in data.get("rows", []):
                genero = row["key"]
                count = row["value"]
                stats[genero] = {"quantidade": count}
            
            return stats
        return {}
    
    def create_backup(self, backup_db: str = "filmes_db_backup"):
        """Cria backup via replicação"""
        replication_data = {
            "source": "filmes_db",
            "target": backup_db,
            "create_target": True
        }
        
        response = requests.post(
            f"{self.couchdb_url}/_replicate",
            json=replication_data,
            headers={"Content-Type": "application/json"}
        )
        
        return response.status_code == 200


def exemplos_uso():
    """Exemplos de uso da API"""
    api = CouchDBFilmesAPI()
    
    print("🎬 Exemplos de Consultas CouchDB - Sistema de Filmes")
    print("=" * 60)
    
    # 1. Buscar filme específico
    print("\n1. Buscar filme específico:")
    filme = api.get_filme_by_id("filme:tt0111161")
    if filme:
        print(f"   Título: {filme.get('titulo')}")
        print(f"   Ano: {filme.get('ano_lancamento')}")
        print(f"   Nota IMDB: {filme.get('nota_imdb')}")
    
    # 2. Buscar por título
    print("\n2. Buscar filmes por título 'Matrix':")
    filmes = api.search_filmes_by_title("Matrix")
    for filme in filmes[:3]:  # Primeiros 3
        print(f"   - {filme.get('titulo')} ({filme.get('ano_lancamento')})")
    
    # 3. Filmes de 1994
    print("\n3. Filmes de 1994:")
    filmes_1994 = api.get_filmes_by_year(1994)
    for filme in filmes_1994[:5]:  # Primeiros 5
        print(f"   - {filme.get('titulo')} - Nota: {filme.get('nota_imdb')}")
    
    # 4. Filmes de Drama
    print("\n4. Filmes de Drama (primeiros 5):")
    dramas = api.get_filmes_by_genre("Drama")
    for filme in dramas[:5]:
        print(f"   - {filme.get('titulo')} ({filme.get('ano')})")
    
    # 5. Top filmes
    print("\n5. Top 5 filmes mais bem avaliados:")
    top_filmes = api.get_top_rated_movies(5)
    for filme in top_filmes:
        print(f"   - {filme.get('titulo')} - Nota: {filme.get('nota_imdb')}")
    
    # 6. Buscar pessoa
    print("\n6. Buscar pessoa 'Morgan Freeman':")
    pessoas = api.search_pessoas_by_name("Morgan Freeman")
    for pessoa in pessoas:
        stats = pessoa.get('estatisticas', {})
        print(f"   - {pessoa.get('nome')}: {stats.get('total_filmes')} filmes")
    
    # 7. Filmes dos anos 90
    print("\n7. Filmes dos anos 90 (primeiros 5):")
    filmes_90s = api.get_movies_by_decade(1990)
    for filme in filmes_90s[:5]:
        print(f"   - {filme.get('titulo')} ({filme.get('ano_lancamento')}) - {filme.get('nota_imdb')}")
    
    # 8. Maiores bilheterias
    print("\n8. Maiores bilheterias (>$500M):")
    blockbusters = api.get_box_office_leaders(500000000)
    for filme in blockbusters[:5]:
        bilheteria = filme.get('financeiro', {}).get('bilheteria', {})
        mundial = bilheteria.get('mundial', 0)
        print(f"   - {filme.get('titulo')}: ${mundial:,.0f}")
    
    # 9. Estatísticas por gênero
    print("\n9. Estatísticas por gênero:")
    stats = api.create_genre_statistics()
    sorted_genres = sorted(stats.items(), key=lambda x: x[1]['quantidade'], reverse=True)
    for genero, data in sorted_genres[:5]:
        print(f"   - {genero}: {data['quantidade']} filmes")


def create_sample_queries():
    """Cria arquivo com queries de exemplo"""
    queries = {
        "consultas_basicas": {
            "filme_por_id": {
                "url": "GET /filmes_db/filme:tt0111161",
                "descrição": "Busca filme específico por ID"
            },
            "todos_filmes": {
                "url": "GET /filmes_db/_all_docs?include_docs=true&startkey=\"filme:\"&endkey=\"filme:\\ufff0\"",
                "descrição": "Lista todos os filmes"
            }
        },
        "consultas_com_views": {
            "filmes_por_ano": {
                "url": "GET /filmes_db/_design/filmes/_view/por_ano?key=1994",
                "descrição": "Filmes de um ano específico"
            },
            "filmes_por_genero": {
                "url": "GET /filmes_db/_design/filmes/_view/por_genero?key=\"Drama\"",
                "descrição": "Filmes de um gênero específico"
            }
        },
        "consultas_mango": {
            "top_rated": {
                "method": "POST",
                "url": "/filmes_db/_find",
                "body": {
                    "selector": {"type": "filme", "nota_imdb": {"$gte": 8.0}},
                    "sort": [{"nota_imdb": "desc"}],
                    "limit": 10
                },
                "descrição": "Top 10 filmes mais bem avaliados"
            },
            "busca_titulo": {
                "method": "POST",
                "url": "/filmes_db/_find",
                "body": {
                    "selector": {
                        "type": "filme",
                        "titulo": {"$regex": "(?i)matrix"}
                    }
                },
                "descrição": "Busca por título (case insensitive)"
            },
            "filmes_decada": {
                "method": "POST",
                "url": "/filmes_db/_find",
                "body": {
                    "selector": {
                        "type": "filme",
                        "ano_lancamento": {"$gte": 2000, "$lt": 2010}
                    },
                    "sort": [{"nota_imdb": "desc"}]
                },
                "descrição": "Filmes de uma década específica"
            }
        },
        "indices_recomendados": [
            {
                "index": {"fields": ["type", "titulo"]},
                "name": "titulo-index"
            },
            {
                "index": {"fields": ["type", "ano_lancamento", "nota_imdb"]},
                "name": "ano-nota-index"
            },
            {
                "index": {"fields": ["type", "generos"]},
                "name": "generos-index"
            }
        ]
    }
    
    with open("d:\\Repositorios\\modelagem-de-dados\\consultas_couchdb.json", "w", encoding="utf-8") as f:
        json.dump(queries, f, indent=2, ensure_ascii=False)
    
    print("✓ Arquivo de consultas criado: consultas_couchdb.json")


if __name__ == "__main__":
    print("Escolha uma opção:")
    print("1. Executar exemplos de consultas")
    print("2. Criar arquivo de consultas de exemplo")
    print("3. Ambos")
    
    choice = input("Digite sua escolha (1-3): ").strip()
    
    if choice in ["1", "3"]:
        exemplos_uso()
    
    if choice in ["2", "3"]:
        create_sample_queries()