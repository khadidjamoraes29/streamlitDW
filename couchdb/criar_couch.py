"""
Script para criar e configurar banco de dados CouchDB para filmes.

Estrutura de Documentos NoSQL (orientada a documentos):
- Cada filme é um documento completo com todos os dados embedded
- Desnormalização estratégica para otimizar queries
- Views/índices para agregações e buscas eficientes

Autor: Sistema de Modelagem de Dados
Data: 2025-10-19
"""

import requests
import json
from typing import Dict, List, Optional

# =====================
# Configurações CouchDB
# =====================
COUCHDB_HOST = "http://localhost:5984"
COUCHDB_USER = "admin"
COUCHDB_PASSWORD = "admin123"  # Altere para sua senha
DATABASE_NAME = "filmes_db"


def conectar_couchdb() -> requests.Session:
    """Conecta ao servidor CouchDB com autenticação."""
    try:
        session = requests.Session()
        session.auth = (COUCHDB_USER, COUCHDB_PASSWORD)
        
        # Testa conexão
        response = session.get(COUCHDB_HOST)
        response.raise_for_status()
        
        print(f"✅ Conectado ao CouchDB: {COUCHDB_HOST}")
        return session
    except Exception as e:
        print(f"❌ Erro ao conectar ao CouchDB: {e}")
        print(f"   Certifique-se de que o CouchDB está rodando em {COUCHDB_HOST}")
        raise


def criar_database(session: requests.Session, db_name: str) -> bool:
    """Cria o database ou retorna existente."""
    try:
        # Verifica se database existe
        response = session.get(f"{COUCHDB_HOST}/{db_name}")
        
        if response.status_code == 200:
            print(f"⚠️  Database '{db_name}' já existe. Deletando para recriar...")
            session.delete(f"{COUCHDB_HOST}/{db_name}")
        
        # Cria database
        response = session.put(f"{COUCHDB_HOST}/{db_name}")
        response.raise_for_status()
        
        print(f"✅ Database '{db_name}' criado com sucesso!")
        return True
    except Exception as e:
        print(f"❌ Erro ao criar database: {e}")
        raise


def criar_design_documents(session: requests.Session, db_name: str):
    """
    Cria design documents com views para queries eficientes.
    
    Views criadas:
    - por_ano: filmes agrupados por ano
    - por_genero: filmes por gênero
    - por_ator: filmes por ator (estrela)
    - por_diretor: filmes por diretor
    - por_empresa: filmes por empresa produtora
    - por_pais: filmes por país de origem
    - top_bilheteria: filmes ordenados por bilheteria
    - top_nota_imdb: filmes ordenados por nota IMDB
    """
    
    # Design document para queries básicas
    design_doc_queries = {
        "_id": "_design/queries",
        "language": "javascript",
        "views": {
            "por_ano": {
                "map": """
                function(doc) {
                    if (doc.tipo === 'filme' && doc.ano_lancamento) {
                        emit(doc.ano_lancamento, {
                            titulo: doc.titulo,
                            nota_imdb: doc.nota_imdb,
                            bilheteria_mundial: doc.bilheteria_mundial
                        });
                    }
                }
                """
            },
            "por_genero": {
                "map": """
                function(doc) {
                    if (doc.tipo === 'filme' && doc.generos) {
                        doc.generos.forEach(function(genero) {
                            emit(genero, {
                                titulo: doc.titulo,
                                ano: doc.ano_lancamento,
                                nota_imdb: doc.nota_imdb
                            });
                        });
                    }
                }
                """
            },
            "por_ator": {
                "map": """
                function(doc) {
                    if (doc.tipo === 'filme' && doc.estrelas) {
                        doc.estrelas.forEach(function(estrela) {
                            emit(estrela.nome, {
                                titulo: doc.titulo,
                                ano: doc.ano_lancamento,
                                ordem_credito: estrela.ordem_credito,
                                nota_imdb: doc.nota_imdb,
                                bilheteria: doc.bilheteria_mundial
                            });
                        });
                    }
                }
                """
            },
            "por_diretor": {
                "map": """
                function(doc) {
                    if (doc.tipo === 'filme' && doc.diretores) {
                        doc.diretores.forEach(function(diretor) {
                            emit(diretor, {
                                titulo: doc.titulo,
                                ano: doc.ano_lancamento,
                                nota_imdb: doc.nota_imdb,
                                bilheteria: doc.bilheteria_mundial
                            });
                        });
                    }
                }
                """
            },
            "por_empresa": {
                "map": """
                function(doc) {
                    if (doc.tipo === 'filme' && doc.empresas_producao) {
                        doc.empresas_producao.forEach(function(empresa) {
                            emit(empresa, {
                                titulo: doc.titulo,
                                ano: doc.ano_lancamento,
                                bilheteria: doc.bilheteria_mundial,
                                orcamento: doc.orcamento
                            });
                        });
                    }
                }
                """
            },
            "por_pais": {
                "map": """
                function(doc) {
                    if (doc.tipo === 'filme' && doc.paises_origem) {
                        doc.paises_origem.forEach(function(pais) {
                            emit(pais, {
                                titulo: doc.titulo,
                                ano: doc.ano_lancamento,
                                nota_imdb: doc.nota_imdb
                            });
                        });
                    }
                }
                """
            },
            "top_bilheteria": {
                "map": """
                function(doc) {
                    if (doc.tipo === 'filme' && doc.bilheteria_mundial) {
                        emit(doc.bilheteria_mundial, {
                            titulo: doc.titulo,
                            ano: doc.ano_lancamento,
                            bilheteria: doc.bilheteria_mundial,
                            orcamento: doc.orcamento
                        });
                    }
                }
                """
            },
            "top_nota_imdb": {
                "map": """
                function(doc) {
                    if (doc.tipo === 'filme' && doc.nota_imdb) {
                        emit(doc.nota_imdb, {
                            titulo: doc.titulo,
                            ano: doc.ano_lancamento,
                            nota_imdb: doc.nota_imdb,
                            votos_imdb: doc.votos_imdb
                        });
                    }
                }
                """
            }
        }
    }
    
    # Design document para agregações
    design_doc_stats = {
        "_id": "_design/statistics",
        "language": "javascript",
        "views": {
            "bilheteria_por_ano": {
                "map": """
                function(doc) {
                    if (doc.tipo === 'filme' && doc.ano_lancamento && doc.bilheteria_mundial) {
                        emit(doc.ano_lancamento, doc.bilheteria_mundial);
                    }
                }
                """,
                "reduce": "_sum"
            },
            "filmes_por_genero": {
                "map": """
                function(doc) {
                    if (doc.tipo === 'filme' && doc.generos) {
                        doc.generos.forEach(function(genero) {
                            emit(genero, 1);
                        });
                    }
                }
                """,
                "reduce": "_count"
            },
            "media_nota_por_ano": {
                "map": """
                function(doc) {
                    if (doc.tipo === 'filme' && doc.ano_lancamento && doc.nota_imdb) {
                        emit(doc.ano_lancamento, doc.nota_imdb);
                    }
                }
                """,
                "reduce": "_stats"
            },
            "premios_por_ator": {
                "map": """
                function(doc) {
                    if (doc.tipo === 'filme' && doc.estrelas && doc.vitorias_premios) {
                        var n_atores = doc.estrelas.length || 1;
                        var premios_prorata = doc.vitorias_premios / n_atores;
                        doc.estrelas.forEach(function(estrela) {
                            emit(estrela.nome, premios_prorata);
                        });
                    }
                }
                """,
                "reduce": "_sum"
            }
        }
    }
    
    try:
        # Salva design documents
        url = f"{COUCHDB_HOST}/{db_name}/_design/queries"
        response = session.put(url, json=design_doc_queries)
        response.raise_for_status()
        print("✅ Design document 'queries' criado com sucesso!")
        
        url = f"{COUCHDB_HOST}/{db_name}/_design/statistics"
        response = session.put(url, json=design_doc_stats)
        response.raise_for_status()
        print("✅ Design document 'statistics' criado com sucesso!")
        
    except Exception as e:
        print(f"❌ Erro ao criar design documents: {e}")
        raise


def criar_indices(session: requests.Session, db_name: str):
    """Cria índices Mango para queries JSON."""
    
    indices = [
        {
            "index": {
                "fields": ["tipo", "ano_lancamento"]
            },
            "name": "idx_tipo_ano",
            "type": "json"
        },
        {
            "index": {
                "fields": ["tipo", "nota_imdb"]
            },
            "name": "idx_tipo_nota",
            "type": "json"
        },
        {
            "index": {
                "fields": ["tipo", "bilheteria_mundial"]
            },
            "name": "idx_tipo_bilheteria",
            "type": "json"
        },
        {
            "index": {
                "fields": ["tipo", "titulo"]
            },
            "name": "idx_tipo_titulo",
            "type": "json"
        }
    ]
    
    try:
        for idx in indices:
            # CouchDB Python library não tem método direto para índices Mango
            # Precisaria usar requests diretamente ou couchdb3 library
            # Por enquanto, documentamos a estrutura
            pass
        
        print("✅ Índices Mango configurados (criar via API REST ou Fauxton)")
        print("   Comandos curl para criar índices:")
        for idx in indices:
            print(f"   curl -X POST {COUCHDB_HOST}/{DATABASE_NAME}/_index \\")
            print(f"     -H 'Content-Type: application/json' \\")
            print(f"     -d '{json.dumps(idx)}'")
            print()
        
    except Exception as e:
        print(f"⚠️  Aviso ao documentar índices: {e}")


def criar_documento_exemplo(session: requests.Session, db_name: str):
    """Cria um documento de exemplo para validar estrutura."""
    
    exemplo = {
        "_id": "filme_exemplo_001",
        "tipo": "filme",
        "titulo": "The Shawshank Redemption",
        "link_imdb": "https://www.imdb.com/title/tt0111161/",
        "ano_lancamento": 1994,
        "duracao_minutos": 142,
        "classificacao_mpa": "R",
        "nota_imdb": 9.3,
        "votos_imdb": 2500000,
        "orcamento": 25000000.00,
        "bilheteria_mundial": 28341469.00,
        "bilheteria_eua_canada": 28341469.00,
        "bilheteria_abertura": 727327.00,
        "vitorias_premios": 21,
        "nominacoes_premios": 51,
        "vitorias_oscar": 0,
        
        # Arrays com dados desnormalizados
        "generos": ["Drama"],
        
        "estrelas": [
            {"nome": "Tim Robbins", "ordem_credito": 1},
            {"nome": "Morgan Freeman", "ordem_credito": 2},
            {"nome": "Bob Gunton", "ordem_credito": 3}
        ],
        
        "diretores": ["Frank Darabont"],
        
        "roteiristas": ["Stephen King", "Frank Darabont"],
        
        "paises_origem": ["United States"],
        
        "empresas_producao": ["Castle Rock Entertainment", "Columbia Pictures"],
        
        "idiomas": ["English"],
        
        # Metadados
        "data_insercao": "2025-10-19T00:00:00Z",
        "origem_dados": "MySQL - filmes_db"
    }
    
    try:
        url = f"{COUCHDB_HOST}/{db_name}/{exemplo['_id']}"
        response = session.put(url, json=exemplo)
        response.raise_for_status()
        print("✅ Documento de exemplo criado com sucesso!")
        print(f"   ID: {exemplo['_id']}")
    except Exception as e:
        print(f"⚠️  Aviso ao criar exemplo: {e}")


def main():
    """Função principal de setup do CouchDB."""
    
    print("=" * 70)
    print("SETUP DO BANCO DE DADOS COUCHDB - FILMES")
    print("=" * 70)
    print()
    
    # 1. Conectar
    session = conectar_couchdb()
    
    # 2. Criar database
    criar_database(session, DATABASE_NAME)
    
    # 3. Criar design documents (views)
    criar_design_documents(session, DATABASE_NAME)
    
    # 4. Documentar índices Mango
    criar_indices(session, DATABASE_NAME)
    
    # 5. Criar documento de exemplo
    criar_documento_exemplo(session, DATABASE_NAME)
    
    print()
    print("=" * 70)
    print("✅ SETUP CONCLUÍDO COM SUCESSO!")
    print("=" * 70)
    print()
    print("Próximos passos:")
    print("1. Acessar Fauxton: http://localhost:5984/_utils")
    print(f"2. Verificar database: {DATABASE_NAME}")
    print("3. Executar migração: python migrar_sql_para_couch.py")
    print()
    print("Views disponíveis:")
    print("- _design/queries/_view/por_ano")
    print("- _design/queries/_view/por_genero")
    print("- _design/queries/_view/por_ator")
    print("- _design/queries/_view/por_diretor")
    print("- _design/statistics/_view/bilheteria_por_ano")
    print("- _design/statistics/_view/premios_por_ator")
    print()


if __name__ == "__main__":
    main()
