#!/usr/bin/env python3
"""
Script para migração de dados SQL para CouchDB
Converte dados relacionais em documentos JSON otimizados para CouchDB
"""

import json
import requests
import uuid
from datetime import datetime
from typing import Dict, List, Any
import re
import pymysql
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from decimal import Decimal

class SQLToCouchDBMigrator:
    def __init__(self, mysql_config: Dict[str, Any], couchdb_url: str, couchdb_db: str):
        self.mysql_config = mysql_config
        self.couchdb_url = couchdb_url
        self.couchdb_db = couchdb_db
        self.base_url = f"{couchdb_url}/{couchdb_db}"
        self.auth = None  # Será definido se necessário
        
        # Criar engine MySQL
        user = mysql_config['user']
        password = quote_plus(mysql_config['password'])
        host = mysql_config['host']
        port = mysql_config['port']
        database = mysql_config['database']
        
        self.engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}")
        
    def convert_decimals(self, obj):
        """Converte objetos Decimal para float recursivamente"""
        if isinstance(obj, dict):
            return {key: self.convert_decimals(value) for key, value in obj.items()}
        elif isinstance(obj, list):
            return [self.convert_decimals(item) for item in obj]
        elif isinstance(obj, Decimal):
            return float(obj) if obj is not None else None
        else:
            return obj
        
    def normalize_string(self, text: str) -> str:
        """Normaliza string para uso como ID"""
        if not text:
            return ""
        return re.sub(r'[^a-z0-9_]', '_', text.lower().strip())
    
    def create_database(self):
        """Cria o banco de dados CouchDB se não existir"""
        print(f"🔨 Tentando criar/verificar banco CouchDB: {self.couchdb_db}")
        try:
            response = requests.put(self.base_url, timeout=10)
            print(f"   Status HTTP: {response.status_code}")
            
            if response.status_code == 401:
                print("   ⚠️ CouchDB requer autenticação. Tentando com credenciais padrão...")
                # Tentar com credenciais padrão
                self.auth = ('admin', 'admin123')
                response = requests.put(self.base_url, auth=self.auth, timeout=10)
                print(f"   Status HTTP (com auth): {response.status_code}")
            
            if response.status_code in [201, 412]:  # Created ou Already exists
                print(f"✓ Banco {self.couchdb_db} criado/verificado")
                return True
            else:
                print(f"✗ Status inesperado: {response.status_code} - {response.text}")
                return False
        except requests.exceptions.Timeout:
            print("✗ Timeout na conexão com CouchDB")
            return False
        except Exception as e:
            print(f"✗ Erro ao criar banco: {e}")
            return False
    
    def get_sql_data(self) -> Dict[str, List[Dict]]:
        """Extrai dados do banco MySQL"""
        data = {}
        
        # Extrair dados das tabelas principais
        tables = [
            'Filmes', 'Pessoas', 'Generos', 'Paises', 
            'Empresas', 'Idiomas'
        ]
        
        with self.engine.connect() as conn:
            for table in tables:
                try:
                    result = conn.execute(text(f"SELECT * FROM {table}"))
                    # Converter rows para dicionários e converter Decimals
                    columns = result.keys()
                    raw_data = [dict(zip(columns, row)) for row in result.fetchall()]
                    data[table.lower()] = [self.convert_decimals(row) for row in raw_data]
                    print(f"✓ Tabela {table}: {len(data[table.lower()])} registros")
                except Exception as e:
                    print(f"⚠️ Erro ao ler tabela {table}: {e}")
                    data[table.lower()] = []
            
            # Extrair dados das tabelas associativas
            associative_tables = [
                'Filme_Estrela', 'Filme_Diretor', 'Filme_Roteirista',
                'Filme_Genero', 'Filme_Pais_Origem', 'Filme_Empresa_Producao',
                'Filme_Idioma'
            ]
            
            for table in associative_tables:
                try:
                    result = conn.execute(text(f"SELECT * FROM {table}"))
                    columns = result.keys()
                    raw_data = [dict(zip(columns, row)) for row in result.fetchall()]
                    data[table.lower()] = [self.convert_decimals(row) for row in raw_data]
                    print(f"✓ Tabela {table}: {len(data[table.lower()])} registros")
                except Exception as e:
                    print(f"⚠️ Tabela {table} não encontrada ou erro: {e}")
                    data[table.lower()] = []
        
        return data
    
    def build_filme_documents(self, sql_data: Dict) -> List[Dict]:
        """Constrói documentos de filme desnormalizados"""
        documentos = []
        
        # Criar mapas de lookup
        pessoas_map = {p['id_pessoa']: p for p in sql_data['pessoas']}
        generos_map = {g['id_genero']: g for g in sql_data['generos']}
        paises_map = {p['id_pais']: p for p in sql_data['paises']}
        empresas_map = {e['id_empresa']: e for e in sql_data['empresas']}
        idiomas_map = {i['id_idioma']: i for i in sql_data['idiomas']}
        
        for filme in sql_data['filmes']:
            filme_id = filme['id_filme']
            
            # Extrair link IMDB para usar como ID
            imdb_id = None
            if filme.get('link_imdb'):
                match = re.search(r'tt\d+', filme['link_imdb'])
                if match:
                    imdb_id = match.group()
            
            doc_id = f"filme:{imdb_id}" if imdb_id else f"filme:{uuid.uuid4()}"
            
            # Construir documento do filme
            documento = {
                "_id": doc_id,
                "type": "filme",
                "titulo": filme['titulo'],
                "link_imdb": filme.get('link_imdb'),
                "ano_lancamento": filme.get('ano_lancamento'),
                "duracao_minutos": filme.get('duracao_minutos'),
                "classificacao_mpa": filme.get('classificacao_mpa'),
                "nota_imdb": filme.get('nota_imdb'),
                "votos_imdb": filme.get('votos_imdb'),
                "financeiro": {
                    "orcamento": filme.get('orcamento'),
                    "bilheteria": {
                        "mundial": filme.get('bilheteria_mundial'),
                        "eua_canada": filme.get('bilheteria_eua_canada'),
                        "abertura": filme.get('bilheteria_abertura')
                    }
                },
                "premios": {
                    "vitorias": filme.get('vitorias_premios'),
                    "nominacoes": filme.get('nominacoes_premios'),
                    "oscars": filme.get('vitorias_oscar')
                },
                "pessoas": {
                    "diretores": [],
                    "atores_principais": [],
                    "roteiristas": []
                },
                "generos": [],
                "paises_origem": [],
                "empresas_producao": [],
                "idiomas": [],
                "created_at": datetime.now().isoformat(),
                "updated_at": datetime.now().isoformat()
            }
            
            # Adicionar diretores
            for rel in sql_data.get('filme_diretor', []):
                if rel['id_filme'] == filme_id:
                    pessoa = pessoas_map.get(rel['id_pessoa'])
                    if pessoa:
                        documento['pessoas']['diretores'].append({
                            "nome": pessoa['nome_pessoa'],
                            "id_pessoa": f"pessoa:{self.normalize_string(pessoa['nome_pessoa'])}"
                        })
            
            # Adicionar atores principais
            for rel in sql_data.get('filme_estrela', []):
                if rel['id_filme'] == filme_id:
                    pessoa = pessoas_map.get(rel['id_pessoa'])
                    if pessoa:
                        documento['pessoas']['atores_principais'].append({
                            "nome": pessoa['nome_pessoa'],
                            "id_pessoa": f"pessoa:{self.normalize_string(pessoa['nome_pessoa'])}",
                            "ordem_credito": rel.get('ordem_credito')
                        })
            
            # Adicionar roteiristas
            for rel in sql_data.get('filme_roteirista', []):
                if rel['id_filme'] == filme_id:
                    pessoa = pessoas_map.get(rel['id_pessoa'])
                    if pessoa:
                        documento['pessoas']['roteiristas'].append({
                            "nome": pessoa['nome_pessoa'],
                            "id_pessoa": f"pessoa:{self.normalize_string(pessoa['nome_pessoa'])}"
                        })
            
            # Adicionar gêneros
            for rel in sql_data.get('filme_genero', []):
                if rel['id_filme'] == filme_id:
                    genero = generos_map.get(rel['id_genero'])
                    if genero:
                        documento['generos'].append(genero['nome_genero'])
            
            # Adicionar países
            for rel in sql_data.get('filme_pais_origem', []):
                if rel['id_filme'] == filme_id:
                    pais = paises_map.get(rel['id_pais'])
                    if pais:
                        documento['paises_origem'].append(pais['nome_pais'])
            
            # Adicionar empresas
            for rel in sql_data.get('filme_empresa_producao', []):
                if rel['id_filme'] == filme_id:
                    empresa = empresas_map.get(rel['id_empresa'])
                    if empresa:
                        documento['empresas_producao'].append(empresa['nome_empresa'])
            
            # Adicionar idiomas
            for rel in sql_data.get('filme_idioma', []):
                if rel['id_filme'] == filme_id:
                    idioma = idiomas_map.get(rel['id_idioma'])
                    if idioma:
                        documento['idiomas'].append(idioma['nome_idioma'])
            
            documentos.append(documento)
        
        # Converter todos os Decimals para float
        documentos = [self.convert_decimals(doc) for doc in documentos]
        return documentos
    
    def build_pessoa_documents(self, sql_data: Dict, filme_docs: List[Dict]) -> List[Dict]:
        """Constrói documentos agregados de pessoas"""
        documentos = []
        
        # Criar mapa de filmes por ID
        filmes_map = {}
        for filme_doc in filme_docs:
            # Extrair ID original do filme
            for filme_sql in sql_data['filmes']:
                if filme_doc['titulo'] == filme_sql['titulo']:
                    filmes_map[filme_sql['id_filme']] = filme_doc
                    break
        
        for pessoa in sql_data['pessoas']:
            pessoa_id = f"pessoa:{self.normalize_string(pessoa['nome_pessoa'])}"
            
            documento = {
                "_id": pessoa_id,
                "type": "pessoa",
                "nome": pessoa['nome_pessoa'],
                "nome_normalizado": self.normalize_string(pessoa['nome_pessoa']),
                "participacoes": {
                    "como_diretor": [],
                    "como_ator": [],
                    "como_roteirista": []
                },
                "estatisticas": {
                    "total_filmes": 0,
                    "anos_ativo": {
                        "inicio": None,
                        "fim": None
                    }
                },
                "created_at": datetime.now().isoformat(),
                "updated_at": datetime.now().isoformat()
            }
            
            anos = []
            
            # Participações como diretor
            for rel in sql_data.get('filme_diretor', []):
                if rel['id_pessoa'] == pessoa['id_pessoa']:
                    filme = filmes_map.get(rel['id_filme'])
                    if filme:
                        documento['participacoes']['como_diretor'].append({
                            "filme_id": filme['_id'],
                            "titulo_filme": filme['titulo'],
                            "ano": filme['ano_lancamento']
                        })
                        if filme['ano_lancamento']:
                            anos.append(filme['ano_lancamento'])
            
            # Participações como ator
            for rel in sql_data.get('filme_estrela', []):
                if rel['id_pessoa'] == pessoa['id_pessoa']:
                    filme = filmes_map.get(rel['id_filme'])
                    if filme:
                        documento['participacoes']['como_ator'].append({
                            "filme_id": filme['_id'],
                            "titulo_filme": filme['titulo'],
                            "ano": filme['ano_lancamento'],
                            "ordem_credito": rel.get('ordem_credito')
                        })
                        if filme['ano_lancamento']:
                            anos.append(filme['ano_lancamento'])
            
            # Participações como roteirista
            for rel in sql_data.get('filme_roteirista', []):
                if rel['id_pessoa'] == pessoa['id_pessoa']:
                    filme = filmes_map.get(rel['id_filme'])
                    if filme:
                        documento['participacoes']['como_roteirista'].append({
                            "filme_id": filme['_id'],
                            "titulo_filme": filme['titulo'],
                            "ano": filme['ano_lancamento']
                        })
                        if filme['ano_lancamento']:
                            anos.append(filme['ano_lancamento'])
            
            # Calcular estatísticas
            total_participacoes = (
                len(documento['participacoes']['como_diretor']) +
                len(documento['participacoes']['como_ator']) +
                len(documento['participacoes']['como_roteirista'])
            )
            
            documento['estatisticas']['total_filmes'] = total_participacoes
            
            if anos:
                documento['estatisticas']['anos_ativo']['inicio'] = min(anos)
                documento['estatisticas']['anos_ativo']['fim'] = max(anos)
            
            if total_participacoes > 0:
                documentos.append(documento)
        
        # Converter todos os Decimals para float
        documentos = [self.convert_decimals(doc) for doc in documentos]
        return documentos
    
    def save_documents(self, documents: List[Dict]):
        """Salva documentos no CouchDB usando bulk insert"""
        if not documents:
            return
        
        bulk_docs = {"docs": documents}
        
        try:
            response = requests.post(
                f"{self.base_url}/_bulk_docs",
                json=bulk_docs,
                headers={'Content-Type': 'application/json'},
                auth=self.auth,
                timeout=30
            )
            
            if response.status_code == 201:
                print(f"✓ {len(documents)} documentos salvos com sucesso")
            else:
                print(f"✗ Erro ao salvar documentos: {response.status_code} - {response.text}")
                
        except Exception as e:
            print(f"✗ Erro na requisição: {e}")
    
    def create_views(self):
        """Cria views úteis no CouchDB"""
        print("🔍 Criando views avançadas...")
        
        # Design document principal para filmes
        filmes_views = {
            "_id": "_design/filmes",
            "views": {
                # View básica: filmes por ano
                "por_ano": {
                    "map": """
                    function(doc) {
                        if (doc.type === 'filme' && doc.ano_lancamento) {
                            emit(doc.ano_lancamento, {
                                titulo: doc.titulo,
                                nota_imdb: doc.nota_imdb,
                                generos: doc.generos,
                                bilheteria: doc.financeiro ? doc.financeiro.bilheteria.mundial : null
                            });
                        }
                    }
                    """,
                    "reduce": "_count"
                },
                
                # View: filmes por gênero
                "por_genero": {
                    "map": """
                    function(doc) {
                        if (doc.type === 'filme' && doc.generos) {
                            doc.generos.forEach(function(genero) {
                                emit(genero, {
                                    titulo: doc.titulo,
                                    ano: doc.ano_lancamento,
                                    nota: doc.nota_imdb,
                                    bilheteria: doc.financeiro ? doc.financeiro.bilheteria.mundial : null
                                });
                            });
                        }
                    }
                    """,
                    "reduce": "_count"
                },
                
                # View: top filmes por nota
                "top_rated": {
                    "map": """
                    function(doc) {
                        if (doc.type === 'filme' && doc.nota_imdb) {
                            emit(doc.nota_imdb, {
                                titulo: doc.titulo,
                                ano: doc.ano_lancamento,
                                generos: doc.generos,
                                votos: doc.votos_imdb
                            });
                        }
                    }
                    """
                },
                
                # View: filmes por década
                "por_decada": {
                    "map": """
                    function(doc) {
                        if (doc.type === 'filme' && doc.ano_lancamento) {
                            var decada = Math.floor(doc.ano_lancamento / 10) * 10;
                            emit(decada, {
                                titulo: doc.titulo,
                                ano: doc.ano_lancamento,
                                nota: doc.nota_imdb
                            });
                        }
                    }
                    """,
                    "reduce": "_count"
                },
                
                # View: filmes por bilheteria
                "por_bilheteria": {
                    "map": """
                    function(doc) {
                        if (doc.type === 'filme' && doc.financeiro && doc.financeiro.bilheteria.mundial) {
                            emit(doc.financeiro.bilheteria.mundial, {
                                titulo: doc.titulo,
                                ano: doc.ano_lancamento,
                                orcamento: doc.financeiro.orcamento,
                                roi: doc.financeiro.orcamento ? 
                                     (doc.financeiro.bilheteria.mundial / doc.financeiro.orcamento) : null
                            });
                        }
                    }
                    """
                },
                
                # View: filmes por duração
                "por_duracao": {
                    "map": """
                    function(doc) {
                        if (doc.type === 'filme' && doc.duracao_minutos) {
                            var categoria;
                            if (doc.duracao_minutos < 90) categoria = 'Curto';
                            else if (doc.duracao_minutos < 120) categoria = 'Médio';
                            else if (doc.duracao_minutos < 180) categoria = 'Longo';
                            else categoria = 'Épico';
                            
                            emit(categoria, {
                                titulo: doc.titulo,
                                duracao: doc.duracao_minutos,
                                ano: doc.ano_lancamento
                            });
                        }
                    }
                    """,
                    "reduce": "_count"
                },
                
                # View: filmes por país
                "por_pais": {
                    "map": """
                    function(doc) {
                        if (doc.type === 'filme' && doc.paises_origem) {
                            doc.paises_origem.forEach(function(pais) {
                                emit(pais, {
                                    titulo: doc.titulo,
                                    ano: doc.ano_lancamento,
                                    nota: doc.nota_imdb
                                });
                            });
                        }
                    }
                    """,
                    "reduce": "_count"
                }
            }
        }
        
        # Design document para pessoas
        pessoas_views = {
            "_id": "_design/pessoas", 
            "views": {
                # View: pessoas por tipo (diretor, ator, roteirista)
                "por_tipo": {
                    "map": """
                    function(doc) {
                        if (doc.type === 'filme' && doc.pessoas) {
                            if (doc.pessoas.diretores) {
                                doc.pessoas.diretores.forEach(function(diretor) {
                                    emit(['diretor', diretor.nome], {
                                        filme: doc.titulo,
                                        ano: doc.ano_lancamento,
                                        nota: doc.nota_imdb
                                    });
                                });
                            }
                            
                            if (doc.pessoas.atores_principais) {
                                doc.pessoas.atores_principais.forEach(function(ator) {
                                    emit(['ator', ator.nome], {
                                        filme: doc.titulo,
                                        ano: doc.ano_lancamento,
                                        ordem: ator.ordem_credito,
                                        nota: doc.nota_imdb
                                    });
                                });
                            }
                            
                            if (doc.pessoas.roteiristas) {
                                doc.pessoas.roteiristas.forEach(function(roteirista) {
                                    emit(['roteirista', roteirista.nome], {
                                        filme: doc.titulo,
                                        ano: doc.ano_lancamento,
                                        nota: doc.nota_imdb
                                    });
                                });
                            }
                        }
                    }
                    """,
                    "reduce": "_count"
                },
                
                # View: colaborações entre pessoas
                "colaboracoes": {
                    "map": """
                    function(doc) {
                        if (doc.type === 'filme' && doc.pessoas) {
                            var pessoas = [];
                            
                            if (doc.pessoas.diretores) {
                                doc.pessoas.diretores.forEach(function(p) {
                                    pessoas.push({nome: p.nome, tipo: 'diretor'});
                                });
                            }
                            if (doc.pessoas.atores_principais) {
                                doc.pessoas.atores_principais.forEach(function(p) {
                                    pessoas.push({nome: p.nome, tipo: 'ator'});
                                });
                            }
                            if (doc.pessoas.roteiristas) {
                                doc.pessoas.roteiristas.forEach(function(p) {
                                    pessoas.push({nome: p.nome, tipo: 'roteirista'});
                                });
                            }
                            
                            // Emitir todas as combinações
                            for (var i = 0; i < pessoas.length; i++) {
                                for (var j = i + 1; j < pessoas.length; j++) {
                                    var key = [pessoas[i].nome, pessoas[j].nome].sort();
                                    emit(key, {
                                        filme: doc.titulo,
                                        ano: doc.ano_lancamento,
                                        tipos: [pessoas[i].tipo, pessoas[j].tipo]
                                    });
                                }
                            }
                        }
                    }
                    """,
                    "reduce": "_count"
                },
                
                # View: filmografia por pessoa (do documento pessoa)
                "filmografia": {
                    "map": """
                    function(doc) {
                        if (doc.type === 'pessoa') {
                            var total = 0;
                            var tipos = [];
                            
                            if (doc.participacoes.como_diretor.length > 0) {
                                tipos.push('diretor');
                                total += doc.participacoes.como_diretor.length;
                            }
                            if (doc.participacoes.como_ator.length > 0) {
                                tipos.push('ator');
                                total += doc.participacoes.como_ator.length;
                            }
                            if (doc.participacoes.como_roteirista.length > 0) {
                                tipos.push('roteirista');
                                total += doc.participacoes.como_roteirista.length;
                            }
                            
                            emit(doc.nome, {
                                total_filmes: total,
                                tipos: tipos,
                                anos_ativo: doc.estatisticas.anos_ativo
                            });
                        }
                    }
                    """
                }
            }
        }
        
        # Design document para análises e estatísticas
        analytics_views = {
            "_id": "_design/analytics",
            "views": {
                # View: estatísticas por ano
                "stats_por_ano": {
                    "map": """
                    function(doc) {
                        if (doc.type === 'filme' && doc.ano_lancamento) {
                            emit(doc.ano_lancamento, {
                                nota: doc.nota_imdb || 0,
                                bilheteria: doc.financeiro ? (doc.financeiro.bilheteria.mundial || 0) : 0,
                                orcamento: doc.financeiro ? (doc.financeiro.orcamento || 0) : 0,
                                duracao: doc.duracao_minutos || 0,
                                premios: doc.premios ? (doc.premios.vitorias || 0) : 0
                            });
                        }
                    }
                    """,
                    "reduce": """
                    function(keys, values, rereduce) {
                        var result = {
                            count: 0,
                            nota_media: 0,
                            bilheteria_total: 0,
                            orcamento_total: 0,
                            duracao_media: 0,
                            premios_total: 0
                        };
                        
                        if (rereduce) {
                            values.forEach(function(v) {
                                result.count += v.count;
                                result.nota_media += v.nota_media * v.count;
                                result.bilheteria_total += v.bilheteria_total;
                                result.orcamento_total += v.orcamento_total;
                                result.duracao_media += v.duracao_media * v.count;
                                result.premios_total += v.premios_total;
                            });
                            if (result.count > 0) {
                                result.nota_media /= result.count;
                                result.duracao_media /= result.count;
                            }
                        } else {
                            result.count = values.length;
                            var nota_sum = 0, duracao_sum = 0;
                            values.forEach(function(v) {
                                nota_sum += v.nota;
                                result.bilheteria_total += v.bilheteria;
                                result.orcamento_total += v.orcamento;
                                duracao_sum += v.duracao;
                                result.premios_total += v.premios;
                            });
                            result.nota_media = result.count > 0 ? nota_sum / result.count : 0;
                            result.duracao_media = result.count > 0 ? duracao_sum / result.count : 0;
                        }
                        
                        return result;
                    }
                    """
                },
                
                # View: ROI (Return on Investment)
                "roi_analysis": {
                    "map": """
                    function(doc) {
                        if (doc.type === 'filme' && doc.financeiro && 
                            doc.financeiro.orcamento && doc.financeiro.bilheteria.mundial) {
                            var roi = doc.financeiro.bilheteria.mundial / doc.financeiro.orcamento;
                            var categoria;
                            
                            if (roi < 1) categoria = 'Prejuízo';
                            else if (roi < 2) categoria = 'Baixo ROI';
                            else if (roi < 5) categoria = 'Médio ROI';
                            else categoria = 'Alto ROI';
                            
                            emit(categoria, {
                                titulo: doc.titulo,
                                roi: roi,
                                orcamento: doc.financeiro.orcamento,
                                bilheteria: doc.financeiro.bilheteria.mundial
                            });
                        }
                    }
                    """,
                    "reduce": "_count"
                }
            }
        }
        
        # Criar todos os design documents
        design_docs = [
            (filmes_views, "filmes"),
            (pessoas_views, "pessoas"), 
            (analytics_views, "analytics")
        ]
        
        for views, name in design_docs:
            try:
                response = requests.put(
                    f"{self.base_url}/_design/{name}",
                    json=views,
                    headers={'Content-Type': 'application/json'},
                    auth=self.auth,
                    timeout=15
                )
                
                if response.status_code in [201, 409]:  # Created ou Conflict (já existe)
                    print(f"✓ Views '{name}' criadas/atualizadas")
                else:
                    print(f"✗ Erro ao criar views '{name}': {response.status_code} - {response.text}")
                    
            except Exception as e:
                print(f"✗ Erro ao criar views '{name}': {e}")
        
        print("🎯 Views criadas com sucesso! Acesse:")
        print("   📊 Filmes por ano: /_design/filmes/_view/por_ano")
        print("   🎭 Filmes por gênero: /_design/filmes/_view/por_genero") 
        print("   ⭐ Top rated: /_design/filmes/_view/top_rated")
        print("   💰 Por bilheteria: /_design/filmes/_view/por_bilheteria")
        print("   👥 Pessoas por tipo: /_design/pessoas/_view/por_tipo")
        print("   🤝 Colaborações: /_design/pessoas/_view/colaboracoes")
        print("   📈 Estatísticas: /_design/analytics/_view/stats_por_ano")
        print("   💹 ROI Analysis: /_design/analytics/_view/roi_analysis")
    
    def migrate(self):
        """Executa a migração completa"""
        print("🚀 Iniciando migração SQL para CouchDB...")
        
        # 1. Criar banco
        print("1️⃣ Criando banco CouchDB...")
        if not self.create_database():
            print("❌ Falha ao criar banco CouchDB. Abortando migração.")
            return
        
        # 2. Extrair dados SQL
        print("2️⃣ Extraindo dados do MySQL...")
        try:
            sql_data = self.get_sql_data()
            print(f"✓ Encontrados {len(sql_data['filmes'])} filmes")
        except Exception as e:
            print(f"❌ Erro ao extrair dados do MySQL: {e}")
            return
        
        # 3. Construir documentos de filme
        print("3️⃣ Construindo documentos de filme...")
        try:
            filme_docs = self.build_filme_documents(sql_data)
            print(f"✓ {len(filme_docs)} documentos de filme criados")
        except Exception as e:
            print(f"❌ Erro ao construir documentos de filme: {e}")
            return
        
        # 4. Construir documentos de pessoa
        print("4️⃣ Construindo documentos de pessoa...")
        try:
            pessoa_docs = self.build_pessoa_documents(sql_data, filme_docs)
            print(f"✓ {len(pessoa_docs)} documentos de pessoa criados")
        except Exception as e:
            print(f"❌ Erro ao construir documentos de pessoa: {e}")
            return
        
        # 5. Salvar documentos
        print("5️⃣ Salvando documentos no CouchDB...")
        try:
            print(f"   📝 Salvando {len(filme_docs)} documentos de filme...")
            self.save_documents(filme_docs)
            print(f"   👥 Salvando {len(pessoa_docs)} documentos de pessoa...")
            self.save_documents(pessoa_docs)
        except Exception as e:
            print(f"❌ Erro ao salvar documentos: {e}")
            return
        
        # 6. Criar views
        print("6️⃣ Criando views...")
        try:
            self.create_views()
        except Exception as e:
            print(f"❌ Erro ao criar views: {e}")
            # Views são opcionais, continuar mesmo com erro
        
        print("✅ Migração concluída com sucesso!")
        print(f"🌐 Acesse: http://localhost:5984/_utils para ver os dados")
        print(f"📊 Banco: {self.couchdb_db}")


def main():
    # Configurações MySQL (instância local - sem Docker)
    MYSQL_CONFIG = {
        'user': 'root',
        'password': 'root123',
        'host': 'localhost',
        'port': 33016,  # Porta padrão MySQL (sem Docker)
        'database': 'filmes'
    }
    
    # Configurações CouchDB (instância local - sem Docker)
    COUCHDB_URL = "http://localhost:5984"  # CouchDB local
    COUCHDB_DB = "filmes_db"  # Nome do banco no CouchDB
    
    print("🔧 Configurações (Serviços Locais - Sem Docker):")
    print(f"   MySQL: {MYSQL_CONFIG['user']}@{MYSQL_CONFIG['host']}:{MYSQL_CONFIG['port']}/{MYSQL_CONFIG['database']}")
    print(f"   CouchDB: {COUCHDB_URL}/{COUCHDB_DB}")
    print("   ℹ️  Usando serviços instalados localmente na máquina")
    print()
    
    # Verificar conexões antes de iniciar
    print("🔍 Verificando conectividade...")
    
    try:
        # Testar MySQL
        from sqlalchemy import create_engine, text
        from urllib.parse import quote_plus
        
        password_enc = quote_plus(MYSQL_CONFIG['password'])
        engine = create_engine(f"mysql+pymysql://{MYSQL_CONFIG['user']}:{password_enc}@{MYSQL_CONFIG['host']}:{MYSQL_CONFIG['port']}/{MYSQL_CONFIG['database']}")
        
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            result.fetchone()
        print("   ✅ MySQL: Conectado com sucesso")
        
    except Exception as e:
        print(f"   ❌ MySQL: Erro na conexão - {e}")
        print("   💡 Verifique se o MySQL está rodando localmente na porta 3306")
        return
    
    try:
        # Testar CouchDB
        response = requests.get(COUCHDB_URL)
        if response.status_code == 200:
            print("   ✅ CouchDB: Conectado com sucesso")
        else:
            print(f"   ❌ CouchDB: Status {response.status_code}")
            return
            
    except Exception as e:
        print(f"   ❌ CouchDB: Erro na conexão - {e}")
        print("   💡 Verifique se o CouchDB está rodando localmente na porta 5984")
        return
    
    print()
    migrator = SQLToCouchDBMigrator(MYSQL_CONFIG, COUCHDB_URL, COUCHDB_DB)
    migrator.migrate()


if __name__ == "__main__":
    main()