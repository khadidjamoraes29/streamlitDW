# Guia de Views CouchDB - Sistema de Filmes

## 📋 Views Criadas

### 🎬 **Design Document: filmes**
Views relacionadas aos filmes e suas características.

#### 1. **por_ano** - Filmes por Ano
```bash
# Todos os filmes de 2010
curl "http://localhost:5984/filmes_db/_design/filmes/_view/por_ano?key=2010"

# Filmes entre 2000 e 2010
curl "http://localhost:5984/filmes_db/_design/filmes/_view/por_ano?startkey=2000&endkey=2010"

# Contagem de filmes por ano
curl "http://localhost:5984/filmes_db/_design/filmes/_view/por_ano?group=true"
```

#### 2. **por_genero** - Filmes por Gênero
```bash
# Todos os filmes de Drama
curl "http://localhost:5984/filmes_db/_design/filmes/_view/por_genero?key=\"Drama\""

# Contagem por gênero
curl "http://localhost:5984/filmes_db/_design/filmes/_view/por_genero?group=true"

# Top 10 dramas
curl "http://localhost:5984/filmes_db/_design/filmes/_view/por_genero?key=\"Drama\"&limit=10"
```

#### 3. **top_rated** - Filmes Mais Bem Avaliados
```bash
# Top 10 filmes (maior nota primeiro)
curl "http://localhost:5984/filmes_db/_design/filmes/_view/top_rated?descending=true&limit=10"

# Filmes com nota >= 8.5
curl "http://localhost:5984/filmes_db/_design/filmes/_view/top_rated?startkey=8.5"
```

#### 4. **por_decada** - Filmes por Década
```bash
# Filmes dos anos 90
curl "http://localhost:5984/filmes_db/_design/filmes/_view/por_decada?key=1990"

# Contagem por década
curl "http://localhost:5984/filmes_db/_design/filmes/_view/por_decada?group=true"
```

#### 5. **por_bilheteria** - Filmes por Bilheteria
```bash
# Top 10 maior bilheteria
curl "http://localhost:5984/filmes_db/_design/filmes/_view/por_bilheteria?descending=true&limit=10"

# Filmes com bilheteria > $1 bilhão
curl "http://localhost:5984/filmes_db/_design/filmes/_view/por_bilheteria?startkey=1000000000"
```

#### 6. **por_duracao** - Filmes por Categoria de Duração
```bash
# Filmes épicos (>180 min)
curl "http://localhost:5984/filmes_db/_design/filmes/_view/por_duracao?key=\"Épico\""

# Contagem por categoria
curl "http://localhost:5984/filmes_db/_design/filmes/_view/por_duracao?group=true"
```

#### 7. **por_pais** - Filmes por País
```bash
# Filmes dos EUA
curl "http://localhost:5984/filmes_db/_design/filmes/_view/por_pais?key=\"United States\""

# Contagem por país
curl "http://localhost:5984/filmes_db/_design/filmes/_view/por_pais?group=true"
```

### 👥 **Design Document: pessoas**
Views relacionadas a pessoas e suas participações.

#### 1. **por_tipo** - Pessoas por Tipo de Participação
```bash
# Todos os diretores
curl "http://localhost:5984/filmes_db/_design/pessoas/_view/por_tipo?startkey=[\"diretor\"]&endkey=[\"diretor\",{}]"

# Filmografia de um ator específico
curl "http://localhost:5984/filmes_db/_design/pessoas/_view/por_tipo?key=[\"ator\",\"Leonardo DiCaprio\"]"

# Contagem de filmes por pessoa
curl "http://localhost:5984/filmes_db/_design/pessoas/_view/por_tipo?group_level=2"
```

#### 2. **colaboracoes** - Colaborações entre Pessoas
```bash
# Colaborações entre duas pessoas
curl "http://localhost:5984/filmes_db/_design/pessoas/_view/colaboracoes?key=[\"Christopher Nolan\",\"Leonardo DiCaprio\"]"

# Todas as colaborações (contagem)
curl "http://localhost:5984/filmes_db/_design/pessoas/_view/colaboracoes?group=true"
```

#### 3. **filmografia** - Filmografia Completa
```bash
# Todas as pessoas e suas estatísticas
curl "http://localhost:5984/filmes_db/_design/pessoas/_view/filmografia"
```

### 📊 **Design Document: analytics**
Views para análises e estatísticas avançadas.

#### 1. **stats_por_ano** - Estatísticas Completas por Ano
```bash
# Estatísticas de 2010
curl "http://localhost:5984/filmes_db/_design/analytics/_view/stats_por_ano?key=2010&reduce=true"

# Estatísticas de todos os anos
curl "http://localhost:5984/filmes_db/_design/analytics/_view/stats_por_ano?group=true"

# Estatísticas da década de 2000
curl "http://localhost:5984/filmes_db/_design/analytics/_view/stats_por_ano?startkey=2000&endkey=2009&reduce=true"
```

#### 2. **roi_analysis** - Análise de ROI (Return on Investment)
```bash
# Filmes com alto ROI
curl "http://localhost:5984/filmes_db/_design/analytics/_view/roi_analysis?key=\"Alto ROI\""

# Contagem por categoria de ROI
curl "http://localhost:5984/filmes_db/_design/analytics/_view/roi_analysis?group=true"
```

## 🚀 Exemplos de Consultas Complexas

### 1. **Dashboard de Ano Específico**
```python
import requests

def dashboard_ano(ano):
    base_url = "http://localhost:5984/filmes_db"
    
    # Filmes do ano
    filmes = requests.get(f"{base_url}/_design/filmes/_view/por_ano?key={ano}").json()
    
    # Estatísticas do ano  
    stats = requests.get(f"{base_url}/_design/analytics/_view/stats_por_ano?key={ano}&reduce=true").json()
    
    # Gêneros do ano
    generos = requests.get(f"{base_url}/_find", json={
        "selector": {"type": "filme", "ano_lancamento": ano},
        "fields": ["generos"]
    }).json()
    
    return {
        "ano": ano,
        "total_filmes": len(filmes["rows"]),
        "estatisticas": stats["rows"][0]["value"] if stats["rows"] else None,
        "filmes": [row["value"] for row in filmes["rows"]]
    }
```

### 2. **Top Diretores por Quantidade de Filmes**
```python
def top_diretores(limit=10):
    base_url = "http://localhost:5984/filmes_db"
    
    response = requests.get(
        f"{base_url}/_design/pessoas/_view/por_tipo",
        params={
            "startkey": '["diretor"]',
            "endkey": '["diretor", {}]',
            "group_level": 2,
            "reduce": "true"
        }
    )
    
    diretores = response.json()["rows"]
    return sorted(diretores, key=lambda x: x["value"], reverse=True)[:limit]
```

### 3. **Análise de Tendências por Década**
```python
def analise_decadas():
    base_url = "http://localhost:5984/filmes_db"
    
    response = requests.get(
        f"{base_url}/_design/filmes/_view/por_decada",
        params={"group": "true"}
    )
    
    decadas = response.json()["rows"]
    
    resultado = {}
    for row in decadas:
        decada = row["key"]
        count = row["value"]
        
        # Buscar estatísticas da década
        stats_response = requests.get(
            f"{base_url}/_design/analytics/_view/stats_por_ano",
            params={
                "startkey": decada,
                "endkey": decada + 9,
                "reduce": "true"
            }
        )
        
        stats = stats_response.json()["rows"]
        resultado[decada] = {
            "total_filmes": count,
            "estatisticas": stats[0]["value"] if stats else None
        }
    
    return resultado
```

## 🎯 Interface Web de Consulta

Acesse a interface administrativa do CouchDB:
- **URL**: http://localhost:5984/_utils
- **Login**: admin / admin123

### Navegação das Views:
1. Clique em "filmes_db" 
2. Vá para a aba "Design Documents"
3. Clique em "_design/filmes", "_design/pessoas" ou "_design/analytics"
4. Clique nas views para visualizar os resultados

## 📈 Monitoramento de Performance

```bash
# Verificar tempo de construção das views
curl "http://localhost:5984/filmes_db/_design/filmes/_info"

# Compactar views para melhor performance
curl -X POST "http://localhost:5984/filmes_db/_compact/filmes"
curl -X POST "http://localhost:5984/filmes_db/_compact/pessoas" 
curl -X POST "http://localhost:5984/filmes_db/_compact/analytics"
```

## 🔧 Customização

Para adicionar novas views, edite o arquivo `migrar_para_couchdb.py` na função `create_views()` e execute novamente a migração ou crie views manualmente via interface web.