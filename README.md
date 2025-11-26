# Projeto Filmes - Sistema de Banco de Dados

Este projeto realiza a **normalização e inserção de dados de filmes** em sistemas de banco de dados, suportando tanto **MySQL (relacional)** quanto **CouchDB (NoSQL)**. Inclui scripts para criação de tabelas, migração de dados e otimização para diferentes casos de uso.

---

## 🎯 Modelos de Dados Disponíveis

### 📊 **Modelo Relacional (MySQL)**
- Estrutura normalizada com tabelas e relacionamentos
- Ideal para consistência transacional e integridade referencial
- Consultas complexas com JOINs

### 📄 **Modelo de Documentos (CouchDB)**
- Estrutura desnormalizada orientada a documentos
- Ideal para escalabilidade e performance de leitura
- API REST nativa e replicação distribuída

---

## 🔹 Pré-requisitos

* Python 3.10+
* Docker e Docker Compose
* Pip (gerenciador de pacotes Python)

---

## 🔹 Setup do ambiente Python

1. **Criar virtual environment:**

```bash
make criar_venv
```

2. **Ativar virtual environment:**

```bash
make ativar
```

3. **Instalar dependências:**

```bash
make instalar
```

4. **Atualizar `requirements.txt` (opcional):**

```bash
make atualizar_requirements
```

5. **Listar pacotes instalados (opcional):**

```bash
make listar
```

---

## 🔹 Rodando os Bancos de Dados

### 🐳 **Opção 1: Com Docker (Recomendado para desenvolvimento)**

O projeto inclui um `docker-compose.yml` que disponibiliza tanto MySQL quanto CouchDB.

```bash
# Subir todos os containers
make up

# Subir apenas MySQL
docker-compose up -d mysql

# Subir apenas CouchDB
docker-compose up -d couchdb
```

### 💻 **Opção 2: Serviços Locais (Produção/Performance)**

Para usar serviços instalados diretamente na máquina:

**MySQL Local:**
- Host: `localhost:3306`
- Usuário/Senha: conforme sua instalação
- Banco: criar `filmes_db`

**CouchDB Local:**
- Instalar: https://couchdb.apache.org/
- Host: `localhost:5984`
- Interface: http://localhost:5984/_utils

📖 **Guia completo**: Ver [INSTALACAO_LOCAL.md](./INSTALACAO_LOCAL.md)

### 🔗 **Acesso aos Bancos:**

**Docker (portas mapeadas):**
- MySQL: `localhost:33016`
- CouchDB: http://localhost:5984/_utils

**Local (portas padrão):**
- MySQL: `localhost:3306`
- CouchDB: http://localhost:5984/_utils

**Credenciais padrão:**
- MySQL: `user/user123` (Docker) ou suas credenciais locais
- CouchDB: `admin/admin123`

---

## � Executando os Scripts

### 📊 **Modelo MySQL (Relacional)**

1. Certifique-se de que o MySQL está rodando
2. Execute o script principal:

```bash
python main.py
```

O script irá:
* Normalizar os dados do CSV
* Criar tabelas relacionais
* Inserir dados normalizados

### 📄 **Migração para CouchDB**

1. Certifique-se de que o CouchDB está rodando
2. Configure o CouchDB (primeira vez):
   - Acesse: http://localhost:5984/_utils
   - Complete o setup do cluster (single node)
3. Execute a migração:

```bash
python migrar_para_couchdb.py
```

4. **Testar consultas:**

```bash
python consultas_couchdb.py
```

---

## 📁 Estrutura do Projeto

```
modelagem-de-dados/
├── banco.sql                     # DDL MySQL original
├── main.py                       # Script principal MySQL
├── migrar_para_couchdb.py        # Script de migração para CouchDB
├── consultas_couchdb.py          # Exemplos de consultas CouchDB
├── docker-compose.yml            # MySQL + CouchDB
├── couchdb_modelo.json           # Modelo de dados CouchDB
├── couchdb_exemplos.md           # Documentos de exemplo
├── MIGRACAO_COUCHDB.md           # Guia completo de migração
└── base de dados nao normalizada/
    └── world_imdb_movies_top_movies_per_year.csv
```

---

## �️ Estruturas de Dados

### **MySQL (Relacional)**

* **Tabela principal:** `Filmes`
* **Tabelas de dimensão:** `Pessoas`, `Generos`, `Idiomas`, `Paises`, `Empresas`
* **Tabelas associativas:** `Filme_Estrela`, `Filme_Diretor`, `Filme_Genero`, etc.

### **CouchDB (Documentos)**

* **Documento Filme:** Informações completas desnormalizadas
* **Documento Pessoa:** Agregação de filmografia
* **Documento Agregação:** Estatísticas pre-computadas
* **Views:** Consultas otimizadas por ano, gênero, etc.

---

## � Exemplos de Consultas

### **MySQL**
```sql
-- Top 10 filmes por nota
SELECT titulo, ano_lancamento, nota_imdb 
FROM Filmes 
ORDER BY nota_imdb DESC 
LIMIT 10;

-- Filmes por diretor
SELECT f.titulo, p.nome_pessoa 
FROM Filmes f
JOIN Filme_Diretor fd ON f.id_filme = fd.id_filme
JOIN Pessoas p ON fd.id_pessoa = p.id_pessoa;
```

### **CouchDB**
```bash
# Filme específico
curl http://localhost:5984/filmes_db/filme:tt0111161

# Filmes por ano (usando view)
curl http://localhost:5984/filmes_db/_design/filmes/_view/por_ano?key=1994

# Busca por título
curl -X POST http://localhost:5984/filmes_db/_find \
  -H "Content-Type: application/json" \
  -d '{"selector": {"type": "filme", "titulo": {"$regex": "Matrix"}}}'
```

---

## 📊 Comparação de Performance

| Aspecto | MySQL | CouchDB |
|---------|-------|---------|
| **Consultas simples** | Rápida | Muito rápida |
| **Consultas complexas** | Excelente (JOINs) | Limitada |
| **Escalabilidade** | Vertical | Horizontal |
| **Consistência** | ACID | Eventual |
| **API** | SQL | REST/JSON |
| **Replicação** | Master-Slave | Multi-Master |

---

## 🎯 Quando Usar Cada Modelo

### **Use MySQL quando:**
- ✅ Precisar de transações ACID
- ✅ Consultas complexas com múltiplos JOINs
- ✅ Integridade referencial crítica
- ✅ Relatórios analíticos complexos

### **Use CouchDB quando:**
- ✅ Precisar de alta escalabilidade
- ✅ Consultas simples e rápidas
- ✅ Aplicações distribuídas
- ✅ Tolerância a falhas
- ✅ Desenvolvimento ágil (schema flexível)

---

## 🔧 Troubleshooting

### **Problemas Comuns MySQL:**
- Container não inicia: verificar porta 33016
- Conexão negada: aguardar inicialização completa
- Encoding: usar UTF-8 no cliente

### **Problemas Comuns CouchDB:**
- Interface não carrega: verificar http://localhost:5984/_utils
- Views lentas: executar compactação
- Docs não salvam: verificar formato JSON

---

## 📚 Documentação Adicional

- [MIGRACAO_COUCHDB.md](./MIGRACAO_COUCHDB.md) - Guia completo de migração
- [couchdb_exemplos.md](./couchdb_exemplos.md) - Exemplos de documentos
- [couchdb_modelo.json](./couchdb_modelo.json) - Especificação do modelo

---

## 🤝 Contribuindo

1. Fork o projeto
2. Crie uma branch para sua feature
3. Commit suas mudanças
4. Push para a branch
5. Abra um Pull Request

---

## 📄 Licença

Este projeto está sob a licença MIT. Veja o arquivo LICENSE para detalhes.