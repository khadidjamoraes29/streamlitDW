import pandas as pd
import pymysql
import re
import os
import sys

class FilmesNormalizer:
    def __init__(self, host='localhost', user='root', password='', database='filmes_db'):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.connection = None
        self.cursor = None
        
    def conectar_banco(self):
        """Conecta ao banco de dados MySQL usando PyMySQL"""
        try:
            self.connection = pymysql.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database,
                charset='utf8mb4',
                autocommit=False
            )
            
            self.cursor = self.connection.cursor()
            print("✅ Conexão com MySQL estabelecida com sucesso")
            return True
                
        except Exception as e:
            print(f"❌ Erro ao conectar ao MySQL: {e}")
            return False
    
    def desconectar_banco(self):
        """Desconecta do banco de dados"""
        if self.connection:
            self.cursor.close()
            self.connection.close()
            print("✅ Conexão com MySQL encerrada")
    
    def limpar_banco(self):
        """Limpa todas as tabelas antes de inserir novos dados"""
        try:
            print("🧹 Limpando dados existentes...")
            
            # Desabilitar verificação de foreign keys temporariamente
            self.cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
            
            tabelas = [
                'Filme_Idioma', 'Filme_Empresa_Producao', 'Filme_Local_Filmagem',
                'Filme_Pais_Origem', 'Filme_Genero', 'Filme_Roteirista', 
                'Filme_Diretor', 'Filme_Estrela', 'Filmes', 'Pessoas', 
                'Generos', 'Paises', 'Locais', 'Empresas', 'Idiomas'
            ]
            
            for tabela in tabelas:
                self.cursor.execute(f"TRUNCATE TABLE {tabela}")
                print(f"   - Tabela {tabela} limpa")
            
            # Reabilitar verificação de foreign keys
            self.cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
            self.connection.commit()
            print("✅ Limpeza concluída")
            
        except Exception as e:
            print(f"❌ Erro ao limpar banco: {e}")
            self.connection.rollback()
    
    def inserir_ou_obter_id(self, tabela, campo_nome, valor, campo_id):
        """Insere um registro ou obtém o ID se já existir"""
        if not valor or pd.isna(valor) or str(valor).strip() == '':
            return None
            
        valor = str(valor).strip()
        
        # Verificar se já existe
        query_select = f"SELECT {campo_id} FROM {tabela} WHERE {campo_nome} = %s"
        self.cursor.execute(query_select, (valor,))
        resultado = self.cursor.fetchone()
        
        if resultado:
            return resultado[0]
        
        # Inserir novo registro
        query_insert = f"INSERT INTO {tabela} ({campo_nome}) VALUES (%s)"
        self.cursor.execute(query_insert, (valor,))
        return self.cursor.lastrowid
    
    def processar_lista_valores(self, valores_str, separador=','):
        """Processa string com múltiplos valores separados por vírgula"""
        if not valores_str or pd.isna(valores_str):
            return []
        
        valores = [v.strip() for v in str(valores_str).split(separador) if v.strip()]
        return valores
    
    def limpar_duracao(self, duracao_str):
        """Converte duração do formato '1h 34m' para minutos"""
        if not duracao_str or pd.isna(duracao_str):
            return None
        
        duracao_str = str(duracao_str).strip()
        total_minutos = 0
        
        # Extrair horas
        horas_match = re.search(r'(\d+)h', duracao_str)
        if horas_match:
            total_minutos += int(horas_match.group(1)) * 60
        
        # Extrair minutos
        minutos_match = re.search(r'(\d+)m', duracao_str)
        if minutos_match:
            total_minutos += int(minutos_match.group(1))
        
        return total_minutos if total_minutos > 0 else None
    
    def limpar_valor_numerico(self, valor):
        """Limpa e converte valores numéricos"""
        if pd.isna(valor) or valor == '' or valor is None:
            return None
        
        try:
            return float(valor)
        except (ValueError, TypeError):
            return None
    
    def processar_filme(self, row):
        """Processa um filme e insere no banco"""
        try:
            # 1. Inserir o filme principal
            query_filme = """
                INSERT INTO Filmes (
                    titulo, link_imdb, ano_lancamento, duracao_minutos, 
                    classificacao_mpa, nota_imdb, votos_imdb, orcamento,
                    bilheteria_mundial, bilheteria_eua_canada, bilheteria_abertura,
                    vitorias_premios, nominacoes_premios, vitorias_oscar
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            # Preparar valores do filme
            classificacao = row.get('rating_mpa')
            if pd.isna(classificacao) or classificacao == '':
                classificacao = None
            
            valores_filme = (
                row['title'],
                row['link'],
                int(row['year']) if pd.notna(row['year']) else None,
                self.limpar_duracao(row['duration']),
                classificacao,
                self.limpar_valor_numerico(row['rating_imdb']),
                self.limpar_valor_numerico(row['vote']),
                self.limpar_valor_numerico(row['budget']),
                self.limpar_valor_numerico(row['gross_world_wide']),
                self.limpar_valor_numerico(row['gross_us_canada']),
                self.limpar_valor_numerico(row['gross_opening_weekend']),
                self.limpar_valor_numerico(row['win']),
                self.limpar_valor_numerico(row['nomination']),
                self.limpar_valor_numerico(row['oscar'])
            )
            
            self.cursor.execute(query_filme, valores_filme)
            id_filme = self.cursor.lastrowid
            
            # 2. Processar diretores
            diretores = self.processar_lista_valores(row['director'])
            for diretor in diretores:
                id_pessoa = self.inserir_ou_obter_id('Pessoas', 'nome_pessoa', diretor, 'id_pessoa')
                if id_pessoa:
                    self.cursor.execute(
                        "INSERT IGNORE INTO Filme_Diretor (id_filme, id_pessoa) VALUES (%s, %s)",
                        (id_filme, id_pessoa)
                    )
            
            # 3. Processar roteiristas
            roteiristas = self.processar_lista_valores(row['writer'])
            for roteirista in roteiristas:
                id_pessoa = self.inserir_ou_obter_id('Pessoas', 'nome_pessoa', roteirista, 'id_pessoa')
                if id_pessoa:
                    self.cursor.execute(
                        "INSERT IGNORE INTO Filme_Roteirista (id_filme, id_pessoa) VALUES (%s, %s)",
                        (id_filme, id_pessoa)
                    )
            
            # 4. Processar estrelas (atores principais)
            estrelas = self.processar_lista_valores(row['star'])
            for i, estrela in enumerate(estrelas):
                id_pessoa = self.inserir_ou_obter_id('Pessoas', 'nome_pessoa', estrela, 'id_pessoa')
                if id_pessoa:
                    self.cursor.execute(
                        "INSERT IGNORE INTO Filme_Estrela (id_filme, id_pessoa, ordem_credito) VALUES (%s, %s, %s)",
                        (id_filme, id_pessoa, i + 1)
                    )
            
            # 5. Processar gêneros
            generos = self.processar_lista_valores(row['genre'])
            for genero in generos:
                id_genero = self.inserir_ou_obter_id('Generos', 'nome_genero', genero, 'id_genero')
                if id_genero:
                    self.cursor.execute(
                        "INSERT IGNORE INTO Filme_Genero (id_filme, id_genero) VALUES (%s, %s)",
                        (id_filme, id_genero)
                    )
            
            # 6. Processar países de origem
            paises = self.processar_lista_valores(row['country_origin'])
            for pais in paises:
                id_pais = self.inserir_ou_obter_id('Paises', 'nome_pais', pais, 'id_pais')
                if id_pais:
                    self.cursor.execute(
                        "INSERT IGNORE INTO Filme_Pais_Origem (id_filme, id_pais) VALUES (%s, %s)",
                        (id_filme, id_pais)
                    )
            
            # 7. Processar locais de filmagem
            if pd.notna(row['filming_location']):
                locais = self.processar_lista_valores(row['filming_location'])
                for local in locais:
                    id_local = self.inserir_ou_obter_id('Locais', 'nome_local', local, 'id_local')
                    if id_local:
                        self.cursor.execute(
                            "INSERT IGNORE INTO Filme_Local_Filmagem (id_filme, id_local) VALUES (%s, %s)",
                            (id_filme, id_local)
                        )
            
            # 8. Processar empresas de produção
            empresas = self.processar_lista_valores(row['production_company'])
            for empresa in empresas:
                id_empresa = self.inserir_ou_obter_id('Empresas', 'nome_empresa', empresa, 'id_empresa')
                if id_empresa:
                    self.cursor.execute(
                        "INSERT IGNORE INTO Filme_Empresa_Producao (id_filme, id_empresa) VALUES (%s, %s)",
                        (id_filme, id_empresa)
                    )
            
            # 9. Processar idiomas
            idiomas = self.processar_lista_valores(row['language'])
            for idioma in idiomas:
                id_idioma = self.inserir_ou_obter_id('Idiomas', 'nome_idioma', idioma, 'id_idioma')
                if id_idioma:
                    self.cursor.execute(
                        "INSERT IGNORE INTO Filme_Idioma (id_filme, id_idioma) VALUES (%s, %s)",
                        (id_filme, id_idioma)
                    )
            
            return True
            
        except Exception as e:
            print(f"❌ Erro ao processar filme '{row['title']}': {e}")
            return False
    
    def normalizar_dados(self, arquivo_csv):
        """Função principal para normalizar os dados"""
        try:
            print("🎬 INICIANDO NORMALIZAÇÃO DE DADOS DE FILMES")
            print("=" * 50)
            
            # Conectar ao banco
            if not self.conectar_banco():
                return False
            
            # Limpar dados existentes
            self.limpar_banco()
            
            # Ler CSV
            print(f"📁 Carregando dados do arquivo: {arquivo_csv}")
            df = pd.read_csv(arquivo_csv)
            total_filmes = len(df)
            print(f"📊 Total de filmes encontrados: {total_filmes}")
            print()
            
            # Processar cada filme
            filmes_processados = 0
            filmes_com_erro = 0
            
            for index, row in df.iterrows():
                if self.processar_filme(row):
                    filmes_processados += 1
                else:
                    filmes_com_erro += 1
                
                # Commit a cada 100 registros para melhor performance
                if (index + 1) % 100 == 0:
                    self.connection.commit()
                    progresso = ((index + 1) / total_filmes) * 100
                    print(f"⏳ Progresso: {index + 1}/{total_filmes} ({progresso:.1f}%)")
            
            # Commit final
            self.connection.commit()
            
            print("\n" + "=" * 50)
            print("🎉 NORMALIZAÇÃO CONCLUÍDA!")
            print("=" * 50)
            print(f"📈 Total de filmes: {total_filmes}")
            print(f"✅ Filmes processados com sucesso: {filmes_processados}")
            print(f"❌ Filmes com erro: {filmes_com_erro}")
            print(f"📊 Taxa de sucesso: {(filmes_processados/total_filmes)*100:.1f}%")
            
            return True
            
        except Exception as e:
            print(f"❌ Erro durante a normalização: {e}")
            if self.connection:
                self.connection.rollback()
            return False
        
        finally:
            self.desconectar_banco()

def main():
    """Função principal"""
    print("🎬 SISTEMA DE NORMALIZAÇÃO DE DADOS DE FILMES")
    print("=" * 50)
    
    # Configurações do banco (altere conforme necessário)
    config_banco = {
        'host': 'localhost',
        'user': 'root',
        'password': 'mdrzz.0301',  # Coloque sua senha aqui
        'database': 'filmes_db'
    }
    
    normalizer = FilmesNormalizer(**config_banco)
    
    # Caminho para o arquivo CSV
    arquivo_csv = "filmes_ingles_apos_2000.csv"
    
    if not os.path.exists(arquivo_csv):
        print(f"❌ Arquivo CSV não encontrado: {arquivo_csv}")
        print("📝 Certifique-se de que o arquivo está no mesmo diretório do script.")
        return
    
    print(f"📁 Arquivo encontrado: {arquivo_csv}")
    
    # Confirmar execução
    resposta = input("\n🤔 Deseja continuar com a normalização? (s/n): ")
    if resposta.lower() not in ['s', 'sim', 'y', 'yes']:
        print("❌ Operação cancelada pelo usuário.")
        return
    
    # Executar normalização
    if normalizer.normalizar_dados(arquivo_csv):
        print("\n🎉 Script executado com sucesso!")
        print("💾 Dados normalizados e inseridos no banco de dados.")
    else:
        print("\n❌ Falha na execução do script!")
        print("🔍 Verifique as mensagens de erro acima.")

if __name__ == "__main__":
    main()