# https://multitecnica.com.br/perdas-na-colheita-o-que-voce-pode-fazer-para-reduzi-las-3/
import requests
import pandas as pd
from datetime import datetime
import os
import oracledb

def criar_tabela_prev_clima():
    try:
        # Conexão com banco de dados
        conn = oracledb.connect(user='rm567787', password="281083", dsn='oracle.fiap.com.br:1521/ORCL')
        cursor = conn.cursor()

        # Verifica se a tabela prev_cliente existe
        cursor.execute("""
            SELECT COUNT(*) FROM ALL_TABLES WHERE TABLE_NAME = UPPER('prev_clima')
        """)
        existe_prev_clima = cursor.fetchone()[0]

        if existe_prev_clima == 0:
            # Cria a tabela prev_clima
            cursor.execute("""
                CREATE TABLE prev_clima (
                    DATA_REF DATE,
                    TEMPERATURA NUMBER(5,2),
                    PRECIPITACAO_PROB_PCT NUMBER(5,2),
                    NUVENS_COBERTURA_PCT NUMBER(5,2),
                    DESCRICAO VARCHAR2(255),
                    PRODUTIVIDADE NUMBER(5,2)
                )
            """)
            print("Tabela 'prev_clima' criada com sucesso.")
        else:
            print("Tabela 'prev_clima' já existe. Nenhuma ação necessária.")

        conn.commit()
        conexao = True

    except Exception as e:
        print("Erro: ", e)
        conexao = False
    finally:
        if 'conn' in locals():
            conn.close()

# Executa a função
criar_tabela_prev_clima()

import oracledb

def criar_tabela_plano_producao():
    try:
        # Conexão com banco de dados
        conn = oracledb.connect(user='rm567787', password="281083", dsn='oracle.fiap.com.br:1521/ORCL')
        cursor = conn.cursor()

        # Verifica se a tabela plano_producao existe
        cursor.execute("""
            SELECT COUNT(*) FROM ALL_TABLES WHERE TABLE_NAME = UPPER('plano_producao')
        """)
        existe_plano_producao = cursor.fetchone()[0]

        if existe_plano_producao == 0:
            # Cria a tabela plano_producao
            cursor.execute("""
                CREATE TABLE plano_producao (
                    DATA_REF DATE,
                    QUANT_COLHEITADEIRA NUMBER(5,2),
                    META_PRODUCAO_HE NUMBER(5,2)
                )
            """)
            print("Tabela 'plano_producao' criada com sucesso.")
        else:
            print("Tabela 'plano_producao' já existe. Nenhuma ação necessária.")

        conn.commit()

    except Exception as e:
        print("Erro: ", e)
    finally:
        if 'conn' in locals():
            conn.close()

# Executa a função
criar_tabela_plano_producao()

#conexão com API Open Weather Map
API_KEY = "e6d1c279f1c8e066b58bd6e7dab3129f"
BASE_URL = "http://api.openweathermap.org/data/2.5/forecast"

city_name = "Florianopolis"
params = {"q": city_name, "appid": API_KEY, "units": "metric"}

DATA = []
TEMP = []
DESCRICAO = []
HUMIDADE = []
NUVENS = []
CHUVA = []
PRECIPITACAO = []

#Trabalhando com arquivo JSON - extração de dados arquivo JSON previsão do climatologica
try:
    response = requests.get(BASE_URL, params=params)
    response.raise_for_status()
    data = response.json()

    if data["cod"] == "200":
        print(f"Previsão de 5 dias para {data['city']['name']}:\n")
        for forecast_item in data["list"]:
            timestamp = forecast_item["dt"]
            DATA.append(datetime.fromtimestamp(timestamp))
            TEMP.append(forecast_item["main"]["temp"])
            DESCRICAO.append(forecast_item["weather"][0]["description"])
            HUMIDADE.append(forecast_item["main"]["humidity"])
            NUVENS.append(forecast_item.get("clouds", {}).get("all", "N/A"))
            CHUVA.append(forecast_item.get("rain", {}).get("3h", 0.0))
            PRECIPITACAO.append(forecast_item.get("pop", "N/A"))

#Tratamento de Erros API Open Weather
except requests.exceptions.RequestException as e:
    print(f"Erro na requisição: {e}")
except KeyError as e:
    print(f"Erro ao processar os dados: chave ausente {e}")

#Construção de Data Frame com previsões climatologicas
previsao = pd.DataFrame({'DATA': DATA,
                         'TEMPERATURA': TEMP,
                         'DESCRICAO': DESCRICAO,
                         'HUMIDADE': HUMIDADE,
                         'NUVENS (COBERTURA %)': NUVENS,
                         'CHUVA (mm 3h)': CHUVA,
                         'PRECIPITACAO (prob %)': PRECIPITACAO})
previsao['CIDADE'] = city_name

previsao['DATA_REF'] = previsao['DATA'].dt.strftime('%Y-%m-%d')

# Agrupar por DATA_REF e calcular as médias
resumo = previsao.groupby('DATA_REF')[['TEMPERATURA', 'PRECIPITACAO (prob %)','NUVENS (COBERTURA %)']].mean().reset_index()

dia_chuva = previsao[previsao['DESCRICAO'].str.contains('rain')]
dia_chuva = dia_chuva.groupby('DATA_REF').head(1)[['DATA_REF', 'DESCRICAO']]

resumo = resumo.merge(dia_chuva, on='DATA_REF', how='left')
resumo = resumo.fillna('seca')

def categorize_score(DESCRICAO):
    if DESCRICAO == 'light rain':
        return 'CHUVA_FRACA'
    elif DESCRICAO == 'moderate rain':
        return 'CHUVA_MODERADA'
    elif DESCRICAO == 'seca':
        return 'SECO'
    else:
        return 'CHUVA_FORTE'

def categorize_produtividade(DESCRICAO):
    if DESCRICAO == 'SECO':
        return 1
    elif DESCRICAO == 'CHUVA_FRACA':
        return 0.9
    elif DESCRICAO == 'CHUVA_MODERADA':
        return 0.8
    else:
        return 0.7

resumo['DESCRICAO'] = resumo['DESCRICAO'].apply(categorize_score)
resumo['PRODUTIVIDADE'] = resumo['DESCRICAO'].apply(categorize_produtividade)

def inserir_novas_datas(resumo_df):
    conn = None
    cursor = None
    try:
        # Conexão com banco de dados
        conn = oracledb.connect(user='rm567787', password="281083", dsn='oracle.fiap.com.br:1521/ORCL')
        cursor = conn.cursor()

        # Buscar todas as datas já existentes na tabela prev_clima
        cursor.execute("SELECT TO_CHAR(DATA_REF, 'YYYY-MM-DD') FROM prev_clima")
        datas_existentes = {row[0] for row in cursor.fetchall()}

        # Filtrar apenas as datas que não estão na tabela prev_clima
        novas_linhas = resumo_df[~resumo_df['DATA_REF'].isin(datas_existentes)]

        if novas_linhas.empty:
            print("Nenhuma nova data para inserir.")
        else:
            # Inserir novas linhas na tabela prev_clima
            for _, row in novas_linhas.iterrows():
                cursor.execute("""
                    INSERT INTO prev_clima (DATA_REF, TEMPERATURA, PRECIPITACAO_PROB_PCT, NUVENS_COBERTURA_PCT, DESCRICAO, PRODUTIVIDADE)
                    VALUES (TO_DATE(:data_ref, 'YYYY-MM-DD'), :temperatura, :precipitacao, :nuvens, :descricao, :produtividade)
                """, {
                    'data_ref': row['DATA_REF'],
                    'temperatura': round(row['TEMPERATURA'], 2),
                    'precipitacao': round(row['PRECIPITACAO (prob %)'], 2),
                    'nuvens': round(row['NUVENS (COBERTURA %)'], 2),
                    'descricao': row['DESCRICAO'],
                    'produtividade': row['PRODUTIVIDADE']
                })

            conn.commit()
            print(f"{len(novas_linhas)} novas datas inseridas com sucesso na tabela prev_clima.")

    except Exception as e:
        print("Erro ao inserir novas datas:", e)
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()

# Executa a função passando o DataFrame resumo
inserir_novas_datas(resumo)

margem = " "
conexao = True

while conexao:
    os.system('cls')  # Limpa tela no Windows

    print("---- CADASTRAR PLANO DE PRODUCAO ----")
    print("""
    1 - Programar Data para producao
    2 - Listar Data para producao
    3 - Alterar Data para producao
    4 - Excluir Data para producao
    5 - EXCLUIR TODO O PLANO DE PRODUCAO
    6 - SAIR
    """)

    try:
        escolha = int(input(margem + "Escolha -> "))
    except ValueError:
        print("Por favor, digite um número válido.")
        continue

    match escolha:
        # CADASTRAR UM PLANO DE PRODUCAO
        case 1:
            try:
                print("----- CADASTRAR PLANO DE PRODUCAO -----\n")

                DATA_REF = input(margem + "Digite a data (AAAA-MM-DD): ")
                QUANT_COLHEITADEIRA = float(input(margem + "Digite quantidade de colheitadeiras: "))
                META_PRODUCAO_HE = float(input(margem + "Digite a meta de hectares para colheita: "))

                # Conexão com BD
                conn = oracledb.connect(user='rm567787', password="281083", dsn='oracle.fiap.com.br:1521/ORCL')
                cursor = conn.cursor()

                cadastro = """
                    INSERT INTO plano_producao (DATA_REF, QUANT_COLHEITADEIRA, META_PRODUCAO_HE)
                    VALUES (TO_DATE(:DATA_REF, 'YYYY-MM-DD'), :QUANT_COLHEITADEIRA, :META_PRODUCAO_HE)
                """

                cursor.execute(cadastro, {
                    "DATA_REF": DATA_REF,
                    "QUANT_COLHEITADEIRA": QUANT_COLHEITADEIRA,
                    "META_PRODUCAO_HE": META_PRODUCAO_HE
                })
                conn.commit()

            except ValueError:
                print("Digite valores numéricos válidos para quantidade e meta!")
            except Exception as e:
                print("Erro na transação do BD:", e)
            else:
                print("\nDados GRAVADOS com sucesso!")
                input("Pressione ENTER para continuar...")
            finally:
                if 'cursor' in locals():
                    cursor.close()
                if 'conn' in locals():
                    conn.close()


        # LISTAR PLANO DE PRODUCAO
        case 2:
            print("----- LISTAR PLANO DE PRODUCAO -----\n")
            lista_dados = []

            try:
                conn = oracledb.connect(user='rm567787', password="281083", dsn='oracle.fiap.com.br:1521/ORCL')
                cursor = conn.cursor()

                cursor.execute('SELECT DATA_REF, QUANT_COLHEITADEIRA, META_PRODUCAO_HE FROM plano_producao')
                data = cursor.fetchall()

                for dt in data:
                    lista_dados.append(dt)

                lista_dados = sorted(lista_dados)

                dados_df = pd.DataFrame.from_records(
                    lista_dados,
                    columns=['DATA_REF', 'QUANT_COLHEITADEIRA', 'META_PRODUCAO_HE']
                )

                if dados_df.empty:
                    print("Não há Plano Cadastrado!")
                else:
                    print(dados_df)
                    print("\nLISTADOS!")

            except Exception as e:
                print("Erro ao consultar dados:", e)

            finally:
                if 'cursor' in locals():
                    cursor.close()
                if 'conn' in locals():
                    conn.close()

            input("Pressione ENTER para continuar...")

        case 3:
            try:
                print("----- ALTERAR PLANO DE PRODUCAO -----\n")

                DATA_REF = input(margem + "Digite a data do plano que deseja alterar (AAAA-MM-DD): ")

                # Conexão com BD
                conn = oracledb.connect(user='rm567787', password="281083", dsn='oracle.fiap.com.br:1521/ORCL')
                cursor = conn.cursor()

                # Consulta para verificar se existe plano com essa data
                consulta = "SELECT DATA_REF, QUANT_COLHEITADEIRA, META_PRODUCAO_HE FROM plano_producao WHERE DATA_REF = TO_DATE(:DATA_REF, 'YYYY-MM-DD')"
                cursor.execute(consulta, {"DATA_REF": DATA_REF})
                data = cursor.fetchall()

                if len(data) == 0:
                    print(f"Não há um plano cadastrado com a DATA = {DATA_REF}")
                    input("\nPressione ENTER")
                else:
                    # Exibe dados atuais
                    print(f"Dados atuais: {data[0]}")

                    # Solicita novos valores
                    novo_DATA_REF = input(margem + "Digite a nova data (AAAA-MM-DD): ")
                    novo_QUANT_COLHEITADEIRA = float(input(margem + "Digite nova quantidade de colheitadeiras: "))
                    nova_META_PRODUCAO_HE = float(input(margem + "Digite nova meta de hectares para colheita: "))

                    # Atualiza registro usando parâmetros
                    alteracao = """
                        UPDATE plano_producao
                        SET DATA_REF = TO_DATE(:novo_DATA_REF, 'YYYY-MM-DD'),
                            QUANT_COLHEITADEIRA = :QUANT_COLHEITADEIRA,
                            META_PRODUCAO_HE = :META_PRODUCAO_HE
                        WHERE DATA_REF = TO_DATE(:DATA_REF, 'YYYY-MM-DD')
                    """
                    cursor.execute(alteracao, {
                        "novo_DATA_REF": novo_DATA_REF,
                        "QUANT_COLHEITADEIRA": novo_QUANT_COLHEITADEIRA,
                        "META_PRODUCAO_HE": nova_META_PRODUCAO_HE,
                        "DATA_REF": DATA_REF
                    })
                    conn.commit()

            except ValueError:
                print("Digite valores numéricos válidos!")
            except Exception as e:
                print("Erro na transação do BD:", e)
            else:
                print("\nDados ATUALIZADOS com sucesso!")
                input("Pressione ENTER para continuar...")
            finally:
                if 'cursor' in locals():
                    cursor.close()
                if 'conn' in locals():
                    conn.close()

        case 4:
            try:
                print("----- EXCLUIR PLANO DE PRODUCAO -----\n")

                DATA_REF = input(margem + "Digite a data do plano que deseja excluir (AAAA-MM-DD): ")

                # Conexão com BD
                conn = oracledb.connect(user='rm567787', password="281083", dsn='oracle.fiap.com.br:1521/ORCL')
                cursor = conn.cursor()

                # Verifica se existe plano com essa data
                consulta = """
                    SELECT DATA_REF, QUANT_COLHEITADEIRA, META_PRODUCAO_HE
                    FROM plano_producao
                    WHERE DATA_REF = TO_DATE(:DATA_REF, 'YYYY-MM-DD')
                """
                cursor.execute(consulta, {"DATA_REF": DATA_REF})
                data = cursor.fetchall()

                if len(data) == 0:
                    print(f"Não há um plano cadastrado com a Data = {DATA_REF}")
                else:
                    print(f"Plano encontrado: {data[0]}")
                    confirmacao = input("Deseja realmente excluir este plano? (S/N): ").upper()

                    if confirmacao == "S":
                        exclusao = "DELETE FROM plano_producao WHERE DATA_REF = TO_DATE(:DATA_REF, 'YYYY-MM-DD')"
                        cursor.execute(exclusao, {"DATA_REF": DATA_REF})
                        conn.commit()
                        print("\nPlano APAGADO com sucesso!")
                    else:
                        print("Operação cancelada.")

            except Exception as e:
                print("Erro na transação do BD:", e)
            finally:
                if 'cursor' in locals():
                    cursor.close()
                if 'conn' in locals():
                    conn.close()

            input("Pressione ENTER para continuar...")

        case 5:
            try:
                conn = oracledb.connect(user='rm567787', password="281083", dsn='oracle.fiap.com.br:1521/ORCL')
                cursor = conn.cursor()
                confirmacao = input("Deseja realmente excluir TODOS os planos? (S/N): ").upper()
                if confirmacao == "S":
                    cursor.execute("DELETE FROM plano_producao")
                    conn.commit()
                    print("Todos os planos foram excluídos!")
                else:
                    print("Operação cancelada.")
            except Exception as e:
                print("Erro na transação do BD:", e)
            finally:
                if 'cursor' in locals():
                    cursor.close()
                if 'conn' in locals():
                    conn.close()
            input("Pressione ENTER para continuar...")


        case 6:
            print("Saindo...")
            conexao = False

        case _:
            print("Opção inválida!")
            input("Pressione ENTER para continuar...")



lista_dados = []
conn = oracledb.connect(user='rm567787', password="281083", dsn='oracle.fiap.com.br:1521/ORCL')
cursor = conn.cursor()

cursor.execute('SELECT * FROM plano_producao')
data = cursor.fetchall()
for dt in data:
    lista_dados.append(dt)
    lista_dados = sorted(lista_dados)
    dados_df = pd.DataFrame.from_records(
        lista_dados,
        columns=['DATA_REF', 'QUANT_COLHEITADEIRA', 'META_PRODUCAO_HE'])

cursor.execute('SELECT * FROM prev_clima')
data = cursor.fetchall()
for dt in data:
    lista_dados.append(dt)
    lista_dados = sorted(lista_dados)
    dados_df_prev = pd.DataFrame.from_records(
        lista_dados,
        columns=['DATA_REF', 'TEMPERATURA', 'PRECIPITACAO_PROB_PCT', 'NUVENS_COBERTURA_PCT', 'DESCRICAO', 'PRODUTIVIDADE'])

dados_df_prev = dados_df_prev.copy()

dados_df_prev = dados_df_prev[dados_df_prev['DESCRICAO'].notna()]

resumo_plano_producao = dados_df_prev.merge(dados_df, on='DATA_REF', how='left')

dados_df_prev = dados_df_prev.copy()

dados_df_prev = dados_df_prev[dados_df_prev['DESCRICAO'].notna()]

resumo_plano_producao = dados_df_prev.merge(dados_df, on='DATA_REF', how='left')

resumo_plano_producao['QUANT_COLHEITADEIRA'] = resumo_plano_producao['QUANT_COLHEITADEIRA'].fillna(0)
resumo_plano_producao['META_PRODUCAO_HE'] = resumo_plano_producao['META_PRODUCAO_HE'].fillna(0)
resumo_plano_producao['PROJECAO_PRODUCAO'] = resumo_plano_producao['QUANT_COLHEITADEIRA'] * resumo_plano_producao['PRODUTIVIDADE']*5 #considerado 5 hectares por maquina ativa no dia


# Função para aplicar as regras
def avaliar_criterios(row):
    # Regra 1: Se quantidade de empilhadeiras == 0
    if row['QUANT_COLHEITADEIRA'] == 0:
        acao = "Cadastrar plano de produção"
    # Regra 2: Se produção > meta
    elif row['PROJECAO_PRODUCAO'] > row['META_PRODUCAO_HE']:
        acao = "Reduzir quantidade de colheitadeiras"
    # Regra 3: Se produção < meta
    elif row['PROJECAO_PRODUCAO'] < row['META_PRODUCAO_HE']:
        acao = "Aumentar quantidade de colheitadeiras"
    # Regra 4: Se produção = meta
    else:
        acao = "Capacidade produtiva ajustada para o desafio"
    return acao

# Criando nova coluna com a avaliação
resumo_plano_producao['ACAO_RECOMENDADA'] = resumo_plano_producao.apply(avaliar_criterios, axis=1)


print(resumo_plano_producao.to_string())

 