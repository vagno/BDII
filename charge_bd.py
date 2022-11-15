#Rodar comandos
#sudo apt-get update -y
#sudo apt-get install -y python3-psycopg2
#pip install psycopg2-binary

#Criar banco de dados bd_log


#Importar bibliotecas
import psycopg2  # Acessar banco de dados
import json #


#Configurações
host = 'localhost'
dbname = 'bd_log'
user = 'postgres'
#password = 'postgres'
sslmode = 'require'
#user = input("Digite o nome do banco:")
password = input ("Digite a senha para acesso ao banco:")


# String para conexão ao banco de dados
conn_string = "host={0} user={1} dbname={2} password={3} sslmode={4}".format(host, user, dbname, password, sslmode)
conn = psycopg2.connect(conn_string)
print("Conexão estabelicida")

cursor = conn.cursor()

# Deletar a tabela caso ela já exista
cursor.execute("DROP TABLE IF EXISTS dados;")

# Criar a tabela
cursor.execute("CREATE TABLE dados (id serial PRIMARY KEY, A VARCHAR(50), B INTEGER);")
print("Tabela criada")


#cursor.execute("INSERT INTO inventory (A, B) VALUES (%s, %s);", ("banana", 150))
#========================================================
#LER ARQUIVO JSON
with open("metadado.json", encoding='utf-8') as file:
    dados = json.load(file)

#quantidade de dados a serem adicionados
qtd =  len(dados['INITIAL']['A'])

# Adicionar dados json no banco de dados bd_log
for i in range(qtd):
    print(dados["INITIAL"]["A"][i])
    dados_a = dados["INITIAL"]["A"][i]
    dados_b = dados["INITIAL"]["B"][i]
    cursor.execute("INSERT INTO dados (A, B) VALUES (%s, %s);", (dados_a, dados_b))

# Clean up

conn.commit()
cursor.close()
conn.close()
print("Conexão fechada")




#print(dados["INITIAL"]["A"][0])
#print(dados["INITIAL"]["B"])

