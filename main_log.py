#Rodar comandos
#sudo apt-get update -y
#sudo apt-get install -y python3-psycopg2
#pip install psycopg2-binary

#executar na pasta do arquivo principal


# carrega o arquivo json, e armazena dados iniciais no banco de dados
import charge_bd


#temp= (i, x)
#temps=[]
#temps.append(temp)
#print("temp")
#Rodar comandos
#sudo apt-get update -y
#sudo apt-get install -y python3-psycopg2
#pip install psycopg2-binary

#executar na pasta do arquivo principal

import csv


# Carrega o arquivo json, e armazena dados iniciais no banco de dados
#import charge_bd



# Leitura arquivo de entrada do Log
arq = open('entradaLog', 'r')# aqui faz abertura do arquivo
arq_log = arq.readlines()
#arq_log = texto.split()
arq.close() 

#retorna qtd de linhas do log
n_linhas_log = len(arq_log)
print("\nnumero de linhas do codigo: ",n_linhas_log)

#for i in range(n_linhas_log):
    #print(arq_log[i])


#encontrar commit
temp = []
T_commits=[]
transacao_ckpt = []
for i in range(n_linhas_log-1,0,-1): # linhas de forma decremental
    x = arq_log[i].find("commit") # valor 1 (encontrou), valor -1(não encontrou)
    ckpt = arq_log[i].find("CKPT")

    if (x == 1):
        transacao = arq_log[i][8] + arq_log[i][9] #transação atual do commit
        #temp = temp.append([i , aux])
        temp = (i, transacao)
        T_commits.append(temp)
        print("commit na linha:",i)

    temp_ckpt = ""
    if (ckpt == 1):
        for k in range ((len(arq_log[i]) - 7)):
            j = k + 7
            if (arq_log[i][j] == "(" or ","):
                
                if (arq_log[i][j] != "," and ")"):
                    temp_ckpt += arq_log[i][j]
                    temp = (i, temp_ckpt)
                    #print("temp_ckpt",temp_ckpt)
                
                if (arq_log[i][j] == ","):
                    #transacao_ckpt.append(temp_ckpt)
                    transacao_ckpt.append(temp)
                    temp_ckpt = ""
                
                if (arq_log[i][j] == ")"):
                    transacao_ckpt.append(temp) # verificar
                    #print("terminou chequagem")
                    break


print ("transações do ckpt:", transacao_ckp)
print("transações dos commit:",T_commits)