#Rodar comandos
#sudo apt-get update -y
#sudo apt-get install -y python3-psycopg2
#pip install psycopg2-binary

#executar na pasta do arquivo principal

import csv


# Carrega o arquivo json, e armazena dados iniciais no banco de dados
#import charge_bd



# Leitura arquivo de entrada do Log
arq = open('entradaLog', 'r')
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
transacao_ckp = []
for i in range(n_linhas_log-1,0,-1): # linhas de forma decremental
    x = arq_log[i].find("commit") # valor 1 (encontrou), valor -1(não encontrou)
    ckp = arq_log[i].find("CKPT")

    if (x == 1):
        transacao = arq_log[i][8] + arq_log[i][9] #transação atual do commit
        #temp = temp.append([i , aux])
        temp = (i, transacao)
        T_commits.append(temp)
        print("commit na linha:",i)

    temp_ckp = ""
    if (ckp == 1):
        for k in range ((len(arq_log[i]) - 7)):
            j = k + 7
            if (arq_log[i][j] == "(" or ","):
                
                if (arq_log[i][j] != "," and ")"):
                    temp_ckp += arq_log[i][j]
                    temp = (i, temp_ckp)
                    #print("temp_ckp",temp_ckp)
                
                if (arq_log[i][j] == ","):
                    #transacao_ckp.append(temp_ckp)
                    transacao_ckp.append(temp)
                    temp_ckp = ""
                
                if (arq_log[i][j] == ")"):
                    transacao_ckp.append(temp) # verificar
                    #print("terminou chequagem")
                    break


print ("transações do ckp:", transacao_ckp)
print("transações dos commit:",T_commits)
