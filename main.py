#Rodar comandos
#sudo apt-get update -y
#sudo apt-get install -y python3-psycopg2
#pip install psycopg2-binary

#executar na pasta do arquivo principal

import csv


# Carrega o arquivo json, e armazena dados iniciais no banco de dados
#import charge_bd



# Leitura arquivo de entrada do Log
arq = open('entradaLog', 'r', encoding="utf_8")
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


    #if (x == 1):
        #transacao = arq_log[i][8] + arq_log[i][9] #transação atual do commit
        #temp = temp.append([i , aux])
        #temp = (i, transacao)
        #T_commits.append(temp)
        #print("commit na linha:",i)

    temp_x =""
    if (x == 1):
        for l in range(len(arq_log[i])-10):
            j = l + 8
            if (arq_log[i][l] != ">"):
                temp_x += arq_log[i][j]
                #print("temp_x",temp_x)
        print("commit na linha:",i)
        temp = (i, temp_x)
        T_commits.append(temp)


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

# Verificar os commits apos o ckp
list_redo= [] #lista para deletar
for i in range(len(transacao_ckp)):
    for j in range(len(T_commits)):
        if (T_commits[j][0] > transacao_ckp[i][0]): # transação pode ser excluida no ckp
            #print("commit",T_commits[j][0])
            #print("ckp",transacao_ckp[i][0])
            list_redo.append(T_commits[j][1])

list_redo = sorted(set(list_redo))
print("list_redo", list_redo)


# Função para procedimento do redo
def realiza_redo(linha):
    print("linha",linha)
    # realizar as operações
    comp_start = arq_log[linha].find("start")
    comp_CKPT = arq_log[linha].find("CKPT")
    comp_commit = arq_log[linha].find("commit")
    comp_crash = arq_log[linha].find("crash")


    if comp_start != 1 and comp_CKPT != 1 and comp_commit != 1 and comp_crash != 1:
        #tem uma operação para verificar necessidade de fazer o redo
        # list_redo = [ T1 , T4 ]
        # <start T1>
        # <T3,2,B,30,1000>
        # <T1,1,A,20,2000>
        # < transaçao, linha, variavel, redo, undu>
        for j in range(len(arq_log[linha])):
            if j!= 0: #inicia leitura no index 1
                # encontrar transação atual e verificar se esta na lista do redo
                temp_x =""
                if (arq_log[i][l] == ","):
                    break
                else:
                    #temp_x += arq_log[i][j]
                    #print("temp_x",temp_x)
                    a=0

        


list_seq_redo = [] # para inicial pelo mais antigo (salva a linha)
# Buscar inicio das transações para realizar o redo
for j in range(len(list_redo)):
    for i in range(n_linhas_log-1,0,-1): # linhas de forma decremental
        linha_start = arq_log[i].find("start " + list_redo[j]) # valor 1 (encontrou), valor -1(não encontrou)
        
        if (linha_start == 1):
            #print("encontrou",arq_log[i])
            #print("linha", i)
            list_seq_redo.append(i)
            break

# Faz sequencia da lista mais antiga
list_seq_redo.sort()

# passar linhas a partir do inicio do redu
for i in range(n_linhas_log):
    realiza_redo(i + list_seq_redo[0]) 
    if (i + list_seq_redo[0] == n_linhas_log-1):
        break

#print(T_commits)
