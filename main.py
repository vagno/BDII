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
#print("\nnumero de linhas do codigo: ",n_linhas_log)

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
        #print("commit na linha:",i)
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




# Função para atualizar dados do redo
def update_db(a,b,c):
    d = 0


# Função para procedimento do redo
def realiza_redo(linha):
    #print("linha",linha)
    # realizar as operações
    comp_start = arq_log[linha].find("start")
    comp_CKPT = arq_log[linha].find("CKPT")
    comp_commit = arq_log[linha].find("commit")
    comp_crash = arq_log[linha].find("crash")


    if comp_start != 1 and comp_CKPT != 1 and comp_commit != 1 and comp_crash != 1:
        #tem uma operação para verificar necessidade de fazer o redo
        #print("linha de transação", + linha)

        # encontrar transação atual e verificar se esta na lista do redo
        for k in range(len(list_redo)):
            t_list_redo = list_redo[k] + "," #
            temp = arq_log[linha].find(t_list_redo)
            #print ("t_list_redo",t_list_redo)

            aux = 0 # variavel para salvar os indices
            if temp == 1: # realizar redo nesta transaçao
                print(arq_log[linha])
                d_t = [list_redo[k]] # transação
                d_id = [] # id da tupla
                d_l = [] # coluna
                d_v = [] # Valor antigo
                ### <T1,1,A,20,2000>
                ### < transaçao, id, coluna, redo, undu>
                ### <transação, “id da tupla”,”coluna”, “valor antigo”, “valor novo”>.
                aux = len(list_redo[k]) + 2 # tamanho da transação na lista redu
                # se deslocando na transação pelo incremento do indice até encontar "," por dado
                temp_concatena = ""
                while (arq_log[linha][aux] != ","):
                    temp_concatena = temp_concatena + arq_log[linha][aux]
                    aux += 1
                d_id = temp_concatena
                aux += 1
                temp_concatena = ""
                while (arq_log[linha][aux] != ","):
                    temp_concatena = temp_concatena + arq_log[linha][aux]
                    aux += 1
                d_l = temp_concatena
                aux += 1
                temp_concatena = ""
                while (arq_log[linha][aux] != ","):
                    temp_concatena = temp_concatena + arq_log[linha][aux]
                    aux += 1
                d_v = temp_concatena

                print ("d_t",d_t)
                print ("d_id",d_id)
                print ("d_l",d_l)
                print ("d_v",d_v)

                # Chama função para fazer update no banco de dados passando os parametros
                update_db (d_id , d_l , d_v)

        


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
## umas acrecentadinha ao avanço do trabalho.
