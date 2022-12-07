import csv

# Leitura arquivo de entrada do Log
arq = open('entradaLog', 'r')
arq_log = arq.readlines()
#arq_log = texto.split()
arq.close() 

#retorna qtd de linhas do log
n_linhas_log = len(arq_log)
#print("\nnumero de linhas do codigo: ",n_linhas_log)

#for i in range(n_linhas_log):
    #print(arq_log[i])


#encontrar commit
for i in range(n_linhas_log-1,0,-1): # linhas de forma decremental
    x = arq_log[i].find("commit") # valor 1 (encontrou), valor -1(não encontrou)
    if (x == 1):
        #print("commit na linha:",i)
        a=1