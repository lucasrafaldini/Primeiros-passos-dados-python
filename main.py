# Importe o pandas e renomeie ele como pd
import pandas as pd


def count_most_common_genres(dataframe):
    #Atribua o valor da coluna genres do dataframe à variável genres
    genres=dataframe['genres']
    # Converta a coluna à uma string/linha
    genres_string=genres.str
    # Agora separe os itens da string com vírgula 
    string_splitted = genres_string.split(', ')
    # Agora separe os itens agrupados em listas como itens individuais por linha 
    stri_expl=string_splitted.explode()
    # Finalmente chame a função value_count para contar os gêneros 
    count=stri_expl.value_counts()
    print(count)

def rating_mean(dataframe):
    media_rating = dataframe['rating'].mean()
    print(f"Média de Avaliações (Rating) Geral: {media_rating}")
    # f string é uma string que interpola textos com valores de variáveis
    # O que está entre chaves {media_rating} significa um valor de uma variável, ou seja, não fixado


# Defina a função que será seu entrypoint
def main(filename="data.csv"):
    #Execute o método read_csv do pandas com o parâmetro definido anteriormente
    data=pd.read_csv(filename)
    # Chame aqui a contagem dos gêneros mais comuns da lista
    count_most_common_genres(data)
    # Aqui eu vou chamar uma função para calcular a média das ratings
    rating_mean(data)

# Utilize o dunder name para se assegurar que o arquivo está sendo executado como entrypoint
# Caso ele esteja sendo executado como entrypoint, o ponto de partida é a função main
if __name__ = "__main__":
    # Defina uma variável com o nome do arquivo que será importado
    # csv_filename = "data.csv"
    main()