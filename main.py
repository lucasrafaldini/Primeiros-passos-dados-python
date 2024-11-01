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

def crossing_best_worst_movies_by_year(best_dataframe, worst_dataframe):
    # Garantir que as colunas 'releaseYear' e 'averageRating' estão em ambos os DataFrames
    melhores_filmes = best_dataframe[['title', 'releaseYear', 'averageRating']]
    piores_filmes = worst_dataframe[['title', 'releaseYear', 'averageRating']]

    # Agrupar por ano e pegar o melhor filme (maior nota) e o pior filme (menor nota)
    melhores_por_ano = melhores_filmes.loc[melhores_filmes.groupby('releaseYear')['averageRating'].idxmax()]
    piores_por_ano = piores_filmes.loc[piores_filmes.groupby('releaseYear')['averageRating'].idxmin()]
    # loc é uma função para agrupar linhas e colunas em um recorte de informações desejado
    # idx = index, é uma função para pegar o índice de um valor específico

    # Renomear as colunas para facilitar a identificação, pois ambas possuem colunas com o mesmo nome e quero facilitar o entendimento no novo dataframe
    melhores_por_ano = melhores_por_ano.rename(columns={'title': 'best_movie', 'averageRating': 'best_rating'})
    piores_por_ano = piores_por_ano.rename(columns={'title': 'worst_movie', 'averageRating': 'worst_rating'})

    # Juntar os dataframes pelo ano
    analise_cruzada = pd.merge(melhores_por_ano, piores_por_ano, on='releaseYear', how='inner')

    # Para finalizar, vamos pedir para exibir o resultado
    print("DataFrame com os Melhores e Piores Filmes por Ano:")
    print(analise_cruzada)

# Defina a função que será seu entrypoint
def main(best_filename="1000-best-movies-imdb.csv", worst_filename="1000-worst-movies-imdb.csv"):
    #Execute o método read_csv do pandas com o parâmetro definido anteriormente
    best_data=pd.read_csv(best_filename)
    # Chame aqui a contagem dos gêneros mais comuns da lista
    count_most_common_genres(best_data)
    # Aqui eu vou chamar uma função para calcular a média das ratings
    rating_mean(best_data)
    # O csv com os melhores filmes já foi importado com o nome de data, agora precisamos importar o dataframe dos piores filmes  
    worst_data=pd.read_csv(worst_filename)
    # Agora vamos chamar a função que cruza os melhores e piores filmes por ano
    crossing_best_worst_movies_by_year(best_data, worst_data)

# Utilize o dunder name para se assegurar que o arquivo está sendo executado como entrypoint
# Caso ele esteja sendo executado como entrypoint, o ponto de partida é a função main
if __name__ = "__main__":
    # Defina uma variável com o nome do arquivo que será importado
    # csv_filename = "data.csv"
    main()