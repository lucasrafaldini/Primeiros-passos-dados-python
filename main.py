# Importe o pandas e renomeie ele como pd
import pandas as pd

# Defina a função que será seu entrypoint
def main(filename="data.csv"):
    #Execute o método read_csv do pandas com o parâmetro definido anteriormente
    pd.read_csv(filename)

# Utilize o dunder name para se assegurar que o arquivo está sendo executado como entrypoint
# Caso ele esteja sendo executado como entrypoint, o ponto de partida é a função main
if __name__ = "__main__":
    # Defina uma variável com o nome do arquivo que será importado
    # csv_filename = "data.csv"
    main()