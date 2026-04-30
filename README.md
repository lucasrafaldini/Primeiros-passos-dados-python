# Primeiros-passos-dados-python
Um repositório com scripts e notebooks para quem está começando a jornada com Python e dados. Orgulhosamente feito para ajudar meu amor a voar mais alto com a ajuda da tecnologia.

## Projeto: Base Unica de Quotes

Objetivo geral: transformar todos os arquivos da pasta `quotes/` em uma base unica para praticar um fluxo completo de dados.

### Objetivos por etapa

1. Coleta: varrer recursivamente a pasta `quotes/` e ler arquivos `.csv` e `.json`.
2. Limpeza: padronizar colunas, corrigir caracteres invalidos, normalizar texto, autor e tags.
3. Conversao: consolidar tudo em um schema unico (`quote`, `author`, `tags`, `source_file`, metadados).
4. Qualidade: remover linhas vazias e duplicatas por chave deterministica (`quote + author`).
5. Analise: gerar metricas de volume, autores unicos, principais autores e principais tags.
6. Visualizacao (proximo passo): usar os arquivos de resumo para criar graficos de frequencia.

### Script de consolidacao

Arquivo: `quotes_pipeline.py`

Executar:

```bash
python3 quotes_pipeline.py
```

Opcoes:

```bash
python3 quotes_pipeline.py --quotes-dir quotes --output-dir data/processed
```

### Saidas geradas

- `data/processed/quotes_unified.csv`
- `data/processed/quotes_unified.json`
- `data/processed/quotes_summary.csv`
- `data/processed/top_20_authors.csv`
- `data/processed/top_30_tags.csv`
