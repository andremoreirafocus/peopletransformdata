# People Transform Data

Este projeto realiza a transformação e consolidação de arquivos JSON armazenados no MinIO em arquivos Parquet, otimizando o armazenamento e análise de dados.

## Principais Funcionalidades

- **Listagem de arquivos JSON**: O programa lista todos os arquivos JSON em uma pasta específica do bucket MinIO, baseada em data e hora.
- **Leitura dos arquivos JSON**: Cada arquivo JSON é lido diretamente do MinIO.
- **Flatten dos dados**: Os dados JSON são "achatados" (flatten) para um dicionário de um único nível, ignorando campos desnecessários como imagens, informações extras e códigos postais.
- **Consolidação dos dados**: Todos os registros processados são agregados em uma única estrutura tabular.
- **Conversão para Parquet**: Os dados consolidados são convertidos para o formato Parquet, eficiente para análise e armazenamento.
- **Upload para MinIO**: O arquivo Parquet consolidado é salvo de volta no MinIO, em um bucket e pasta definidos pelo usuário.

## Como funciona

1. O programa identifica todos os arquivos JSON em uma pasta do bucket de origem (ex: `raw/year=2025/month=12/day=19/hour=14/`).
2. Cada arquivo é lido, processado e transformado em um registro tabular.
3. Todos os registros são consolidados em um único DataFrame.
4. O DataFrame é convertido em um arquivo Parquet.
5. O arquivo Parquet é enviado para o bucket de destino (ex: `processed/year=2025/month=12/day=19/hour=14/consolidated-2025121914.parquet`).

## Requisitos
- Python 3.8+
- MinIO em execução
- Bibliotecas: pandas, pyarrow, minio


## Execução

1. Configure as variáveis de acesso ao MinIO no início do arquivo `main.py`.
2. Execute o script:

```bash
python3 main.py
```

O programa irá processar todos os arquivos JSON do período configurado e gerar um único arquivo Parquet consolidado no bucket de destino.

## Exemplo de arquivo Parquet

Um arquivo de exemplo gerado pelo programa está disponível:

- [`sample-consolidated-2025121914.parquet`](sample-consolidated-2025121914.parquet.as.json)

Este arquivo pode ser utilizado para testar a leitura, análise ou integração com outras ferramentas de dados.

---

Este projeto é ideal para pipelines de dados que precisam transformar e consolidar grandes volumes de arquivos JSON em formatos otimizados para análise.