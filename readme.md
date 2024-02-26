##### Português
## Subscription/Devices - Basic ETL - Medallion Architecture

Neste projeto, poderemos conhecer de maneira simplificada o conceito de arquitetura medalhão para
o tratamento dos dados recebidos no storage até a entrega da tabela final que será utilizada pelos times
de Data Analytics e Data Science.

Os arquivos ingeridos estão em formato .json. A sessão spark inicializada é em delta, assim como as tabelas
geradas nas camadas Bronze, Silver e Gold, contudo é perfeitamente possível inicializar uma sessão vanilla
do spark e salvar os arquivos em formato parquet.

--
##### Inglês
Subscription/Devices - Basic ETL - Medallion Architecture
In this project, we will explore in a simplified manner the concept of medallion architecture for handling the data received in storage until the delivery of the final table that will be used by Data Analytics and Data Science teams.

The ingested files are in .json format. The Spark session initialized is in delta, as well as the tables generated in the Bronze, Silver, and Gold layers. However, it is perfectly possible to initialize a vanilla Spark session and save the files in parquet format.