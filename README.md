# Comex Filter API
Api para o projeto criado para o Laboratório de Contas Regionais da Amazônia (LACAM) com o intuito de filtrar e auxiliar o economista a extrair informaçoes dos dados disponíveis na base de dados do Comercio Exterior Brasileiro

# Features
- Buscar dados nas tabelas de importação e exportação do comércio exterior.
  - Filtrar por ano de referência da tabela.
  - Filtrar por unidades regionais (apenas municípios atualmente) 
  - Modo de agregação.
    - Totalmente desagregado (requisita os dados na menor unidade e sem agregações)
    - Total exportado de cada produto de determinado(s) municípios
    - Total exportado de cada produto daquela região
    - Total exportado de cada produto de determinado(s) municípios, separados por país (origem ou destino)
    - Total exportado de cada produto daquela região, separados por país (origem ou destino)
- Buscar e listar os municípios Brasileiros.
- Buscar e listar as informações do sistema harmonizado.

# Autor
- José Vinícius - [GitHub](https://gist.github.com/jbsaraiva)

# Tecnologias
- Python
- Fastapi
