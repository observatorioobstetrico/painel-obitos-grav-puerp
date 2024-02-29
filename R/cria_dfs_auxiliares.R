library(dplyr)
library(tidyverse)
library(getPass)
library(httr)

token = getPass()  #Token de acesso à API da PCDaS

url_base = "https://bigdata-api.fiocruz.br"

convertRequestToDF <- function(request){
  variables = unlist(content(request)$columns)
  variables = variables[names(variables) == "name"]
  column_names <- unname(variables)
  values = content(request)$rows
  df <- as.data.frame(do.call(rbind,lapply(values,function(r) {
    row <- r
    row[sapply(row, is.null)] <- NA
    rbind(unlist(row))
  } )))
  names(df) <- column_names
  return(df)
}

endpoint <- paste0(url_base,"/","sql_query")

# Obtendo um dataframe com o nome dos capítulos e categorias da CID10 -----
df_capitulos <- read.csv2("R/databases/CID-10-CAPITULOS.CSV") |>
  mutate(DESCRICAO = str_sub(DESCRICAO, nchar("Capítulo "), nchar(DESCRICAO)))

df_categorias <- read.csv2("R/databases/CID-10-CATEGORIAS.CSV")

df_cid10 <- data.frame(
  CAUSABAS = character(nrow(df_categorias)),
  capitulo_cid10 = character(nrow(df_categorias)),
  causabas_categoria = character(nrow(df_categorias))
)

for (i in 1:nrow(df_categorias)) {
  j <- 1
  parar <- FALSE
  while (!parar) {
    if (df_categorias$CAT[i] >= df_capitulos$CATINIC[j] & df_categorias$CAT[i] <= df_capitulos$CATFIM[j]) {
      df_cid10$CAUSABAS[i] <- df_categorias$CAT[i]
      df_cid10$capitulo_cid10[i] <- df_capitulos$DESCRICAO[j]
      df_cid10$causabas_categoria[i] <- df_categorias$DESCRICAO[i]
      parar <- TRUE
    } else {
      j <- j + 1
    }
  }
}

## Exportando o arquivo
write.csv(df_cid10, "R/databases/df_cid10.csv", row.names = FALSE)

# Obtendo um dataframe com as regiões, UFs e nomes de cada município -----
df_aux_municipios <- data.frame()
params = paste0('{
      "token": {
        "token": "',token,'"
      },
      "sql": {
        "sql": {"query":" SELECT res_codigo_adotado, res_MUNNOME, res_SIGLA_UF, res_REGIAO, COUNT(1)',
                ' FROM \\"datasus-sinasc\\"',
                ' GROUP BY res_codigo_adotado, res_MUNNOME, res_SIGLA_UF, res_REGIAO",
                        "fetch_size": 65000}
      }
    }')

request <- POST(url = endpoint, body = params, encode = "form")
df_aux_municipios <- convertRequestToDF(request)
names(df_aux_municipios) <- c("CODMUNRES", "municipio", "uf", "regiao", "nascidos")

df_aux_municipios <- df_aux_municipios |>
  select(!nascidos) |>
  arrange(CODMUNRES)

## Exportando o arquivo
write.csv(df_aux_municipios, "R/databases/df_aux_municipios.csv", row.names = FALSE)
