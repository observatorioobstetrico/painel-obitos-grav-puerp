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
  mutate(
    DESCRICAO = str_sub(DESCRICAO, nchar("Capítulo ") + 1, nchar(DESCRICAO)),
    across(
      everything(),
      ~ str_replace_all(., "\\[", "(") |>
        str_replace_all("\\]", ")") |>
        str_replace_all("ü", "u"))
  ) 


df_categorias <- read.csv2("R/databases/CID-10-CATEGORIAS.CSV") |>
  mutate(
    across(
      everything(),
      ~ str_replace_all(., "\\[", "(") |>
        str_replace_all("\\]", ")") |>
        str_replace_all("ü", "u"))
  ) 

df_subcategorias <- read.csv2("R/databases/CID-10-SUBCATEGORIAS.CSV") |>
  mutate(
    CAUSABAS = str_sub(SUBCAT, 1, 3),
    causabas_subcategoria = paste(SUBCAT, DESCRICAO),
    across(
      everything(),
      ~ str_replace_all(., "\\[", "(") |>
        str_replace_all("\\]", ")") |>
        str_replace_all("ü", "u"))
  )

df_grupos <- read.csv2("R/databases/CID-10-GRUPOS.CSV") |>
  mutate(
    across(
      everything(),
      ~ str_replace_all(., "\\[", "(") |>
        str_replace_all("\\]", ")") |>
        str_replace_all("ü", "u"))
  )

df_cid10_ms <- data.frame(
  causabas = character(nrow(df_categorias)),
  capitulo_cid10 = character(nrow(df_categorias)),
  grupo_cid10 = character(nrow(df_categorias)),
  causabas_categoria = character(nrow(df_categorias))
)

for (i in 1:nrow(df_categorias)) {
  j <- 1
  k <- 1
  parar <- FALSE
  while (!parar) {
    if (df_categorias$CAT[i] >= df_capitulos$CATINIC[j] & df_categorias$CAT[i] <= df_capitulos$CATFIM[j]) {
      if (df_categorias$CAT[i] >= df_grupos$CATINIC[k] & df_categorias$CAT[i] <= df_grupos$CATFIM[k]) {
        df_cid10_ms$causabas[i] <- df_categorias$CAT[i]
        df_cid10_ms$capitulo_cid10[i] <- df_capitulos$DESCRICAO[j]
        df_cid10_ms$grupo_cid10[i] <- glue::glue("({df_grupos$CATINIC[k]}-{df_grupos$CATFIM[k]}) {df_grupos$DESCRICAO[k]}")
        df_cid10_ms$causabas_categoria[i] <- paste(df_categorias$CAT[i], df_categorias$DESCRICAO[i])
        parar <- TRUE
      } else {
        k <- k + 1
      }
    } else {
      j <- j + 1
    }
  }
}

df_cid10_ms_completo <- right_join(df_cid10_ms, df_subcategorias |> clean_names() |> select(!c(descricao))) |>
  janitor::clean_names() |>
  select(!causabas) |>
  select(causabas = subcat, capitulo_cid10, grupo_cid10, causabas_categoria, causabas_subcategoria)

## Juntando com algumas CAUSABAS que aparecem nos dados do SIM, mas que só têm descrição na PCDaS
df_cid10_pcdas_aux <- data.frame()
params = paste0('{
      "token": {
        "token": "',token,'"
      },
      "sql": {
        "sql": {"query":" SELECT CAUSABAS, causabas_capitulo, causabas_grupo, causabas_categoria, causabas_subcategoria, COUNT(1)',
                ' FROM \\"datasus-sim\\"',
                ' GROUP BY CAUSABAS, causabas_capitulo, causabas_grupo, causabas_categoria, causabas_subcategoria",
                        "fetch_size": 65000}
      }
    }')

request <- POST(url = endpoint, body = params, encode = "form")
df_cid10_pcdas_aux <- convertRequestToDF(request)
names(df_cid10_pcdas_aux) <- c("causabas", "capitulo_cid10", "grupo_cid10", "causabas_categoria", "causabas_subcategoria", "obitos")

df_cid10_pcdas <- df_cid10_pcdas_aux |>
  get_dupes(causabas) |>
  select(!dupe_count) |>
  filter(!is.na(causabas_subcategoria)) |>
  full_join(df_cid10_pcdas_aux |> filter(!(causabas %in% get_dupes(df_cid10_pcdas_aux, causabas)$causabas))) |>
  select(!obitos)

## Arrumando o nome de alguns dos capítulos e categorias (que vieram ruins da PCDaS)
df_cid10 <- full_join(df_cid10_ms_completo, df_cid10_pcdas |> filter(!(causabas %in% df_cid10_ms_completo$causabas))) |>
  mutate(
    capitulo_cid10 = case_when(
      capitulo_cid10 == "XX.  Causas externas de morbidade e mortalidade" ~ "XX - Causas externas de morbidade e de mortalidade",
      capitulo_cid10 == "XV.  Gravidez parto e puerpério" ~ "XV - Gravidez, parto e puerpério",
      capitulo_cid10 == "I.   Algumas doenças infecciosas e parasitárias" ~ "I - Algumas doenças infecciosas e parasitárias",
      capitulo_cid10 == "II.  Neoplasias (tumores)" ~ "II - Neoplasias (tumores)",
      capitulo_cid10 == "II.  Neoplasias (tumores)" ~ "II - Neoplasias (tumores)",
      .default = capitulo_cid10
    ),
    causabas_categoria = case_when(
      causabas_categoria == "O60   Trabalho de parto pre-termo" ~ "O60 Trabalho de parto pré-termo",
      causabas_categoria == "O98   Doen inf paras mat COP compl grav part puerp" ~ "O98 Doenças infecciosas e parasitárias maternas classificáveis em outra parte mas que compliquem a gravidez, o parto e o puerpério",
      .default = causabas_categoria
    )
  ) |>
  arrange(causabas)

## Exportando o arquivo
write.csv(df_cid10, "R/databases/df_cid10.csv", row.names = FALSE)

# Obtendo um dataframe com as regiões, UFs e nomes de cada município -----
df_aux_municipios <- data.frame()
params = paste0('{
      "token": {
        "token": "',token,'"
      },
      "sql": {
        "sql": {"query":" SELECT CODMUNRES, res_codigo_adotado, res_MUNNOME, res_SIGLA_UF, res_REGIAO, COUNT(1)',
                ' FROM \\"datasus-sim\\"',
                ' GROUP BY CODMUNRES, res_codigo_adotado, res_MUNNOME, res_SIGLA_UF, res_REGIAO",
                        "fetch_size": 65000}
      }
    }')

request <- POST(url = endpoint, body = params, encode = "form")
df_aux_municipios <- convertRequestToDF(request)
names(df_aux_municipios) <- c("codmunres", "res_codigo_adotado", "municipio", "uf", "regiao", "nascidos")

df_aux_municipios <- df_aux_municipios |>
  select(!nascidos) |>
  arrange(codmunres)

## Exportando o arquivo
write.csv(df_aux_municipios, "R/databases/df_aux_municipios.csv", row.names = FALSE)
