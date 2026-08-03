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

# Obtendo um dataframe com as regiões, UFs e nomes de cada município -----
df_aux_municipios <- data.frame()
params = paste0('{
      "token": {
        "token": "',token,'"
      },
      "sql": {
        "sql": {"query":" SELECT CODMUNRES, res_codigo_adotado, res_MUNNOME, res_SIGLA_UF, res_NOME_UF, res_REGIAO, COUNT(1)',
                ' FROM \\"datasus-sim\\"',
                ' GROUP BY CODMUNRES, res_codigo_adotado, res_MUNNOME, res_NOME_UF, res_SIGLA_UF, res_REGIAO",
                        "fetch_size": 65000}
      }
    }')

request <- POST(url = endpoint, body = params, encode = "form")
df_aux_municipios <- convertRequestToDF(request)
names(df_aux_municipios) <- c("codmunres", "res_codigo_adotado", "municipio", "uf", "nome_uf", "regiao", "nascidos")

df_aux_municipios <- df_aux_municipios |>
  select(!nascidos) |>
  arrange(codmunres)

## Exportando o arquivo
write.csv(df_aux_municipios, "R/databases/df_aux_municipios.csv", row.names = FALSE)

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

## Adicionando categorias faltantes
#df_cid10 <- read.csv("R/databases/df_cid10.csv")
df_cid10 <- df_cid10 |>
  mutate(
    causabas_categoria = case_when(
      causabas_categoria == "O97 Morte por sequelas de causas obstétricas" ~ "O97 Morte por sequelas de causas obstétricas", 
      causabas_categoria == "O14 Hipertensão gestacional (induzida pela gravidez) com proteinúria significativa" ~ "O14 Pré-eclâmpsia", 
      TRUE ~ causabas_categoria
    )
  ) |>
  add_row(
    causabas = "A090",
    capitulo_cid10 = "I - Algumas doenças infecciosas e parasitárias",
    grupo_cid10 = "(A90-A99) Febres por arbovírus e febres hemorrágicas virais",
    causabas_categoria = "A09 Diarréia e gastroenterite de origem infecciosa presumível",
    causabas_subcategoria = "A090 Outras gastroenterites e colites não especificadas de origem infecciosa"
  ) |>
  add_row(
    causabas = "A099",
    capitulo_cid10 = "I - Algumas doenças infecciosas e parasitárias",
    grupo_cid10 = "(A90-A99) Febres por arbovírus e febres hemorrágicas virais",
    causabas_categoria = "A09 Diarréia e gastroenterite de origem infecciosa presumível",
    causabas_subcategoria = "A099 Gastroenterite e colite de origem não especificada"
  ) |>
  add_row(
    causabas = "A97",
    capitulo_cid10 = "I - Algumas doenças infecciosas e parasitárias",
    grupo_cid10 = "(A90-A99) Febres por arbovírus e febres hemorrágicas virais",
    causabas_categoria = "A97 Dengue",
    causabas_subcategoria = "A97 Dengue"
  ) |>
  add_row(
    causabas = "A970",
    capitulo_cid10 = "I - Algumas doenças infecciosas e parasitárias",
    grupo_cid10 = "(A90-A99) Febres por arbovírus e febres hemorrágicas virais",
    causabas_categoria = "A97 Dengue",
    causabas_subcategoria = "A970 Dengue sem sinais de alerta"
  ) |>
  add_row(
    causabas = "A971",
    capitulo_cid10 = "I - Algumas doenças infecciosas e parasitárias",
    grupo_cid10 = "(A90-A99) Febres por arbovírus e febres hemorrágicas virais",
    causabas_categoria = "A97 Dengue",
    causabas_subcategoria = "A971 Dengue com sinais de alerta"
  ) |>
  add_row(
    causabas = "A972",
    capitulo_cid10 = "I - Algumas doenças infecciosas e parasitárias",
    grupo_cid10 = "(A90-A99) Febres por arbovírus e febres hemorrágicas virais",
    causabas_categoria = "A97 Dengue",
    causabas_subcategoria = "A972 Dengue grave"
  ) |>
  add_row(
    causabas = "A979",
    capitulo_cid10 = "I - Algumas doenças infecciosas e parasitárias",
    grupo_cid10 = "(A90-A99) Febres por arbovírus e febres hemorrágicas virais",
    causabas_categoria = "A97 Dengue",
    causabas_subcategoria = "A979 Dengue, não especificada"
  ) |>
  add_row(
    causabas = "D686",
    capitulo_cid10 = "III  - Doenças do sangue e dos órgãos hematopoéticos e alguns transtornos imunitários",
    grupo_cid10 = "(D65-D69) Defeitos da coagulação, púrpura e outras afecções hemorrágicas",
    causabas_categoria = "D68 Outros defeitos da coagulação",
    causabas_subcategoria = "D686 Outras trombofilias"
  ) |>
  add_row(
    causabas = "I483",
    capitulo_cid10 = "IX - Doenças do aparelho circulatório",
    grupo_cid10 = "(I30-I52) Outras formas de doença do coração",
    causabas_categoria = "I48 Flutter e fibrilação atrial",
    causabas_subcategoria = "I483 Flutter atrial típico"
  ) |>
  add_row(
    causabas = "I489",
    capitulo_cid10 = "IX - Doenças do aparelho circulatório",
    grupo_cid10 = "(I30-I52) Outras formas de doença do coração",
    causabas_categoria = "I48 Flutter e fibrilação atrial",
    causabas_subcategoria = "I489 Flutter e fibrilação atrial não especificados"
  ) |>
  add_row(
    causabas = "K358",
    capitulo_cid10 = "XI - Doenças do aparelho digestivo",
    grupo_cid10 = "(K35-K38) Doenças do apêndice",
    causabas_categoria = "K35 Apendicite aguda",
    causabas_subcategoria = "K358 Outras apendicites agudas e não especificadas"
  ) |>
  add_row(
    causabas = "O14",
    capitulo_cid10 = "XV - Gravidez, parto e puerpério",
    grupo_cid10 = "(O10-O16) Edema, proteinúria e transtornos hipertensivos na gravidez, no parto e no puerpério",
    causabas_categoria = "O14 Pré-eclâmpsia",
    causabas_subcategoria = "O14 Pré-eclâmpsia"
  ) |>
  add_row(
    causabas = "O142",
    capitulo_cid10 = "XV - Gravidez, parto e puerpério",
    grupo_cid10 = "(O10-O16) Edema, proteinúria e transtornos hipertensivos na gravidez, no parto e no puerpério",
    causabas_categoria = "O14 Pré-eclâmpsia",
    causabas_subcategoria = "O142 Síndrome HELLP"
  ) |>
  add_row(
    causabas = "O432",
    capitulo_cid10 = "XV - Gravidez, parto e puerpério",
    grupo_cid10 = "(O30-O48) Assistência prestada à mãe por motivos ligados ao feto e à cavidade amniótica e por possíveis problemas relativos ao parto",
    causabas_categoria = "O43 Transtornos da placenta",
    causabas_subcategoria = "O432 Aderência mórbida da placenta"
  ) |>
  add_row(
    causabas = "O62",
    capitulo_cid10 = "XV - Gravidez, parto e puerpério",
    grupo_cid10 = "(O60-O75) Complicações do trabalho de parto e do parto",
    causabas_categoria = "O62 Anormalidades da contração uterina",
    causabas_subcategoria = "O62 Anormalidades da contração uterina"
  ) |>
  add_row(
    causabas = "O72",
    capitulo_cid10 = "XV - Gravidez, parto e puerpério",
    grupo_cid10 = "(O60-O75) Complicações do trabalho de parto e do parto",
    causabas_categoria = "O72 Hemorragia pós-parto",
    causabas_subcategoria = "O72 Hemorragia pós-parto"
  ) |>
  add_row(
    causabas = "O937",
    capitulo_cid10 = "XV - Gravidez, parto e puerpério",
    grupo_cid10 = "(O93-O93) - Causas externas relacionadas com morte materna",
    causabas_categoria = "O93 Causas externas relacionadas com morte materna",
    causabas_subcategoria = "O937 Morte materna associada a agressão"
  ) |>
  add_row(
    causabas = "O960",
    capitulo_cid10 = "XV - Gravidez, parto e puerpério",
    grupo_cid10 = "(O94-O99) Outras afecções obstétricas não classificadas em outra parte",
    causabas_categoria = "O96 Morte, por qualquer causa obstétrica, que ocorre mais de 42 dias, mas menos de 1 ano, após o parto",
    causabas_subcategoria = "O960 Morte materna após 42 dias, mas antes de um ano, após o parto, devida a causa obstétrica direta"
  ) |>
  add_row(
    causabas = "O961",
    capitulo_cid10 = "XV - Gravidez, parto e puerpério",
    grupo_cid10 = "(O94-O99) Outras afecções obstétricas não classificadas em outra parte",
    causabas_categoria = "O96 Morte, por qualquer causa obstétrica, que ocorre mais de 42 dias, mas menos de 1 ano, após o parto",
    causabas_subcategoria = "O961 Morte materna após 42 dias, mas antes de um ano, após o parto, devida a causa obstétrica indireta"
  ) |>
  add_row(
    causabas = "O969",
    capitulo_cid10 = "XV - Gravidez, parto e puerpério",
    grupo_cid10 = "(O94-O99) Outras afecções obstétricas não classificadas em outra parte",
    causabas_categoria = "O96 Morte, por qualquer causa obstétrica, que ocorre mais de 42 dias, mas menos de 1 ano, após o parto",
    causabas_subcategoria = "O969 Morte materna após 42 dias, mas antes de um ano, após o parto, devida a causa obstétrica não especificada"
  ) |>
  add_row(
    causabas = "O970",
    capitulo_cid10 = "XV - Gravidez, parto e puerpério",
    grupo_cid10 = "(O94-O99) Outras afecções obstétricas não classificadas em outra parte",
    causabas_categoria = "O97 Morte por sequelas de causas obstétricas",
    causabas_subcategoria = "O970 Morte materna por sequelas de causa obstétrica direta"
  ) |>
  add_row(
    causabas = "O971",
    capitulo_cid10 = "XV - Gravidez, parto e puerpério",
    grupo_cid10 = "(O94-O99) Outras afecções obstétricas não classificadas em outra parte",
    causabas_categoria = "O97 Morte por sequelas de causas obstétricas",
    causabas_subcategoria = "O970 Morte materna por sequelas de causa obstétrica indireta"
  ) |>
  add_row(
    causabas = "O979",
    capitulo_cid10 = "XV - Gravidez, parto e puerpério",
    grupo_cid10 = "(O94-O99) Outras afecções obstétricas não classificadas em outra parte",
    causabas_categoria = "O97 Morte por sequelas de causas obstétricas",
    causabas_subcategoria = "O979 Morte materna por sequelas de causa obstétrica não especificada"
  ) |>
  arrange(causabas)

## Exportando o arquivo
write.csv(df_cid10, "R/databases/df_cid10.csv", row.names = FALSE)


