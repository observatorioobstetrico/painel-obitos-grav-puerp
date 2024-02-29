library(microdatasus)
library(tidyverse)
library(httr)
library(janitor)
library(getPass)
library(repr)
library(data.table)
library(readr)

# Lendo dataframes auxiliares (criados em cria_dfs_auxiliares.R) ----------
#df_cid10 <- read.csv("R/databases/df_cid10.csv")
df_aux_municipios <- read.csv("R/databases/df_aux_municipios.csv") |>
  mutate_if(is.numeric, as.character)

token = getPass()  #Token de acesso à API da PCDaS (todos os arquivos gerados se encontram na pasta "Databases", no Google Drive)

url_base = "https://bigdata-api.fiocruz.br"
endpoint <- paste0(url_base,"/","sql_query")

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

df_cid10_aux <- data.frame()
params = paste0('{
      "token": {
        "token": "',token,'"
      },
      "sql": {
        "sql": {"query":" SELECT CAUSABAS, causabas_capitulo, causabas_categoria, COUNT(1)',
                ' FROM \\"datasus-sim\\"',
                ' GROUP BY CAUSABAS, causabas_capitulo, causabas_categoria",
                        "fetch_size": 65000}
      }
    }')

request <- POST(url = endpoint, body = params, encode = "form")
df_cid10_aux <- convertRequestToDF(request)
names(df_cid10_aux) <- c("CAUSABAS", "capitulo_cid10", "causabas_categoria", "obitos")

df_cid10 <- df_cid10_aux |>
  select(!obitos) |>
  full_join(
    data.frame(
      CAUSABAS = c("O839", "O280", "O301", "O600"),
      capitulo_cid10 = rep("XV.  Gravidez parto e puerpério", 4),
      causabas_categoria = c(
        "O83 Outros tipos de parto único assistido",
        'O28 Achados anormais do rastreamento ("screening") antenatal da mãe',
        "O30 Gestação múltipla",
        "O60 Trabalho de parto pré-termo"
      )
    )
  )

rm(df_cid10_aux)


# Baixando os dados preliminares do SIM de 2023 ---------------------------
download.file("https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SIM/DO22OPEN.csv", "R/databases/DO22OPEN.csv", mode = "wb")
download.file("https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SIM/Mortalidade_Geral_2023.csv", "R/databases/DO23OPEN.csv", mode = "wb")

dados_preliminares_2022_aux <- read.csv2("R/databases/DO22OPEN.csv") 
dados_preliminares_2023_aux <- read.csv2("R/databases/DO23OPEN.csv") |>
  select(!contador)

dados_preliminares_2023_aux2 <- full_join(dados_preliminares_2022_aux, dados_preliminares_2023_aux)

## Fazendo as manipulações necessárias ------------------------------------
dados_preliminares_2023 <- dados_preliminares_2023_aux2 |>
  mutate_if(is.numeric, as.character) |>
  mutate(
    #CAUSABAS = str_sub(CAUSABAS, 1, 3),
    CAUSABAS = ifelse(CAUSABAS %in% c("O935", "O937"), "O95", CAUSABAS),
    OBITOGRAV = ifelse(is.na(OBITOGRAV), "9", OBITOGRAV),
    OBITOPUERP = ifelse(is.na(OBITOPUERP), "9", OBITOPUERP),
    ano = as.numeric(substr(DTOBITO, nchar(DTOBITO) - 3, nchar(DTOBITO))),
    idade = as.numeric(
      ifelse(
        IDADE == "999" | is.na(IDADE),
        99,
        ifelse(
          as.numeric(IDADE) >= 400 & as.numeric(IDADE) <= 499,
          substr(IDADE, 2, 3), 
          0
        )
      )
    ),
    racacor = case_when(
      RACACOR == "1" ~ "Branca",
      RACACOR == "2" ~ "Preta",
      RACACOR == "3" ~ "Amarela",
      RACACOR == "4" ~ "Parda",
      RACACOR == "5" ~ "Indígena",
      is.na(RACACOR) | RACACOR == "9" ~ "Ignorado"
    ),
    escolaridade = case_when(
      ESC2010 == "0" ~ "Sem escolaridade",
      ESC2010 == "1" ~ "Fundamental I",
      ESC2010 == "2" ~ "Fundamental II",
      ESC2010 == "3" ~ "Médio",
      ESC2010 == "4" ~ "Superior incompleto",
      ESC2010 == "5" ~ "Superior completo",
      is.na(ESC2010) | ESC2010 == "9" ~ "Ignorado"
    ),
    est_civil = case_when(
      ESTCIV == "1" ~ "Solteiro",
      ESTCIV == "2" ~ "Casado",
      ESTCIV == "3" ~ "Viúvo",
      ESTCIV == "4" ~ "Separado Judic./Divorciado",
      ESTCIV == "5" ~ "União Estável",
      is.na(ESTCIV) | ESTCIV == "9" ~ "Ignorado"
    ),
    local_ocorrencia_obito = case_when(
      LOCOCOR == "1" ~ "Hospital",
      LOCOCOR == "2" ~ "Outro Estab. Saúde",
      LOCOCOR == "3" ~ "Domicílio",
      LOCOCOR == "4" ~ "Via Pública",
      LOCOCOR == "5" ~ "Outros",
      is.na(LOCOCOR) | LOCOCOR == "9" | LOCOCOR == "6" ~ "Ignorado"
    ),
    assistencia_med = case_when(
      ASSISTMED == "1" ~ "Com assistência",
      ASSISTMED == "2" ~ "Sem assistência",
      is.na(ASSISTMED) | ASSISTMED == "9" ~ "Ignorado"
    ),
    necropsia = case_when(
      NECROPSIA == "1" ~ "Sim",
      NECROPSIA == "2" ~ "Não",
      is.na(NECROPSIA) | NECROPSIA == "9" ~ "Ignorado"
    ),
    obito_em_idade_fertil = if_else(
      condition = as.numeric(idade) >= 10 & as.numeric(idade) <= 49,
      true = "Sim",
      false = "Não"
    ),
    tipo_de_morte_materna = if_else(
      condition = (CAUSABAS >= "B200" & CAUSABAS <= "B249") |
        (CAUSABAS >= "O100" & CAUSABAS <= "O109") |
        ((CAUSABAS >= "O240" & CAUSABAS != "O244") & CAUSABAS <= "O259") |
        (CAUSABAS == "O94") |
        (CAUSABAS >= "O980" & CAUSABAS <= "O999"),
      true = "Indireta",
      false = if_else(CAUSABAS == "O95", true = "Não especificada", false = "Direta")
    ),
    # tipo_de_morte_materna = if_else(
    #   condition = (CAUSABAS == "B20") |
    #     (CAUSABAS == "O10") |
    #     ((CAUSABAS >= "O24") & CAUSABAS <= "O25") |
    #     (CAUSABAS == "O94") |
    #     (CAUSABAS >= "O98" & CAUSABAS <= "O99"),
    #   true = "Indireta",
    #   false = if_else(CAUSABAS == "O95", true = "Não especificada", false = "Direta")
    # ),
    periodo_do_obito = case_when(
      OBITOGRAV == "1" & OBITOPUERP != "1" & OBITOPUERP != "2" ~ "Durante a gravidez, parto ou aborto",
      OBITOGRAV != "1" & OBITOPUERP == "1" ~ "Durante o puerpério, até 42 dias",
      OBITOGRAV != "1" & OBITOPUERP == "2" ~ "Durante o puerpério, de 43 dias a menos de 1 ano",
      (OBITOGRAV == "2" & OBITOPUERP == "3") | (OBITOGRAV == "2" & OBITOPUERP == "9") | (OBITOGRAV == "9" & OBITOPUERP == "3")  ~ "Não na gravidez ou no puerpério",
      OBITOGRAV == "9" & OBITOPUERP == "9" ~ "Não informado ou ignorado",
      (OBITOGRAV == "1" & OBITOPUERP == "1") | (OBITOGRAV == "1" & OBITOPUERP == "2") ~ "Período inconsistente"
    ),
    investigacao_cmm = if_else(
      FONTEINV == "1",
      true = "Sim", 
      false = if_else(FONTEINV == "9", true = "Sem informação", false = "Não",  missing = "Sem informação"),
      missing = "Sem informação"
    )
  ) |>
  #left_join(df_cid10 |> mutate(CAUSABAS = substr(CAUSABAS, 1, 3)) |> unique()) |>
  left_join(df_cid10) |>
  left_join(df_aux_municipios) 


# Para a seção de óbitos maternos oficiais --------------------------------
## Lendo o arquivo com os óbitos maternos de 1996 a 2022 ------------------
# dados_obitos_maternos_1996_2022 <- read.csv("R/databases/obitos_maternos_muni_1996_2022.csv") |>
#   mutate(codigo = as.character(codigo))

dados_obitos_maternos_1996_2022 <- read.csv("R/databases/Obitos_maternos_muni2021.csv") |>
  mutate(codigo = as.character(codigo))

## Filtrando, nos dados preliminares, apenas os óbitos maternos -----------
df_maternos_preliminares <- dados_preliminares_2023 |>
  filter(
    SEXO == "2",
    ((CAUSABAS >= "O000"  &  CAUSABAS <= "O959") |
       (CAUSABAS >= "O980"  &  CAUSABAS <= "O999") |
       (CAUSABAS == "A34" & OBITOPUERP != "2") |
       ((CAUSABAS >= "B200"  &  CAUSABAS <= "B249") & (OBITOGRAV == "1" | OBITOPUERP == "1")) |
       (CAUSABAS == "D392" & (OBITOGRAV == "1" | OBITOPUERP == "1")) |
       (CAUSABAS == "E230" & (OBITOGRAV == "1" | OBITOPUERP == "1")) |
       ((CAUSABAS >= "F530"  &  CAUSABAS <= "F539") & (OBITOPUERP != "2")) |
       (CAUSABAS == "M830" & OBITOPUERP != "2"))
  ) |>
  mutate(
    obitos = 1,
    .keep = "unused",
  ) |>
  select(
    codigo = CODMUNRES, municipio, uf, regiao, ano, CAUSABAS, capitulo_cid10, causabas_categoria,
    tipo_de_morte_materna, periodo_do_obito, investigacao_cmm, racacor, idade, obitos
  ) |>
  group_by(across(!obitos)) |>
  summarise(obitos = sum(obitos)) |>
  ungroup() |>
  arrange(codigo) |>
  as.data.frame()

get_dupes(df_maternos_preliminares)
sum(df_maternos_preliminares$obitos[which(df_maternos_preliminares$ano == 2023)])
sum(df_maternos_preliminares$obitos[which(df_maternos_preliminares$ano == 2022)])
df_maternos_preliminares[is.na(df_maternos_preliminares), ]

## Juntando as duas bases -------------------------------------------------
df_obitos_maternos <- full_join(dados_obitos_maternos_1996_2022, df_maternos_preliminares)

## Exportando os dados -----------------------------------------------------
write.table(df_obitos_maternos, 'dados_oobr_obitos_grav_puerp_maternos_oficiais_2023.csv', sep = ",", dec = ".", row.names = FALSE)


# Para a seção de análise cruzada -----------------------------------------
## Lendo o arquivo com os óbitos maternos p/ essa seção de 96 a 2022 ------
# dados_ac_1996_2022 <- read.csv("R/databases/obitos_maternos_estendidos_1996_2022.csv") |>
#   mutate(codigo = as.character(codigo))

dados_ac_1996_2022 <- read.csv("R/databases/Obitos_maternos_estendidos_1996_2021.csv") |>
  mutate(codigo = as.character(codigo)) |>
  select(
    codigo, municipio, uf, regiao, ano = ano_obito, racacor = raca_cor, est_civil, escolaridade, idade = idade_obito, 
    local_ocorrencia_obito, assistencia_med, necropsia, tipo_de_morte_materna, periodo_do_obito, obito_em_idade_fertil,
    investigacao_cmm, capitulo_cid10 = causabas_capitulo
  )

## Filtrando, nos dados preliminares, apenas os óbitos maternos -----------
df_ac_preliminares <- dados_preliminares_2023 |>
  filter(
    SEXO == "2",
    ((CAUSABAS >= "O000"  &  CAUSABAS <= "O959") |
       (CAUSABAS >= "O980"  &  CAUSABAS <= "O999") |
       (CAUSABAS == "A34" & OBITOPUERP != "2") |
       ((CAUSABAS >= "B200"  &  CAUSABAS <= "B249") & (OBITOGRAV == "1" | OBITOPUERP == "1")) |
       (CAUSABAS == "D392" & (OBITOGRAV == "1" | OBITOPUERP == "1")) |
       (CAUSABAS == "E230" & (OBITOGRAV == "1" | OBITOPUERP == "1")) |
       ((CAUSABAS >= "F530"  &  CAUSABAS <= "F539") & (OBITOPUERP != "2")) |
       (CAUSABAS == "M830" & OBITOPUERP != "2"))
  ) |>
  select(
    codigo = CODMUNRES, municipio, uf, regiao, ano, racacor, est_civil, escolaridade, idade, 
    local_ocorrencia_obito, assistencia_med, necropsia, tipo_de_morte_materna, periodo_do_obito,
    obito_em_idade_fertil, investigacao_cmm, capitulo_cid10
  ) 

nrow(df_ac_preliminares[which(df_ac_preliminares$ano == 2023), ])
nrow(df_ac_preliminares[which(df_ac_preliminares$ano == 2022), ])
df_ac_preliminares[is.na(df_ac_preliminares), ]

##Juntando as duas bases
df_obitos_maternos_ac <- full_join(dados_ac_1996_2022, df_ac_preliminares)

##Exportando os dados 
write.table(df_obitos_maternos_ac, 'dados_oobr_obitos_grav_puerp_analise_cruzada_2023.csv', sep = ",", dec = ".", row.names = FALSE)


# Para a seção de óbitos maternos desconsiderados -------------------------
## Lendo o arquivo com os óbitos desconsiderados de 1996 a 2022 -----------
# dados_desconsiderados_1996_2022 <- read.csv("R/databases/obitos_desconsiderados_muni_1996_2022.csv") |>
#   mutate(codigo = as.character(codigo))

dados_desconsiderados_1996_2022 <- read.csv("R/databases/Obitos_desconsiderados_muni2021.csv") |>
  mutate(codigo = as.character(codigo))

## Filtrando, nos dados preliminares, apenas pelos óbitos descons. --------
df_descons_preliminares <- dados_preliminares_2023 |>
  filter(
    SEXO == "2",
    OBITOGRAV == "1" | OBITOPUERP == "1" | OBITOPUERP == "2",
    !(((CAUSABAS >= "O000"  &  CAUSABAS <= "O959") |
         (CAUSABAS >= "O980"  &  CAUSABAS <= "O999") |
         (CAUSABAS == "A34" & OBITOPUERP != "2") |
         ((CAUSABAS >= "B200"  &  CAUSABAS <= "B249") & (OBITOGRAV == "1" | OBITOPUERP == "1")) |
         (CAUSABAS == "D392" & (OBITOGRAV == "1" | OBITOPUERP == "1")) |
         (CAUSABAS == "E230" & (OBITOGRAV == "1" | OBITOPUERP == "1")) |
         ((CAUSABAS >= "F530"  &  CAUSABAS <= "F539") & (OBITOPUERP != "2")) |
         (CAUSABAS == "M830" & OBITOPUERP != "2")))
  ) |>
  select(
    regiao, uf, municipio, codigo = CODMUNRES, ano, capitulo_cid10, causabas_categoria,
    periodo_do_obito, racacor, idade, investigacao_cmm
  ) |>
  mutate(obitos = 1) |>
  group_by(across(!obitos)) |>
  summarise(obitos = sum(obitos)) |>
  ungroup() |>
  as.data.frame()

df_descons_preliminares[is.na(df_descons_preliminares), ]

##Juntando as duas bases
df_obitos_desconsiderados <- full_join(dados_desconsiderados_1996_2022, df_descons_preliminares)

##Exportando os dados 
write.table(df_obitos_desconsiderados, 'dados_oobr_obitos_grav_puerp_desconsiderados_2023.csv', sep = ",", dec = ".", row.names = FALSE)


# Para a seção de óbitos maternos por UF ----------------------------------
df_obitos_desc_uf <- df_obitos_desconsiderados |>
  clean_names() |>
  mutate(
    var_auxiliar = if_else(condition = capitulo_cid10 %like% "^XX", true = 1, false = 0),
    periodo_do_obito = if_else(condition = var_auxiliar == 0, 
                               true = paste("obitos_descons", tolower(periodo_do_obito), " ", "_exceto_ext"),
                               false = paste("obitos_descons", tolower(periodo_do_obito), " ", "_ext")
    )
  ) |>
  group_by(uf, regiao, ano, periodo_do_obito, idade) |>
  summarise(obitos = sum(as.numeric(obitos))) |>
  ungroup() |>
  spread(periodo_do_obito, obitos, fill = 0) |>
  select(1:5, 7, 9, 11, 6, 8, 10, 12) |>
  clean_names() 

df_obitos_maternos_uf <- df_obitos_maternos |>
  group_by(uf, regiao, ano, idade) |>
  summarise(obitos_maternos = sum(obitos)) |>
  ungroup()

df_obitos_uf <- full_join(df_obitos_desc_uf, df_obitos_maternos_uf) |>
  select(1:4, 13, 5:12)

df_obitos_uf$idade[is.na(df_obitos_uf$idade)] <- 0
df_obitos_uf[is.na(df_obitos_uf)] <- 0

df_obitos_uf$idade <- as.numeric(df_obitos_uf$idade)

##Exportando os dados
write.table(df_obitos_uf, 'dados_oobr_obitos_grav_puerp_ufs_2023.csv', sep = ",", dec = ".", row.names = FALSE)






