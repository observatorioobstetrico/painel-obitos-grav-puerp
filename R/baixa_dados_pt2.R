library(microdatasus)
library(tidyverse)
library(httr)
library(janitor)
library(getPass)
library(repr)
library(data.table)
library(readr)

# Lendo dataframes auxiliares (criados em cria_dfs_auxiliares.R) ----------
df_cid10 <- read.csv("R/databases/df_cid10.csv")

df_aux_municipios <- read.csv("R/databases/df_aux_municipios.csv") |>
  mutate_if(is.numeric, as.character) |>
  clean_names()


# Baixando os dados preliminares do SIM de 2023 ---------------------------
download.file("https://s3.sa-east-1.amazonaws.com/ckan.saude.gov.br/SIM/Mortalidade_Geral_2023.csv", "R/databases/DO23OPEN.csv", mode = "wb")

dados_preliminares_2023_aux <- read.csv2("R/databases/DO23OPEN.csv") |> 
  clean_names()

## Fazendo as manipulações necessárias ------------------------------------
dados_preliminares_2023 <- dados_preliminares_2023_aux |>
  mutate_if(is.numeric, as.character) |>
  mutate(
    causabas = ifelse(causabas %in% c("O935", "O937"), "O95", causabas),
    obitograv = ifelse(is.na(obitograv), "9", obitograv),
    obitopuerp = ifelse(is.na(obitopuerp), "9", obitopuerp),
    ano = as.numeric(substr(dtobito, nchar(dtobito) - 3, nchar(dtobito))),
    idade = as.numeric(
      ifelse(
        idade == "999" | is.na(idade),
        99,
        ifelse(
          as.numeric(idade) >= 400 & as.numeric(idade) <= 499,
          substr(idade, 2, 3), 
          0
        )
      )
    ),
    racacor = case_when(
      racacor == "1" ~ "Branca",
      racacor == "2" ~ "Preta",
      racacor == "3" ~ "Amarela",
      racacor == "4" ~ "Parda",
      racacor == "5" ~ "Indígena",
      is.na(racacor) | racacor == "9" ~ "Ignorado"
    ),
    escolaridade = case_when(
      esc2010 == "0" ~ "Sem escolaridade",
      esc2010 == "1" ~ "Fundamental I",
      esc2010 == "2" ~ "Fundamental II",
      esc2010 == "3" ~ "Médio",
      esc2010 == "4" ~ "Superior incompleto",
      esc2010 == "5" ~ "Superior completo",
      is.na(esc2010) | esc2010 == "9" ~ "Ignorado"
    ),
    est_civil = case_when(
      estciv == "1" ~ "Solteiro",
      estciv == "2" ~ "Casado",
      estciv == "3" ~ "Viúvo",
      estciv == "4" ~ "Separado Judic./Divorciado",
      estciv == "5" ~ "União Estável",
      is.na(estciv) | estciv == "9" ~ "Ignorado"
    ),
    local_ocorrencia_obito = case_when(
      lococor == "1" ~ "Hospital",
      lococor == "2" ~ "Outro Estab. Saúde",
      lococor == "3" ~ "Domicílio",
      lococor == "4" ~ "Via Pública",
      lococor == "5" ~ "Outros",
      is.na(lococor) | lococor == "9" | lococor == "6" ~ "Ignorado"
    ),
    assistencia_med = case_when(
      assistmed == "1" ~ "Com assistência",
      assistmed == "2" ~ "Sem assistência",
      is.na(assistmed) | assistmed == "9" ~ "Ignorado"
    ),
    necropsia = case_when(
      necropsia == "1" ~ "Sim",
      necropsia == "2" ~ "Não",
      is.na(necropsia) | necropsia == "9" ~ "Ignorado"
    ),
    obito_em_idade_fertil = if_else(
      condition = as.numeric(idade) >= 10 & as.numeric(idade) <= 49,
      true = "Sim",
      false = "Não"
    ),
    tipo_de_morte_materna = if_else(
      condition = (causabas >= "B200" & causabas <= "B249") |
        (causabas >= "O100" & causabas <= "O109") |
        ((causabas >= "O240" & causabas != "O244") & causabas <= "O259") |
        (causabas == "O94") |
        (causabas >= "O980" & causabas <= "O999"),
      true = "Indireta",
      false = if_else(causabas == "O95", true = "Não especificada", false = "Direta")
    ),
    periodo_do_obito = case_when(
      obitograv == "1" & obitopuerp != "1" & obitopuerp != "2" ~ "Durante a gravidez, parto ou aborto",
      obitograv != "1" & obitopuerp == "1" ~ "Durante o puerpério, até 42 dias",
      obitograv != "1" & obitopuerp == "2" ~ "Durante o puerpério, de 43 dias a menos de 1 ano",
      (obitograv == "2" & obitopuerp == "3") | (obitograv == "2" & obitopuerp == "9") | (obitograv == "9" & obitopuerp == "3")  ~ "Não na gravidez ou no puerpério",
      obitograv == "9" & obitopuerp == "9" ~ "Não informado ou ignorado",
      (obitograv == "1" & obitopuerp == "1") | (obitograv == "1" & obitopuerp == "2") ~ "Período inconsistente"
    ),
    investigacao_cmm = if_else(
      fonteinv == "1",
      true = "Sim", 
      false = if_else(fonteinv == "9", true = "Sem informação", false = "Não",  missing = "Sem informação"),
      missing = "Sem informação"
    )
  ) |>
  left_join(df_cid10) |>
  left_join(df_aux_municipios) 


# Para a seção de óbitos maternos oficiais --------------------------------
## Lendo o arquivo com os óbitos maternos de 1996 a 2022 ------------------
dados_obitos_maternos_1996_2022 <- read.csv("R/databases/obitos_maternos_muni_1996_2022.csv") |>
  mutate(codigo = as.character(codigo)) 

## Filtrando, nos dados preliminares, apenas os óbitos maternos -----------
df_maternos_preliminares <- dados_preliminares_2023 |>
  filter(
    sexo == "2",
    ((causabas >= "O000"  &  causabas <= "O959") |
       (causabas >= "O980"  &  causabas <= "O999") |
       (causabas == "A34" & obitopuerp != "2") |
       ((causabas >= "B200"  &  causabas <= "B249") & (obitograv == "1" | obitopuerp == "1")) |
       (causabas == "D392" & (obitograv == "1" | obitopuerp == "1")) |
       (causabas == "E230" & (obitograv == "1" | obitopuerp == "1")) |
       ((causabas >= "F530"  &  causabas <= "F539") & (obitopuerp != "2")) |
       (causabas == "M830" & obitopuerp != "2"))
  ) |>
  mutate(
    obitos = 1,
    .keep = "unused",
  ) |>
  select(
    codigo = res_codigo_adotado, municipio, uf, regiao, ano, causabas, capitulo_cid10, causabas_categoria,
    tipo_de_morte_materna, periodo_do_obito, investigacao_cmm, racacor, idade, obitos
  ) |>
  group_by(across(!obitos)) |>
  summarise(obitos = sum(obitos)) |>
  ungroup() |>
  arrange(codigo) |>
  as.data.frame()

get_dupes(df_maternos_preliminares)
sum(df_maternos_preliminares$obitos[which(df_maternos_preliminares$ano == 2023)])
df_maternos_preliminares[is.na(df_maternos_preliminares), ]

## Juntando as duas bases -------------------------------------------------
df_obitos_maternos <- full_join(dados_obitos_maternos_1996_2022, df_maternos_preliminares)

## Exportando os dados -----------------------------------------------------
write.table(df_obitos_maternos, 'dados_oobr_obitos_grav_puerp_maternos_oficiais_2023.csv', sep = ",", dec = ".", row.names = FALSE)


# Para a seção de garbage codes -------------------------------------------
## Lendo o dataframe que recebe as CIDs consideradas garbage codes --------
df_garbage_codes <- read.csv("R/databases/df_garbage_codes.csv")

# Lendo o arquivo com os garbage codes de 1996 a 2022 ---------------------
dados_garbage_codes_1996_2022 <- read.csv("R/databases/obitos_garbage_code_muni_1996_2022.csv") |>
  mutate(codigo = as.character(codigo)) 

## Filtrando os óbitos maternos preenchidos com garbage codes -------------
df_maternos_garbage_codes_preliminares <- df_maternos_preliminares |>
  filter(causabas %in% df_garbage_codes$causabas)

## Juntando as duas bases -------------------------------------------------
df_maternos_garbage_codes <- full_join(dados_garbage_codes_1996_2022, df_maternos_garbage_codes_preliminares)

## Exportando os dados -----------------------------------------------------
write.table(df_maternos_garbage_codes, 'dados_oobr_obitos_grav_puerp_garbage_codes_2023.csv', sep = ",", dec = ".", row.names = FALSE)


# Para a seção de análise cruzada -----------------------------------------
## Lendo o arquivo com os óbitos maternos p/ essa seção de 96 a 2022 ------
dados_ac_1996_2022 <- read.csv("R/databases/obitos_maternos_estendidos_1996_2022.csv") |>
  mutate(codigo = as.character(codigo))

## Filtrando, nos dados preliminares, apenas os óbitos maternos -----------
df_ac_preliminares <- dados_preliminares_2023 |>
  filter(
    sexo == "2",
    ((causabas >= "O000"  &  causabas <= "O959") |
       (causabas >= "O980"  &  causabas <= "O999") |
       (causabas == "A34" & obitopuerp != "2") |
       ((causabas >= "B200"  &  causabas <= "B249") & (obitograv == "1" | obitopuerp == "1")) |
       (causabas == "D392" & (obitograv == "1" | obitopuerp == "1")) |
       (causabas == "E230" & (obitograv == "1" | obitopuerp == "1")) |
       ((causabas >= "F530"  &  causabas <= "F539") & (obitopuerp != "2")) |
       (causabas == "M830" & obitopuerp != "2"))
  ) |>
  select(
    codigo = res_codigo_adotado, municipio, uf, regiao, ano, racacor, est_civil, escolaridade, idade, 
    local_ocorrencia_obito, assistencia_med, necropsia, tipo_de_morte_materna, periodo_do_obito,
    obito_em_idade_fertil, investigacao_cmm, capitulo_cid10
  ) 

nrow(df_ac_preliminares[which(df_ac_preliminares$ano == 2023), ])
df_ac_preliminares[is.na(df_ac_preliminares), ]

##Juntando as duas bases
df_obitos_maternos_ac <- full_join(dados_ac_1996_2022, df_ac_preliminares)

##Exportando os dados 
write.table(df_obitos_maternos_ac, 'dados_oobr_obitos_grav_puerp_analise_cruzada_2023.csv', sep = ",", dec = ".", row.names = FALSE)


# Para a seção de óbitos maternos desconsiderados -------------------------
## Lendo o arquivo com os óbitos desconsiderados de 1996 a 2022 -----------
dados_desconsiderados_1996_2022 <- read.csv("R/databases/obitos_desconsiderados_muni_1996_2022.csv") |>
  mutate(codigo = as.character(codigo))

## Filtrando, nos dados preliminares, apenas pelos óbitos descons. --------
df_descons_preliminares <- dados_preliminares_2023 |>
  filter(
    sexo == "2",
    obitograv == "1" | obitopuerp == "1" | obitopuerp == "2",
    !(((causabas >= "O000"  &  causabas <= "O959") |
         (causabas >= "O980"  &  causabas <= "O999") |
         (causabas == "A34" & obitopuerp != "2") |
         ((causabas >= "B200"  &  causabas <= "B249") & (obitograv == "1" | obitopuerp == "1")) |
         (causabas == "D392" & (obitograv == "1" | obitopuerp == "1")) |
         (causabas == "E230" & (obitograv == "1" | obitopuerp == "1")) |
         ((causabas >= "F530"  &  causabas <= "F539") & (obitopuerp != "2")) |
         (causabas == "M830" & obitopuerp != "2")))
  ) |>
  select(
    codigo = res_codigo_adotado, ano, municipio, uf, regiao, capitulo_cid10, causabas_categoria,
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


## Para a seção de óbitos maternos por UF ----------------------------------
df_obitos_desc_uf <- df_obitos_desconsiderados |>
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

##Exportando os dados
write.table(df_obitos_uf, 'dados_oobr_obitos_grav_puerp_ufs_2023.csv', sep = ",", dec = ".", row.names = FALSE)






