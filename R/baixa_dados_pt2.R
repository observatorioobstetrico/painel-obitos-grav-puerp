library(microdatasus)
library(tidyverse)
library(httr)
library(janitor)
library(getPass)
library(repr)
library(data.table)
library(readr)

token = getPass()  #Token de acesso à API da PCDaS (todos os arquivos gerados se encontram na pasta "Databases", no Google Drive)

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


# Criando data.frames auxiliares ------------------------------------------
## Obtendo um dataframe com o nome dos capítulos e categorias da CID10 --------
df_cid10 <- data.frame()
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
df_cid10 <- convertRequestToDF(request)
names(df_cid10) <- c("CAUSABAS", "capitulo_cid10", "causabas_categoria", "obitos")

df_cid10 <- df_cid10 |>
  select(!obitos) |>
  arrange(CAUSABAS)


## Obtendo um dataframe com as regiões, UFs e nomes de cada município -------
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
names(df_aux_municipios) <- c("CODMUNRES", "res_MUNNOME", "res_SIGLA_UF", "res_REGIAO", "nascidos")

df_aux_municipios <- df_aux_municipios |>
  select(!nascidos) |>
  arrange(CODMUNRES)


# Baixando os dados preliminares do SIM de 2023 ---------------------------
dados_preliminares_2022_aux1 <- fetch_datasus(
  year_start = 2022, 
  year_end = 2023,
  information_system = "SIM-DO"
) 

## Por enquanto, o microdatasus só tem dados até 2022
unique(substr(dados_preliminares_2022_aux1$DTOBITO, nchar(dados_preliminares_2022_aux1$DTOBITO) - 3, nchar(dados_preliminares_2022_aux1$DTOBITO)))

## Lendo os dados preliminares de 2023, baixados do opendatasus
dados_preliminares_2023_aux1 <- read.csv2("R/databases/DO23OPEN.csv")

## Checando se as colunas são as mesmas
names(dados_preliminares_2023_aux1)[which(!(names(dados_preliminares_2023_aux1) %in% names(dados_preliminares_2022_aux1)))]
names(dados_preliminares_2022_aux1)[which(!(names(dados_preliminares_2022_aux1) %in% names(dados_preliminares_2023_aux1)))]

## Retirando as variáveis que não batem
dados_preliminares_2022_aux2 <- dados_preliminares_2022_aux1 |>
  select(!c(ESTABDESCR, NUDIASOBIN, NUDIASINF, FONTESINF, CONTADOR)) 

dados_preliminares_2023_aux2 <- dados_preliminares_2023_aux1 |>
  select(!c(contador, OPOR_DO, TP_ALTERA, CB_ALT)) |>
  mutate_if(is.numeric, as.character)

## Juntando as duas bases
dados_preliminares_2023_aux <- full_join(dados_preliminares_2022_aux2, dados_preliminares_2023_aux2)

## Transformando algumas variáveis
dados_preliminares_2023 <- left_join(dados_preliminares_2023_aux, df_aux_municipios) |>
  # mutate(
  #   SEXO = if_else(SEXO == "Feminino", 2, 1, missing = 0),
  #   OBITOGRAV = case_when(
  #     OBITOGRAV == "Sim" ~ 1,
  #     OBITOGRAV == "Não" ~ 2,
  #     OBITOGRAV == "Ignorado" | is.na(OBITOGRAV) ~ 9
  #   ),
  #   OBITOPUERP = case_when(
  #     OBITOPUERP == "De 0 a 42 dias" ~ 1,
  #     OBITOPUERP == "De 43 dias a 1 ano" ~ 2,
  #     OBITOPUERP == "Não" ~ 3,
#     OBITOPUERP == "Ignorado" | is.na(OBITOPUERP) ~ 9
#   ),
#   RACACOR = ifelse(is.na(RACACOR), "Ignorado", RACACOR),
#   ESTCIV = ifelse(is.na(ESTCIV), "Ignorado", ESTCIV),
#   ESC2010 = ifelse(is.na(ESC2010), 9, ESC2010),
#   LOCOCOR = ifelse(is.na(LOCOCOR), "Ignorado", LOCOCOR),
#   ASSISTMED = ifelse(is.na(ASSISTMED), "Ignorado", ASSISTMED),
#   NECROPSIA = ifelse(is.na(NECROPSIA), "Ignorado", NECROPSIA),
#   FONTEINV = ifelse(is.na(FONTEINV), "Ignorado", FONTEINV)
# ) |>
  mutate(
    CAUSABAS = ifelse(CAUSABAS == "O935", "O95", CAUSABAS),
    OBITOGRAV = ifelse(is.na(OBITOGRAV), "9", OBITOGRAV),
    OBITOPUERP = ifelse(is.na(OBITOPUERP), "9", OBITOPUERP)
  ) |>
  mutate(
    ano = as.numeric(substr(DTOBITO, nchar(DTOBITO) - 3, nchar(DTOBITO))),
    idade = as.numeric(
      ifelse(
        as.numeric(IDADE) >= 400 & as.numeric(IDADE) <= 499, 
        substr(IDADE, 2, 3), 
        ifelse(IDADE == "999" | is.na(IDADE), 99, 0)
      )
    ),
    RACACOR = case_when(
      RACACOR == "1" ~ "Branca",
      RACACOR == "2" ~ "Preta",
      RACACOR == "3" ~ "Amarela",
      RACACOR == "4" ~ "Parda",
      RACACOR == "5" ~ "Indígena",
      is.na(RACACOR) ~ "Ignorado"
    ),
    .keep = "unused",
    .after = CODMUNRES
  ) 


# Para a seção de óbitos maternos oficiais --------------------------------
## Lendo o arquivo com os óbitos maternos de 1996 a 2021
dados_obitos_maternos_1996_2021 <- read.csv("R/databases/Obitos_maternos_muni2021.csv") 

df_maternos_preliminares <- dados_preliminares_2023 |>
  filter(
    SEXO == "2",
    ((CAUSABAS >= "O000"  &  CAUSABAS <= "O959") |
       (CAUSABAS >= "O980"  &  CAUSABAS <= "O999") |
       (CAUSABAS == "A34" & OBITOPUERP != "2") |
       ((CAUSABAS >= "B200"  &  CAUSABAS <= "B249") & (OBITOGRAV == "1" | OBITOPUERP == "1")) |
       (CAUSABAS == "D392" & (OBITOGRAV == "1" | OBITOPUERP == "1")) |
       (CAUSABAS == "E230" & (OBITOGRAV == "1" | OBITOPUERP == "1")) |
       ((CAUSABAS >= "F530"  &  CAUSABAS <= "F539") & (OBITOPUERP != "2") |
       (CAUSABAS == "M830" & OBITOPUERP != "2")))
  ) |>
  select(res_MUNNOME, CODMUNRES, ano, CAUSABAS, OBITOGRAV, OBITOPUERP, RACACOR, idade, FONTEINV) |>
  mutate(
    tipo_de_morte_materna = if_else(
      condition = (CAUSABAS >= "B200" & CAUSABAS <= "B249") |
        (CAUSABAS >= "O100" & CAUSABAS <= "O109") |
        ((CAUSABAS >= "O240" & CAUSABAS != "O244") & CAUSABAS <= "O259") |
        (CAUSABAS == "O94") |
        (CAUSABAS >= "O980" & CAUSABAS <= "O999"),
      true = "Indireta",
      false = if_else(CAUSABAS == "O95", true = "Não especificada", false = "Direta")
    ),
    periodo_do_obito = case_when(
      OBITOGRAV == "1" & OBITOPUERP != "1" & OBITOPUERP != "2" ~ "Durante a gravidez, parto ou aborto",
      OBITOGRAV != "1" & OBITOPUERP == "1" ~ "Durante o puerpério, até 42 dias",
      OBITOGRAV != "1" & OBITOPUERP == "2" ~ "Durante o puerpério, de 43 dias a menos de 1 ano",
      (OBITOGRAV == "2" & OBITOPUERP == "3") | (OBITOGRAV == "2" & OBITOPUERP == "9") | (OBITOGRAV == "9" & OBITOPUERP == "3")  ~ "Não na gravidez ou no puerpério",
      #OBITOGRAV == "2" & OBITOPUERP == "9" ~ "Durante o puerpério, até 1 ano, período Não discriminado",
      (OBITOGRAV == "9") & (OBITOPUERP == "9") ~ "Não informado ou ignorado",
      (OBITOGRAV == "1" & OBITOPUERP == "1") | (OBITOGRAV == "1" & OBITOPUERP == "2") ~ "Período inconsistente"),
    .after = CAUSABAS,
    investigacao_cmm = if_else(FONTEINV == "1", true = "Sim", false = if_else(FONTEINV == "9", true = "Sem informação", false = "Não",  missing = "Sem informação"), missing = "Sem informação")
  ) |>
  mutate(obitos = 1) |>
  group_by(across(!obitos)) |>
  summarise(obitos = sum(obitos)) |>
  ungroup() |>
  left_join(df_cid10) |>
  left_join(df_aux_municipios) |>
  select(
    regiao = res_REGIAO, uf = res_SIGLA_UF, municipio = res_MUNNOME, codigo = CODMUNRES,
    ano, capitulo_cid10, causabas_categoria, tipo_de_morte_materna, periodo_do_obito, investigacao_cmm, 
    RACACOR, idade, obitos
  ) |>
  arrange(regiao, uf) |>
  clean_names()

df_maternos_preliminares$codigo <- as.numeric(df_maternos_preliminares$codigo)
df_maternos_preliminares$ano <- as.numeric(df_maternos_preliminares$ano)
df_maternos_preliminares$idade <- as.numeric(df_maternos_preliminares$idade)


##Juntando as duas bases
df_obitos_maternos <- full_join(dados_obitos_maternos_1996_2021, df_maternos_preliminares)

##Exportando os dados 
write.table(df_obitos_maternos, 'dados_oobr_obitos_grav_puerp_maternos_oficiais_2023.csv', sep = ",", dec = ".", row.names = FALSE)


# Para a seção de análise cruzada -----------------------------------------
##Lendo o arquivo com os óbitos maternos para a análise cruzada de 1996 a 2021
dados_ac_1996_2021 <- read.csv("R/databases/Obitos_maternos_estendidos2021.csv") 

##Nos dados preliminares, filtrando apenas pelos óbitos maternos
df_ac_preliminares_aux <- dados_preliminares_2023 |>
  filter(
    SEXO == "2",
    ((CAUSABAS >= "O000"  &  CAUSABAS <= "O959") |
       (CAUSABAS >= "O980"  &  CAUSABAS <= "O999") |
       (CAUSABAS == "A34" & OBITOPUERP != "2") |
       ((CAUSABAS >= "B200"  &  CAUSABAS <= "B249") & (OBITOGRAV == "1" | OBITOPUERP == "1")) |
       (CAUSABAS == "D392" & (OBITOGRAV == "1" | OBITOPUERP == "1")) |
       (CAUSABAS == "E230" & (OBITOGRAV == "1" | OBITOPUERP == "1")) |
       ((CAUSABAS >= "F530"  &  CAUSABAS <= "F539") & (OBITOPUERP != "2" | OBITOPUERP == " ")) |
       (CAUSABAS == "M830" & OBITOPUERP != "2"))
  ) |>
  left_join(df_cid10) |>
  left_join(df_aux_municipios) |>
  select(res_REGIAO, res_SIGLA_UF, res_MUNNOME, CODMUNRES, ano, RACACOR, ESTCIV, ESC2010, LOCOCOR, idade, PESO, ASSISTMED, NECROPSIA, FONTEINV, CAUSABAS, capitulo_cid10, causabas_categoria, OBITOGRAV, OBITOPUERP)

colnames(df_ac_preliminares_aux) <- c("regiao", "uf", "municipio", "codigo", "ano_obito", "raca_cor", "est_civil", "escolaridade", "local_ocorrencia_obito", "idade_obito", "peso", "assistencia_med", "necropsia", "fonteinv", "causabas", "causabas_capitulo", "causabas_categoria", "obitograv", "obitopuerp")

##Realizando os tratamentos necessários nos dados preliminares
df_ac_preliminares <- df_ac_preliminares_aux |>
  mutate(
    escolaridade = case_when(
      escolaridade == "0" ~ "Sem escolaridade",
      escolaridade == "1" ~ "Fundamental I",
      escolaridade == "2" ~ "Fundamental II",
      escolaridade == "3" ~ "Médio",
      escolaridade == "4" ~ "Superior incompleto",
      escolaridade == "5" ~ "Superior completo",
      escolaridade == "9" | is.na(escolaridade) ~ "Ignorado"
    ),
    obito_em_idade_fertil = if_else(
      condition = as.numeric(idade_obito) >= 10 & as.numeric(idade_obito) <= 49,
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
      #obitograv == "2" & obitopuerp == "9" ~ "Durante o puerpério, até 1 ano, período Não discriminado",
      (obitograv == "9") & (obitopuerp == "9") ~ "Não informado ou ignorado",
      (obitograv == "1" & obitopuerp == "1") | (obitograv == "1" & obitopuerp == "2") ~ "Período inconsistente"),
    .after = causabas_categoria,
    investigacao_cmm = if_else(fonteinv == "1", true = "Sim", false = if_else(fonteinv == "9", true = "Sem informação", false = "Não",  missing = "Sem informação"), missing = "Sem informação")
  ) |> 
  select(!c(codigo, causabas, obitograv, obitopuerp, fonteinv)) 

df_ac_preliminares$idade_obito <- as.numeric(df_ac_preliminares$idade_obito)
df_ac_preliminares$idade_obito[is.na(df_ac_preliminares$idade_obito)] <- 99
df_ac_preliminares[is.na(df_ac_preliminares)] <- "Ignorado"
df_ac_preliminares$ano_obito <- as.numeric(df_ac_preliminares$ano_obito)

##Juntando as duas bases
df_obitos_maternos_ac <- full_join(
  dados_ac_1996_2021,
  df_ac_preliminares
)

##Exportando os dados 
write.table(df_obitos_maternos_ac, 'dados_oobr_obitos_grav_puerp_analise_cruzada_2023.csv', sep = ",", dec = ".", row.names = FALSE)


# Para a seção de óbitos maternos desconsiderados -------------------------
##Lendo o arquivo com os óbitos de gestantes e puérperas desconsiderados de 1996 a 2021
dados_desconsiderados_1996_2021 <- read.csv("R/databases/Obitos_desconsiderados_muni2021.csv") 

##Nos dados preliminares, filtrando apenas pelos óbitos de gestantes e puérperas desconsiderados
df_descons_preliminares_aux <- dados_preliminares_2023 |>
  filter(
    SEXO == "2",
    OBITOGRAV == "1" | OBITOPUERP == "1" | OBITOPUERP == "2",
    !((CAUSABAS >= "O000"  &  CAUSABAS <= "O959") |
        (CAUSABAS >= "O980"  &  CAUSABAS <= "O999") |
        (CAUSABAS == "A34" & OBITOPUERP != "2") |
        ((CAUSABAS >= "B200"  &  CAUSABAS <= "B249") & (OBITOGRAV == "1" | OBITOPUERP == "1")) |
        (CAUSABAS == "D392" & (OBITOGRAV == "1" | OBITOPUERP == "1")) |
        (CAUSABAS == "E230" & (OBITOGRAV == "1" | OBITOPUERP == "1")) |
        ((CAUSABAS >= "F530"  &  CAUSABAS <= "F539") & (OBITOPUERP != "2")) |
        (CAUSABAS == "M830" & OBITOPUERP != "2"))
  ) |>
  left_join(df_cid10) |>
  left_join(df_aux_municipios) |>
  select(
    regiao = res_REGIAO, uf = res_SIGLA_UF, municipio = res_MUNNOME, CODMUNRES, ano, CAUSABAS,
    capitulo_cid10, causabas_categoria, OBITOGRAV, OBITOPUERP, RACACOR, idade, FONTEINV
  ) |>
  mutate(obitos = 1) |>
  group_by(across(!obitos)) |>
  summarise(obitos = sum(obitos)) |>
  ungroup() 

colnames(df_descons_preliminares_aux) <- c("regiao", "uf", "municipio", "codigo", "ano", "causabas", "capitulo_cid10", "causabas_categoria", "obitograv", "obitopuerp", "racacor", "idade", "fonteinv", "obitos")

##Realizando os tratamentos necessários nos dados preliminares
df_descons_preliminares_aux <- df_descons_preliminares_aux |>
  mutate(
    periodo_do_obito = case_when(
      obitograv == "1" & obitopuerp != "1" & obitopuerp != "2" ~ "Durante a gravidez, parto ou aborto",
      obitograv != "1" & obitopuerp == "1" ~ "Durante o puerpério, até 42 dias",
      obitograv != "1" & obitopuerp == "2" ~ "Durante o puerpério, de 43 dias a menos de 1 ano",
      (obitograv == "1" & obitopuerp == "1") | (obitograv == "1" & obitopuerp == "2") ~ "Período inconsistente"
    ),
    .after = causabas_categoria,
    investigacao_cmm = if_else(fonteinv == "1", true = "Sim", false = if_else(fonteinv == "9", true = "Sem informação", false = "Não",  missing = "Sem informação"), missing = "Sem informação")
  ) |> 
  select(!c(codigo, causabas, obitograv, obitopuerp, fonteinv)) |>
  group_by(across(!obitos)) |>
  summarise(obitos = sum(as.numeric(obitos))) |>
  ungroup()

df_descons_preliminares_aux$idade <- as.numeric(df_descons_preliminares_aux$idade)
df_descons_preliminares_aux$ano <- as.numeric(df_descons_preliminares_aux$ano)

##Juntando as duas bases
df_obitos_desconsiderados <- full_join(
  dados_desconsiderados_1996_2021,
  df_descons_preliminares_aux
)

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






