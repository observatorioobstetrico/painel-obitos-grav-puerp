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
  mutate_if(is.numeric, as.character)


# Baixando os dados do SIM de 1996 a 2022 ---------------------------------
df_sim_aux <- fetch_datasus(
  year_start = 1996,
  year_end = 2022,
  information_system = "SIM-DO"
) 

write.csv(df_sim_aux, "dados_sim_1996_2022.csv", row.names = FALSE)
df_sim_aux <- data.table::fread("dados_sim_1996_2022.csv")

df_sim <- df_sim_aux |>
  mutate_if(is.numeric, as.character) |>
  mutate(
    CAUSABAS = str_sub(CAUSABAS, 1, 3),
    CAUSABAS = ifelse(CAUSABAS == "O93", "O95", CAUSABAS),
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
  left_join(df_cid10) |>
  left_join(df_aux_municipios)
  

## Criando a base com os óbitos maternos oficiais -------------------------
df_obitos_maternos <- df_sim |>
  filter(
    SEXO == "2",
    (CAUSABAS >= "O00"  &  CAUSABAS <= "O95") |
      (CAUSABAS >= "O98"  &  CAUSABAS <= "O99") |
      (CAUSABAS == "A34" & OBITOPUERP != "2") |
      ((CAUSABAS >= "B20"  &  CAUSABAS <= "B24") & (OBITOGRAV == "1" | OBITOPUERP == "1")) |
      (CAUSABAS == "D39" & (OBITOGRAV == "1" | OBITOPUERP == "1")) |
      (CAUSABAS == "E23" & (OBITOGRAV == "1" | OBITOPUERP == "1")) |
      (CAUSABAS == "F53"  & OBITOPUERP != "2") |
      (CAUSABAS == "M83" & OBITOPUERP != "2")
  ) |>
  mutate(
    obitos = 1,
    .keep = "unused",
  ) |>
  select(
    codigo = CODMUNRES, municipio, uf, regiao, ano, capitulo_cid10, causabas_categoria,
    tipo_de_morte_materna, periodo_do_obito, investigacao_cmm, racacor, idade, obitos
  ) |>
  group_by(across(!obitos)) |>
  summarise(obitos = sum(obitos)) |>
  ungroup() |>
  arrange(codigo)

as.data.frame(df_obitos_maternos)[is.na(as.data.frame(df_obitos_maternos)), ]

### Exportando os dados 
write.table(df_obitos_maternos, 'R/databases/obitos_maternos_muni_1996_2022.csv', sep = ",", dec = ".", row.names = FALSE)


## Criando a base para o menu de análise cruzada --------------------------
df_obitos_maternos_ac <- df_sim |>
  filter(
    SEXO == "2",
    (CAUSABAS >= "O00"  &  CAUSABAS <= "O95") |
      (CAUSABAS >= "O98"  &  CAUSABAS <= "O99") |
      (CAUSABAS == "A34" & OBITOPUERP != "2") |
      ((CAUSABAS >= "B20"  &  CAUSABAS <= "B24") & (OBITOGRAV == "1" | OBITOPUERP == "1")) |
      (CAUSABAS == "D39" & (OBITOGRAV == "1" | OBITOPUERP == "1")) |
      (CAUSABAS == "E23" & (OBITOGRAV == "1" | OBITOPUERP == "1")) |
      (CAUSABAS == "F53"  & OBITOPUERP != "2") |
      (CAUSABAS == "M83" & OBITOPUERP != "2")
  ) |>
  select(
    codigo = CODMUNRES, municipio, uf, regiao, ano, racacor, est_civil, escolaridade, idade, 
    local_ocorrencia_obito, assistencia_med, necropsia, investigacao_cmm, capitulo_cid10
  ) 

### Exportando os dados 
write.table(df_obitos_maternos_ac_completo, 'R/databases/obitos_maternos_estendidos_1996_2022.csv', sep = ",", dec = ".", row.names = FALSE)


## Criando a base com os óbitos maternos desconsiderados ------------------
df_obitos_desconsiderados <- df_sim |>
  filter(
    SEXO == "2",
    (OBITOGRAV == "1" | OBITOPUERP == "1" | OBITOPUERP == "2"), 
    !((CAUSABAS >= "O00"  &  CAUSABAS <= "O95") |
        (CAUSABAS >= "O98"  &  CAUSABAS <= "O99") |
        (CAUSABAS == "A34" & OBITOPUERP != "2") |
        ((CAUSABAS >= "B20"  &  CAUSABAS <= "B24") & (OBITOGRAV == "1" | OBITOPUERP == "1")) |
        (CAUSABAS == "D39" & (OBITOGRAV == "1" | OBITOPUERP == "1")) |
        (CAUSABAS == "E23" & (OBITOGRAV == "1" | OBITOPUERP == "1")) |
        (CAUSABAS == "F53"  & OBITOPUERP != "2") |
        (CAUSABAS == "M83" & OBITOPUERP != "2"))
  ) |>
  select(
    regiao, uf, municipio, codigo = CODMUNRES, ano, capitulo_cid10, causabas_categoria,
    periodo_do_obito, racacor, idade, investigacao_cmm
  ) |>
  mutate(obitos = 1) |>
  group_by(across(!obitos)) |>
  summarise(obitos = sum(obitos)) |>
  ungroup() 

df_obitos_desconsiderados[is.na(df_obitos_desconsiderados)]

### Exportando os dados 
write.table(df_obitos_desconsiderados, 'R/databases/obitos_desconsiderados_muni_1996_2022.csv', sep = ",", dec = ".", row.names = FALSE)


## Criando a base para a seção de óbitos maternos por UF ------------------
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

View(df_obitos_uf[is.na(df_obitos_uf$idade), ])

df_obitos_uf$idade[is.na(df_obitos_uf$idade)] <- 0
df_obitos_uf[is.na(df_obitos_uf)] <- 0

df_obitos_uf$idade <- as.numeric(df_obitos_uf$idade)


df_desc_antigoa <- read.csv("R/databases/Obitos_desconsiderados_muni2021.csv")

glimpse(df_desc_antigoa)
glimpse(df_obitos_desconsiderados)

df_desc_antigo <- df_desc_antigoa |>
  filter(ano < 2021) |>
  mutate(codigo = as.character(codigo))

df_obitos_desconsiderados_completo <- full_join(df_desc_antigo, df_obitos_desconsiderados) |>
  arrange(codigo, ano)

##Exportando os dados
write.table(df_obitos_uf, 'dados_oobr_obitos_grav_puerp_ufs_2023.csv', sep = ",", dec = ".", row.names = FALSE)

























---------------------------
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

estados <- c('RO','AC','AM','RR','PA','AP','TO','MA','PI','CE','RN','PB','PE','AL','SE','BA','MG','ES','RJ','SP','PR','SC','RS','MS','MT','GO','DF')
endpoint <- paste0(url_base,"/","sql_query")

##Óbitos maternos oficiais dos anos de 1996 a 2022



df_obitos_maternos_aux <- dataframe <- data.frame()

for (estado in estados){
  
  params = paste0('{
      "token": {
        "token": "',token,'"
      },
      "sql": {
        "sql": {"query":" SELECT res_REGIAO, res_SIGLA_UF, res_MUNNOME, res_codigo_adotado, ano_obito, CAUSABAS, causabas_capitulo, causabas_categoria, OBITOGRAV, OBITOPUERP, def_raca_cor, idade_obito_anos, FONTEINV, COUNT(1)',
                        ' FROM \\"datasus-sim\\"',
                        ' WHERE (res_SIGLA_UF = \'',estado,'\' AND SEXO = 2 AND',
                              ' ((CAUSABAS >= \'O000\'  AND  CAUSABAS <= \'O959\') OR',
                              ' (CAUSABAS >= \'O980\'  AND  CAUSABAS <= \'O999\') OR',
                              ' (CAUSABAS = \'A34\' AND OBITOPUERP != 2) OR',
                              ' ((CAUSABAS >= \'B200\'  AND  CAUSABAS <= \'B249\') AND (OBITOGRAV = 1 OR OBITOPUERP = 1)) OR',
                              ' (CAUSABAS = \'D392\' AND (OBITOGRAV = 1 OR OBITOPUERP = 1)) OR',
                              ' (CAUSABAS = \'E230\' AND (OBITOGRAV = 1 OR OBITOPUERP = 1)) OR',
                              ' ((CAUSABAS >= \'F530\'  AND  CAUSABAS <= \'F539\') AND (OBITOPUERP != 2 OR OBITOPUERP = \' \')) OR',
                              ' (CAUSABAS = \'M830\' AND OBITOPUERP != 2)))',
                        ' GROUP BY res_REGIAO, res_SIGLA_UF, res_MUNNOME, res_codigo_adotado, ano_obito, CAUSABAS, causabas_capitulo, causabas_categoria, OBITOGRAV, OBITOPUERP, def_raca_cor, idade_obito_anos, FONTEINV",
                        "fetch_size": 65000}
      }
    }')
  
  request <- POST(url = endpoint, body = params, encode = "form")
  dataframe <- convertRequestToDF(request)
  names(dataframe) <- c("regiao", "uf", "municipio", "codigo", "ano", "causabas", "capitulo_cid10", "causabas_categoria", "obitograv", "obitopuerp", "racacor", "idade", "fonteinv", "obitos")
  df_obitos_maternos_aux <- rbind(df_obitos_maternos_aux, dataframe)
  
  repeat {
    
    cursor <- content(request)$cursor
    
    params = paste0('{
          "token": {
            "token": "',token,'"
          },
          "sql": {
            "sql": {"query":" SELECT res_REGIAO, res_SIGLA_UF, res_MUNNOME, res_codigo_adotado, ano_obito, CAUSABAS, causabas_capitulo, causabas_categoria, OBITOGRAV, OBITOPUERP, def_raca_cor, idade_obito_anos, FONTEINV, COUNT(1)',
                            ' FROM \\"datasus-sim\\"',
                            ' WHERE (res_SIGLA_UF = \'',estado,'\' AND SEXO = 2 AND',
                                  ' ((CAUSABAS >= \'O000\'  AND  CAUSABAS <= \'O959\') OR',
                                  ' (CAUSABAS >= \'O980\'  AND  CAUSABAS <= \'O999\') OR',
                                  ' (CAUSABAS = \'A34\' AND OBITOPUERP != 2) OR',
                                  ' ((CAUSABAS >= \'B200\'  AND  CAUSABAS <= \'B249\') AND (OBITOGRAV = 1 OR OBITOPUERP = 1)) OR',
                                  ' (CAUSABAS = \'D392\' AND (OBITOGRAV = 1 OR OBITOPUERP = 1)) OR',
                                  ' (CAUSABAS = \'E230\' AND (OBITOGRAV = 1 OR OBITOPUERP = 1)) OR',
                                  ' ((CAUSABAS >= \'F530\'  AND  CAUSABAS <= \'F539\') AND (OBITOPUERP != 2 OR OBITOPUERP = \' \')) OR',
                                  ' (CAUSABAS = \'M830\' AND OBITOPUERP != 2)))',
                            ' GROUP BY res_REGIAO, res_SIGLA_UF, res_MUNNOME, res_codigo_adotado, ano_obito, CAUSABAS, causabas_capitulo, causabas_categoria, OBITOGRAV, OBITOPUERP, def_raca_cor, idade_obito_anos, FONTEINV",
                            "fetch_size": 65000, "cursor": "',cursor,'"}
                            }
                    }')
    
    
    request <- POST(url = endpoint, body = params, encode = "form")
    
    if (length(content(request)$rows) == 0)
      break
    else print("oi")
    
    request <- POST(url = endpoint, body = params, encode = "form")
    dataframe <- convertRequestToDF(request)
    names(dataframe) <- c("regiao", "uf", "municipio", "codigo", "ano", "causabas", "capitulo_cid10", "causabas_categoria", "obitograv", "obitopuerp", "racacor", "idade", "fonteinv", "obitos")
    df_obitos_maternos_aux <- rbind(df_obitos_maternos_aux, dataframe)
  }
}

head(df_obitos_maternos_aux)

df_obitos_maternos <- df_obitos_maternos_aux |>
  mutate(
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
      #obitograv == "2" & obitopuerp == "9" ~ "Durante o puerpério, até 1 ano, período não discriminado",
      obitograv == "9" & obitopuerp == "9" ~ "Não informado ou ignorado",
      (obitograv == "1" & obitopuerp == "1") | (obitograv == "1" & obitopuerp == "2") ~ "Período inconsistente"),
    .after = causabas_categoria,
    investigacao_cmm = if_else(fonteinv == "1", true = "Sim", false = if_else(fonteinv == "9", true = "Sem informação", false = "Não",  missing = "Sem informação"), missing = "Sem informação")
  ) |> 
  select(!c(causabas, obitograv, obitopuerp, fonteinv)) |>
  group_by(across(!obitos)) |>
  summarise(obitos = sum(as.numeric(obitos))) |>
  ungroup()

df_obitos_maternos$idade <- as.numeric(df_obitos_maternos$idade)

##Exportando os dados 
write.table(df_obitos_maternos, 'R/databases/Obitos_maternos_muni2021.csv', sep = ",", dec = ".", row.names = FALSE)


##Óbitos maternos oficiais dos anos de 1996 a 2021 para o menu de análise cruzada
df_obitos_maternos_ac_aux <- dataframe <- data.frame()

for (estado in estados){
  
  params = paste0('{
      "token": {
        "token": "',token,'"
      },
      "sql": {
        "sql": {"query":" SELECT res_REGIAO, res_SIGLA_UF, res_MUNNOME, res_codigo_adotado, ano_obito, def_raca_cor, def_est_civil, ESC2010, def_loc_ocor, idade_obito_anos, PESO, def_assist_med, def_necropsia, FONTEINV, CAUSABAS, causabas_capitulo, causabas_categoria, OBITOGRAV, OBITOPUERP, COUNT(1)',
                  ' FROM \\"datasus-sim\\"',
                  ' WHERE (res_SIGLA_UF = \'',estado,'\' AND SEXO = 2 AND',
                  ' ((CAUSABAS >= \'O000\'  AND  CAUSABAS <= \'O959\') OR',
                  ' (CAUSABAS >= \'O980\'  AND  CAUSABAS <= \'O999\') OR',
                  ' (CAUSABAS = \'A34\' AND OBITOPUERP != 2) OR',
                  ' ((CAUSABAS >= \'B200\'  AND  CAUSABAS <= \'B249\') AND (OBITOGRAV = 1 OR OBITOPUERP = 1)) OR',
                  ' (CAUSABAS = \'D392\' AND (OBITOGRAV = 1 OR OBITOPUERP = 1)) OR',
                  ' (CAUSABAS = \'E230\' AND (OBITOGRAV = 1 OR OBITOPUERP = 1)) OR',
                  ' ((CAUSABAS >= \'F530\'  AND  CAUSABAS <= \'F539\') AND (OBITOPUERP != 2 OR OBITOPUERP = \' \')) OR',
                  ' (CAUSABAS = \'M830\' AND OBITOPUERP != 2)))',
                  ' GROUP BY res_REGIAO, res_SIGLA_UF, res_MUNNOME, res_codigo_adotado, ano_obito, def_raca_cor, def_est_civil, ESC2010, def_loc_ocor, idade_obito_anos, PESO, def_assist_med, def_necropsia, FONTEINV, CAUSABAS, causabas_capitulo, causabas_categoria, OBITOGRAV, OBITOPUERP",
                        "fetch_size": 65000}
      }
    }')
  
  request <- POST(url = endpoint, body = params, encode = "form")
  dataframe <- convertRequestToDF(request)
  names(dataframe) <- c("regiao", "uf", "municipio", "codigo", "ano_obito", "raca_cor", "est_civil", "escolaridade", "local_ocorrencia_obito", "idade_obito", "peso", "assistencia_med", "necropsia", "fonteinv", "causabas", "causabas_capitulo", "causabas_categoria", "obitograv", "obitopuerp", "obitos")
  df_obitos_maternos_ac_aux <- rbind(df_obitos_maternos_ac_aux, dataframe)
  
  repeat {
    
    cursor <- content(request)$cursor
    
    params = paste0('{
          "token": {
            "token": "',token,'"
          },
          "sql": {
            "sql": {"query":" SELECT res_REGIAO, res_SIGLA_UF, res_MUNNOME, res_codigo_adotado, ano_obito, def_raca_cor, def_est_civil, ESC2010, def_loc_ocor, idade_obito_anos, PESO, def_assist_med, def_necropsia, FONTEINV, CAUSABAS, causabas_capitulo, causabas_categoria, OBITOGRAV, OBITOPUERP, COUNT(1)',
                    ' FROM \\"datasus-sim\\"',
                    ' WHERE (res_SIGLA_UF = \'',estado,'\' AND SEXO = 2 AND',
                    ' ((CAUSABAS >= \'O000\'  AND  CAUSABAS <= \'O959\') OR',
                    ' (CAUSABAS >= \'O980\'  AND  CAUSABAS <= \'O999\') OR',
                    ' (CAUSABAS = \'A34\' AND OBITOPUERP != 2) OR',
                    ' ((CAUSABAS >= \'B200\'  AND  CAUSABAS <= \'B249\') AND (OBITOGRAV = 1 OR OBITOPUERP = 1)) OR',
                    ' (CAUSABAS = \'D392\' AND (OBITOGRAV = 1 OR OBITOPUERP = 1)) OR',
                    ' (CAUSABAS = \'E230\' AND (OBITOGRAV = 1 OR OBITOPUERP = 1)) OR',
                    ' ((CAUSABAS >= \'F530\'  AND  CAUSABAS <= \'F539\') AND (OBITOPUERP != 2 OR OBITOPUERP = \' \')) OR',
                    ' (CAUSABAS = \'M830\' AND OBITOPUERP != 2)))',
                    ' GROUP BY res_REGIAO, res_SIGLA_UF, res_MUNNOME, res_codigo_adotado, ano_obito, def_raca_cor, def_est_civil, ESC2010, def_loc_ocor, idade_obito_anos, PESO, def_assist_med, def_necropsia, FONTEINV, CAUSABAS, causabas_capitulo, causabas_categoria, OBITOGRAV, OBITOPUERP",
                            "fetch_size": 65000, "cursor": "',cursor,'"}
                            }
                    }')
    
    
    request <- POST(url = endpoint, body = params, encode = "form")
    
    if (length(content(request)$rows) == 0)
      break
    else print("oi")
    
    request <- POST(url = endpoint, body = params, encode = "form")
    dataframe <- convertRequestToDF(request)
    names(dataframe) <- c("regiao", "uf", "municipio", "codigo", "ano_obito", "raca_cor", "est_civil", "escolaridade", "local_ocorrencia_obito", "idade_obito", "assistencia_med", "necropsia", "fonteinv", "causabas", "causabas_capitulo", "causabas_categoria", "obitograv", "obitopuerp", "obitos")
    df_obitos_maternos_ac_aux <- rbind(df_obitos_maternos_ac_aux, dataframe)
  }
}

head(df_obitos_maternos_ac_aux)
unique(df_obitos_maternos_ac_aux$obitos)

df_obitos_maternos_ac_aux2 <- rbind(df_obitos_maternos_ac_aux, df_obitos_maternos_ac_aux[rep(which(df_obitos_maternos_ac_aux$obitos == 2)), ]) |>
  dplyr::select(!c("obitos"))

df_obitos_maternos_ac <- df_obitos_maternos_ac_aux2 |>
  mutate(
    escolaridade = case_when(
      escolaridade == "0" ~ "Sem escolaridade",
      escolaridade == "1" ~ "Fundamental I",
      escolaridade == "2" ~ "Fundamental II",
      escolaridade == "3" ~ "Médio",
      escolaridade == "4" ~ "Superior incompleto",
      escolaridade == "5" ~ "Superior completo",
      escolaridade == "9" ~ "Ignorado"
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
      #obitograv == "2" & obitopuerp == "9" ~ "Durante o puerpério, até 1 ano, período não discriminado",
      obitograv == "9" & obitopuerp == "9" ~ "Não informado ou ignorado",
      (obitograv == "1" & obitopuerp == "1") | (obitograv == "1" & obitopuerp == "2") ~ "Período inconsistente"),
    .after = causabas_categoria,
    investigacao_cmm = if_else(fonteinv == "1", true = "Sim", false = if_else(fonteinv == "9", true = "Sem informação", false = "Não",  missing = "Sem informação"), missing = "Sem informação")
  ) |> 
  select(!c(causabas, obitograv, obitopuerp, fonteinv)) 

df_obitos_maternos_ac$idade_obito <- as.numeric(df_obitos_maternos_ac$idade_obito)
df_obitos_maternos_ac$idade_obito[is.na(df_obitos_maternos_ac$idade_obito)] <- 99
df_obitos_maternos_ac[is.na(df_obitos_maternos_ac)] <- "Ignorado"

##Exportando os dados 
write.table(df_obitos_maternos_ac, 'R/databases/Obitos_maternos_estendidos_1996_2021.csv', sep = ",", dec = ".", row.names = FALSE)


#Óbitos maternos desconsiderados dos anos de 1996 a 2021
df_obitos_desconsiderados_aux <- dataframe <- data.frame()

for (estado in estados){
  
  params = paste0('{
      "token": {
        "token": "',token,'"
      },
      "sql": {
        "sql": {"query":" SELECT res_REGIAO, res_SIGLA_UF, res_MUNNOME, res_codigo_adotado, ano_obito, CAUSABAS, causabas_capitulo, causabas_categoria, OBITOGRAV, OBITOPUERP, def_raca_cor, idade_obito_anos, FONTEINV, COUNT(1)',
                  ' FROM \\"datasus-sim\\"',
                  ' WHERE (res_SIGLA_UF = \'',estado,'\' AND SEXO = 2 AND (OBITOGRAV = 1 OR OBITOPUERP = 1 OR OBITOPUERP = 2) AND',
                        ' NOT (((CAUSABAS >= \'O000\'  AND  CAUSABAS <= \'O959\') OR',
                            ' (CAUSABAS >= \'O980\'  AND  CAUSABAS <= \'O999\') OR',
                            ' (CAUSABAS = \'A34\' AND OBITOPUERP != 2) OR',
                            ' ((CAUSABAS >= \'B200\'  AND  CAUSABAS <= \'B249\') AND (OBITOGRAV = 1 OR OBITOPUERP = 1)) OR',
                            ' (CAUSABAS = \'D392\' AND (OBITOGRAV = 1 OR OBITOPUERP = 1)) OR',
                            ' (CAUSABAS = \'E230\' AND (OBITOGRAV = 1 OR OBITOPUERP = 1)) OR',
                            ' ((CAUSABAS >= \'F530\'  AND  CAUSABAS <= \'F539\') AND (OBITOPUERP != 2 OR OBITOPUERP = \' \')) OR',
                            ' (CAUSABAS = \'M830\' AND OBITOPUERP != 2))))',
                  ' GROUP BY res_REGIAO, res_SIGLA_UF, res_MUNNOME, res_codigo_adotado, ano_obito, CAUSABAS, causabas_capitulo, causabas_categoria, OBITOGRAV, OBITOPUERP, def_raca_cor, idade_obito_anos, FONTEINV",
                  "fetch_size": 65000}
                  }
                  }')
  
  request <- POST(url = endpoint, body = params, encode = "form")
  dataframe <- convertRequestToDF(request)
  names(dataframe) <- c("regiao", "uf", "municipio", "codigo", "ano", "causabas", "capitulo_cid10", "causabas_categoria", "obitograv", "obitopuerp", "racacor", "idade", "fonteinv", "obitos")
  df_obitos_desconsiderados_aux <- rbind(df_obitos_desconsiderados_aux, dataframe)
  
  repeat {
    
    cursor <- content(request)$cursor
    
    params = paste0('{
          "token": {
            "token": "',token,'"
          },
          "sql": {
            "sql": {"query":" SELECT res_REGIAO, res_SIGLA_UF, res_MUNNOME, res_codigo_adotado, ano_obito, CAUSABAS, causabas_capitulo, causabas_categoria, OBITOGRAV, OBITOPUERP, def_raca_cor, idade_obito_anos, FONTEINV, COUNT(1)',
                    ' FROM \\"datasus-sim\\"',
                    ' WHERE (res_SIGLA_UF = \'',estado,'\' AND SEXO = 2 AND (OBITOGRAV = 1 OR OBITOPUERP = 1 OR OBITOPUERP = 2) AND',
                          ' NOT (((CAUSABAS >= \'O000\'  AND  CAUSABAS <= \'O959\') OR',
                              ' (CAUSABAS >= \'O980\'  AND  CAUSABAS <= \'O999\') OR',
                              ' (CAUSABAS = \'A34\' AND OBITOPUERP != 2) OR',
                              ' ((CAUSABAS >= \'B200\'  AND  CAUSABAS <= \'B249\') AND (OBITOGRAV = 1 OR OBITOPUERP = 1)) OR',
                              ' (CAUSABAS = \'D392\' AND (OBITOGRAV = 1 OR OBITOPUERP = 1)) OR',
                              ' (CAUSABAS = \'E230\' AND (OBITOGRAV = 1 OR OBITOPUERP = 1)) OR',
                              ' ((CAUSABAS >= \'F530\'  AND  CAUSABAS <= \'F539\') AND (OBITOPUERP != 2 OR OBITOPUERP = \' \')) OR',
                              ' (CAUSABAS = \'M830\' AND OBITOPUERP != 2))))',
                    ' GROUP BY res_REGIAO, res_SIGLA_UF, res_MUNNOME, res_codigo_adotado, ano_obito, CAUSABAS, causabas_capitulo, causabas_categoria, OBITOGRAV, OBITOPUERP, def_raca_cor, idade_obito_anos, FONTEINV",
                    "fetch_size": 65000, "cursor": "',cursor,'"}
                    }
                    }')
    
    
    request <- POST(url = endpoint, body = params, encode = "form")
    
    if (length(content(request)$rows) == 0)
      break
    else print("oi")
    
    request <- POST(url = endpoint, body = params, encode = "form")
    dataframe <- convertRequestToDF(request)
    names(dataframe) <- c("regiao", "uf", "municipio", "codigo", "ano", "causabas", "capitulo_cid10", "causabas_categoria", "obitograv", "obitopuerp", "racacor", "idade", "fonteinv", "obitos")
    df_obitos_desconsiderados_aux <- rbind(df_obitos_desconsiderados_aux, dataframe)
  }
}

head(df_obitos_desconsiderados_aux)

df_obitos_desconsiderados <- df_obitos_desconsiderados_aux |>
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
  select(!c(causabas, obitograv, obitopuerp, fonteinv)) |>
  group_by(across(!obitos)) |>
  summarise(obitos = sum(as.numeric(obitos))) |>
  ungroup()

df_obitos_desconsiderados$idade <- as.numeric(df_obitos_desconsiderados$idade)


##Exportando os dados 
write.table(df_obitos_desconsiderados, 'R/databases/Obitos_desconsiderados_muni2021.csv', sep = ",", dec = ".", row.names = FALSE)



##Óbitos de grávidas e puérperas por estado dos anos de 1996 a 2021
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
write.table(df_obitos_uf, 'R/databases/obitos_por_uf2022.csv', sep = ",", dec = ".", row.names = FALSE)












