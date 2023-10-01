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

estados <- c('RO','AC','AM','RR','PA','AP','TO','MA','PI','CE','RN','PB','PE','AL','SE','BA','MG','ES','RJ','SP','PR','SC','RS','MS','MT','GO','DF')
endpoint <- paste0(url_base,"/","sql_query")

##Óbitos maternos oficiais dos anos de 1996 a 2021
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
  select(!c(codigo, causabas, obitograv, obitopuerp, fonteinv)) |>
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
  select(!c(codigo, causabas, obitograv, obitopuerp, fonteinv)) 

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
  select(!c(codigo, causabas, obitograv, obitopuerp, fonteinv)) |>
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
write.table(df_obitos_uf, 'R/databases/Obitos_por_uf2021.csv', sep = ",", dec = ".", row.names = FALSE)












