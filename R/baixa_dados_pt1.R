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


# Baixando os dados do SIM de 1996 a 2022 ---------------------------------
ano1 <- c(1996, 2002, 2008, 2014, 2020)
ano2 <- c(2001, 2007, 2013, 2019, 2022)

for (i in 1:length(ano1)) {
  df_sim_aux <- fetch_datasus(
    year_start = ano1[i],
    year_end = ano2[i],
    information_system = "SIM-DO"
  ) |>
    clean_names()
  
  df_sim <- df_sim_aux |>
    mutate_if(is.numeric, as.character) |>
    mutate(
      causabas = ifelse(causabas %in% c("O935", "O937"), "O95", causabas),
      obitograv = ifelse(is.na(obitograv), "9", obitograv),
      obitopuerp = ifelse(is.na(obitopuerp) | obitopuerp %in% c("0", "4", "8"), "9", obitopuerp),
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
      escolaridade = ifelse(
        "esc2010" %in% names(df_sim_aux),
        case_when(
          esc2010 == "0" ~ "Sem escolaridade",
          esc2010 == "1" ~ "Fundamental I",
          esc2010 == "2" ~ "Fundamental II",
          esc2010 == "3" ~ "Médio",
          esc2010 == "4" ~ "Superior incompleto",
          esc2010 == "5" ~ "Superior completo",
          is.na(esc2010) | esc2010 == "9" ~ "Ignorado"
        ),
        "Ignorado"
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
      investigacao_cmm = ifelse(
        "fonteinv" %in% names(df_sim_aux),
        if_else(
          fonteinv == "1",
          true = "Sim", 
          false = if_else(fonteinv == "9", true = "Sem informação", false = "Não",  missing = "Sem informação"),
          missing = "Sem informação"
        ),
        "Sem informação"
      )
    ) |>
    left_join(df_aux_municipios) |>
    left_join(df_cid10)
  
  
  ## Para a seção de óbitos maternos oficiais --------------------------------
  ### Filtrando, nos dados do SIM, apenas os óbitos maternos -----------------
  df_obitos_maternos <- df_sim |>
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
      codigo = res_codigo_adotado, municipio, uf, regiao, ano, causabas, causabas_categoria, capitulo_cid10,
      tipo_de_morte_materna, periodo_do_obito, investigacao_cmm, racacor, idade, obitos
    ) |>
    group_by(across(!obitos)) |>
    summarise(obitos = sum(obitos)) |>
    ungroup() |>
    arrange(codigo)

  ## Para a seção de garbage codes -------------------------------------------
  ### Lendo o dataframe que recebe as CIDs consideradas garbage codes --------
  df_garbage_codes <- read.csv("R/databases/df_garbage_codes.csv")
  
  ### Filtrando os óbitos maternos preenchidos com garbage codes -------------
  df_maternos_garbage_codes <- df_obitos_maternos |>
    filter(causabas %in% df_garbage_codes$causabas)
  

  ## Para a seção de análise cruzada -----------------------------------------
  ### Filtrando, nos dados do SIM, apenas os óbitos maternos -----------------
  df_obitos_maternos_ac <- df_sim |>
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
  
  
  ## Para a seção de óbitos maternos desconsiderados -------------------------
  ### Filtrando, nos dados do SIM, apenas pelos óbitos desconsiderados -------
  df_obitos_desconsiderados <- df_sim |>
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
    mutate(
      obitos = 1,
      .keep = "unused",
    ) |>
    select(
      codigo = res_codigo_adotado, ano, municipio, uf, regiao, capitulo_cid10, causabas_categoria,
      periodo_do_obito, racacor, idade, investigacao_cmm, obitos
    ) |>
    group_by(across(!obitos)) |>
    summarise(obitos = sum(obitos)) |>
    ungroup() |>
    arrange(codigo)
  
    
  
  if (i == 1) {
    df_obitos_maternos_completo <- df_obitos_maternos
    df_maternos_garbage_codes_completo <- df_maternos_garbage_codes
    df_obitos_maternos_ac_completo <- df_obitos_maternos_ac
    df_obitos_desconsiderados_completo <- df_obitos_desconsiderados
  } else {
    df_obitos_maternos_completo <- full_join(df_obitos_maternos, df_obitos_maternos_completo) |> arrange(codigo, ano)
    df_maternos_garbage_codes_completo <- full_join(df_maternos_garbage_codes, df_maternos_garbage_codes_completo) |> arrange(codigo, ano)
    df_obitos_maternos_ac_completo <- full_join(df_obitos_maternos_ac, df_obitos_maternos_ac_completo) |> arrange(codigo, ano)
    df_obitos_desconsiderados_completo <- full_join(df_obitos_desconsiderados, df_obitos_desconsiderados_completo) |> arrange(codigo, ano)
  }
  
  rm(df_sim_aux, df_sim)

}

df_obitos_maternos_completo |> filter(ano == 2022) |> pull(obitos) |> sum()
df_obitos_maternos_completo |> filter(ano == 1996) |> pull(obitos) |> sum()











