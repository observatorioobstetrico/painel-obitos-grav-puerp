library(dplyr)
library(janitor)
library(tidyr)
library(data.table)

#Lendo o arquivo com os dados preliminares do SIM de 2022
dados_preliminares_2022 <- fread("R/databases/DO22OPEN_T.csv", encoding = "UTF-8") 

# Para a seção de óbitos maternos oficiais --------------------------------
##Lendo o arquivo com os óbitos maternos de 1996 a 2021
dados_obitos_maternos_1996_2021 <- read.csv("R/databases/Obitos_maternos_muni2021.csv") 

##Nos dados preliminares, filtrando apenas pelos óbitos maternos
df_maternos_preliminares_aux <- dados_preliminares_2022 |>
  filter(
    SEXO == 2,
    ((CAUSABAS >= "O000"  &  CAUSABAS <= "O959") |
       (CAUSABAS >= "O980"  &  CAUSABAS <= "O999") |
       (CAUSABAS == "A34" & OBITOPUERP != 2) |
       ((CAUSABAS >= "B200"  &  CAUSABAS <= "B249") & (OBITOGRAV == 1 | OBITOPUERP == 1)) |
       (CAUSABAS == "D392" & (OBITOGRAV == 1 | OBITOPUERP == 1)) |
       (CAUSABAS == "E230" & (OBITOGRAV == 1 | OBITOPUERP == 1)) |
       ((CAUSABAS >= "F530"  &  CAUSABAS <= "F539") & (OBITOPUERP != 2 | OBITOPUERP == " ")) |
       (CAUSABAS == "M830" & OBITOPUERP != 2))
  ) |>
  select(res_REGIAO, res_SIGLA_UF, res_MUNNOME, res_codigo_adotado, ano_obito, CAUSABAS, causabas_capitulo, causabas_categoria, OBITOGRAV, OBITOPUERP, def_raca_cor, idade_obito_anos, FONTEINV) |>
  mutate(obitos = 1) |>
  group_by(across(!obitos)) |>
  summarise(obitos = sum(obitos)) |>
  ungroup() 

colnames(df_maternos_preliminares_aux) <- c("regiao", "uf", "municipio", "codigo", "ano", "causabas", "capitulo_cid10", "causabas_categoria", "obitograv", "obitopuerp", "racacor", "idade", "fonteinv", "obitos")

##Realizando os tratamentos necessários nos dados preliminares
df_maternos_preliminares <- df_maternos_preliminares_aux |>
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

##Juntando as duas bases
df_obitos_maternos <- full_join(
  dados_obitos_maternos_1996_2021,
  df_maternos_preliminares,
  by = join_by(
    regiao, uf, municipio, ano, capitulo_cid10, causabas_categoria, tipo_de_morte_materna, periodo_do_obito,
    investigacao_cmm, racacor, idade, obitos
  )
)

##Exportando os dados 
write.table(df_obitos_maternos, 'Obitos_maternos_muni2022.csv', sep = ",", dec = ".", row.names = FALSE)


# Para a seção de análise cruzada -----------------------------------------
##Lendo o arquivo com os óbitos maternos para a análise cruzada de 1996 a 2021
dados_ac_1996_2021 <- read.csv("R/databases/Obitos_maternos_estendidos_1996_2021.csv") 

##Nos dados preliminares, filtrando apenas pelos óbitos maternos
df_ac_preliminares_aux <- dados_preliminares_2022 |>
  filter(
    SEXO == 2,
    ((CAUSABAS >= "O000"  &  CAUSABAS <= "O959") |
       (CAUSABAS >= "O980"  &  CAUSABAS <= "O999") |
       (CAUSABAS == "A34" & OBITOPUERP != 2) |
       ((CAUSABAS >= "B200"  &  CAUSABAS <= "B249") & (OBITOGRAV == 1 | OBITOPUERP == 1)) |
       (CAUSABAS == "D392" & (OBITOGRAV == 1 | OBITOPUERP == 1)) |
       (CAUSABAS == "E230" & (OBITOGRAV == 1 | OBITOPUERP == 1)) |
       ((CAUSABAS >= "F530"  &  CAUSABAS <= "F539") & (OBITOPUERP != 2 | OBITOPUERP == " ")) |
       (CAUSABAS == "M830" & OBITOPUERP != 2))
  ) |>
  select(res_REGIAO, res_SIGLA_UF, res_MUNNOME, res_codigo_adotado, ano_obito, def_raca_cor, def_est_civil, ESC2010, def_loc_ocor, idade_obito_anos, PESO, def_assist_med, def_necropsia, FONTEINV, CAUSABAS, causabas_capitulo, causabas_categoria, OBITOGRAV, OBITOPUERP)

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

df_ac_preliminares$idade_obito <- as.numeric(df_ac_preliminares$idade_obito)
df_ac_preliminares$idade_obito[is.na(df_ac_preliminares$idade_obito)] <- 99
df_ac_preliminares[is.na(df_ac_preliminares)] <- "Ignorado"

##Juntando as duas bases
df_obitos_maternos_ac <- full_join(
  dados_ac_1996_2021,
  df_ac_preliminares
)

##Exportando os dados 
write.table(df_obitos_maternos_ac, 'Obitos_maternos_estendidos_1996_2021.csv', sep = ",", dec = ".", row.names = FALSE)


# Para a seção de óbitos maternos desconsiderados -------------------------
##Lendo o arquivo com os óbitos de gestantes e puérperas desconsiderados de 1996 a 2021
dados_desconsiderados_1996_2021 <- read.csv("R/databases/Obitos_desconsiderados_muni2021.csv") 

##Nos dados preliminares, filtrando apenas pelos óbitos de gestantes e puérperas desconsiderados
df_descons_preliminares_aux <- dados_preliminares_2022 |>
  filter(
    SEXO == 2,
    OBITOGRAV == 1 | OBITOPUERP == 1 | OBITOPUERP == 2,
    !((CAUSABAS >= "O000"  &  CAUSABAS <= "O959") |
       (CAUSABAS >= "O980"  &  CAUSABAS <= "O999") |
       (CAUSABAS == "A34" & OBITOPUERP != 2) |
       ((CAUSABAS >= "B200"  &  CAUSABAS <= "B249") & (OBITOGRAV == 1 | OBITOPUERP == 1)) |
       (CAUSABAS == "D392" & (OBITOGRAV == 1 | OBITOPUERP == 1)) |
       (CAUSABAS == "E230" & (OBITOGRAV == 1 | OBITOPUERP == 1)) |
       ((CAUSABAS >= "F530"  &  CAUSABAS <= "F539") & (OBITOPUERP != 2 | OBITOPUERP == " ")) |
       (CAUSABAS == "M830" & OBITOPUERP != 2))
  ) |>
  select(res_REGIAO, res_SIGLA_UF, res_MUNNOME, res_codigo_adotado, ano_obito, CAUSABAS, causabas_capitulo, causabas_categoria, OBITOGRAV, OBITOPUERP, def_raca_cor, idade_obito_anos, FONTEINV) |>
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

##Juntando as duas bases
df_obitos_desconsiderados <- full_join(
  dados_desconsiderados_1996_2021,
  df_descons_preliminares_aux,
  by = join_by(
    regiao, uf, municipio, ano, capitulo_cid10, causabas_categoria, periodo_do_obito, investigacao_cmm, racacor, idade,
    obitos
  )
)

##Exportando os dados 
write.table(df_obitos_desconsiderados, 'Obitos_desconsiderados_muni2022.csv', sep = ",", dec = ".", row.names = FALSE)

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
write.table(df_obitos_uf, 'Obitos_por_uf2022.csv', sep = ",", dec = ".", row.names = FALSE)


