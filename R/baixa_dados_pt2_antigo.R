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
names(df_cid10) <- c("CAUSABAS", "causabas_capitulo", "causabas_categoria", "obitos")

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

## Baixando os dados preliminares do SIM de 2022 ---------------------------
dados_preliminares_2022_aux <- fetch_datasus(
  year_start = 2022, 
  year_end = 2022,
  information_system = "SIM-DO"
) 

dados_preliminares_2022 <- dados_preliminares_2022_aux |>
  mutate(
    ano_obito = as.numeric(substr(DTOBITO, nchar(DTOBITO) - 3, nchar(DTOBITO))),
    idade_obito_anos = if_else(
      IDADE != "000" & IDADE < "400",
      0,
      case_when(
        IDADE == "000" | is.na(IDADE) ~ 99,
        IDADE >= "400" & IDADE < "500" ~ as.numeric(substr(IDADE, 2, 3)),
        IDADE >= "500" ~ as.numeric(substr(IDADE, 2, 3)) + 100
      ),
      missing = 99
    ),
    def_raca_cor = case_when(
      RACACOR == "1" ~ "Branca",
      RACACOR == "2" ~ "Preta",
      RACACOR == "3" ~ "Amarela",
      RACACOR == "4" ~ "Parda",
      RACACOR == "5" ~ "Indígena",
      is.na(RACACOR) ~ "Ignorado"
    ),
    def_est_civil = case_when(
      ESTCIV == "9" | is.na(ESTCIV) ~ "Ignorado",
      ESTCIV == "1" ~ "Solteiro",
      ESTCIV == "2" ~ "Casado",
      ESTCIV == "3" ~ "Viúvo",
      ESTCIV == "4" ~ "Separado Judic./Divorciado",
      ESTCIV == "5" ~ "União consensual"
    ),
    def_loc_ocor = case_when(
      LOCOCOR == "9" | is.na(LOCOCOR) ~ "Ignorado",
      LOCOCOR == "1" ~ "Hospital",
      LOCOCOR == "2" ~ "Outro Estab. Saúde",
      LOCOCOR == "3" ~ "Domicílio",
      LOCOCOR == "4" ~ "Via Pública",
      LOCOCOR == "5" ~ "Outros",
      LOCOCOR == "6" ~ "Hospital"  #Parei aqui, faltam criar outras variáveis e descobrir o que é o 6
    ),
    .keep = "unused"
  ) |>
  mutate_at(
    vars(SEXO, OBITOGRAV, OBITOPUERP),
    as.numeric
  ) |>
  left_join(df_cid10) |>
  left_join(df_aux_municipios)
  
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
       ((CAUSABAS >= "F530"  &  CAUSABAS <= "F539") & (OBITOPUERP != 2)) |
       (CAUSABAS == "M830" & OBITOPUERP != 2))
  ) |>
  select(res_REGIAO, res_SIGLA_UF, res_MUNNOME, CODMUNRES, ano_obito, CAUSABAS, causabas_capitulo, causabas_categoria, OBITOGRAV, OBITOPUERP, def_raca_cor, idade_obito_anos, FONTEINV) |>
  mutate(obitos = 1) |>
  group_by(across(!obitos)) |>
  summarise(obitos = sum(obitos)) |>
  ungroup() 

colnames(df_maternos_preliminares_aux) <- c("regiao", "uf", "municipio", "codigo", "ano", "causabas", "capitulo_cid10", "causabas_categoria", "obitograv", "obitopuerp", "racacor", "idade", "fonteinv", "obitos")

df_maternos_preliminares_aux$codigo <- as.numeric(df_maternos_preliminares_aux$codigo)
df_maternos_preliminares_aux$ano <- as.numeric(df_maternos_preliminares_aux$ano)
df_maternos_preliminares_aux$idade <- as.numeric(df_maternos_preliminares_aux$idade)

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
write.table(df_obitos_maternos, 'dados_oobr_obitos_grav_puerp_maternos_oficiais_2022.csv', sep = ",", dec = ".", row.names = FALSE)



# Para a seção de análise cruzada -----------------------------------------
##Lendo o arquivo com os óbitos maternos para a análise cruzada de 1996 a 2021
dados_ac_1996_2021 <- read.csv("R/databases/Obitos_maternos_estendidos2021.csv") 

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
       ((CAUSABAS >= "F530"  &  CAUSABAS <= "F539") & (OBITOPUERP != 2)) |
       (CAUSABAS == "M830" & OBITOPUERP != 2))
  ) |>
  select(res_REGIAO, res_SIGLA_UF, res_MUNNOME, CODMUNRES, ano_obito, def_raca_cor, def_est_civil, ESC2010, def_loc_ocor, idade_obito_anos, PESO, def_assist_med, def_necropsia, FONTEINV, CAUSABAS, causabas_capitulo, causabas_categoria, OBITOGRAV, OBITOPUERP)

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
df_ac_preliminares$ano_obito <- as.numeric(df_ac_preliminares$ano_obito)

##Juntando as duas bases
df_obitos_maternos_ac <- full_join(
  dados_ac_1996_2021,
  df_ac_preliminares
)

##Exportando os dados 
write.table(df_obitos_maternos_ac, 'dados_oobr_obitos_grav_puerp_analise_cruzada_2022.csv', sep = ",", dec = ".", row.names = FALSE)


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
       ((CAUSABAS >= "F530"  &  CAUSABAS <= "F539") & (OBITOPUERP != 2)) |
       (CAUSABAS == "M830" & OBITOPUERP != 2))
  ) |>
  select(res_REGIAO, res_SIGLA_UF, res_MUNNOME, CODMUNRES, ano_obito, CAUSABAS, causabas_capitulo, causabas_categoria, OBITOGRAV, OBITOPUERP, def_raca_cor, idade_obito_anos, FONTEINV) |>
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
  df_descons_preliminares_aux,
  by = join_by(
    regiao, uf, municipio, ano, capitulo_cid10, causabas_categoria, periodo_do_obito, investigacao_cmm, racacor, idade,
    obitos
  )
)

##Exportando os dados 
write.table(df_obitos_desconsiderados, 'dados_oobr_obitos_grav_puerp_desconsiderados_2022.csv', sep = ",", dec = ".", row.names = FALSE)


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
write.table(df_obitos_uf, 'dados_oobr_obitos_grav_puerp_ufs_2022.csv', sep = ",", dec = ".", row.names = FALSE)



