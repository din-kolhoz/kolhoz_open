##
get_df_sheck <- function(start_date = '2022-05-01',end_date = '2022-06-01',
                         check_sales_start = 0, check_sales_end = 1000000,
                         check_margin_start = 0, check_margin_end = 1000000,
                         con_dalion_en) {
  
  request_code <- paste0("SELECT [code_1c_shop],
[department_name],
[ds],
ROUND(SUM([sales_amount_fact]),2) AS sales,
ROUND(SUM([prime_cost]),2) AS prime,
COUNT(distinct [guid_check]) AS n_checks,
SUM([check_time_sec]) AS check_time

FROM(
SELECT [code_1c_shop],
[department_name],
[date] AS ds,
[sales_amount_fact],
[prime_cost],
[guid_check],
DATEDIFF(SECOND, [check_time_opening], [check_time_closing]) AS check_time_sec

FROM [dalion_en].[dbo].[sales]
WHERE ([date] BETWEEN 'Repl_start_date' AND 'Repl_end_date') 
AND ([sales_amount_fact] BETWEEN Repl_chsl0 AND Repl_chsl1)
AND (([sales_amount_fact]-[prime_cost]) BETWEEN Repl_chmrg0 AND Repl_chmrg1)) AS df_sales_time

GROUP BY [ds], [code_1c_shop], [department_name]
ORDER BY [code_1c_shop], [department_name], [ds]")
  
  # date
  request_code <- gsub("Repl_start_date", start_date, request_code)
  request_code <- gsub("Repl_end_date",   end_date,   request_code)
  # check sale
  request_code <- gsub("Repl_chsl0",   check_sales_start,   request_code)
  request_code <- gsub("Repl_chsl1",   check_sales_end,   request_code)
  # check margin
  request_code <- gsub("Repl_chmrg0",   check_margin_start,   request_code)
  request_code <- gsub("Repl_chmrg1",   check_margin_end,   request_code)
  
  df_sheck <- dbGetQuery(con_dalion_en,request_code)
  df_sheck$ds <- as.Date(df_sheck$ds)
  
  df_sheck <- df_sheck %>%
    mutate(dep_id = case_when(department_name == 'Мясной' ~ 'СМ',
                              department_name == 'Колбасный' ~ 'БЛ',
                              department_name == 'Кондитерский' ~ 'НК',
                              department_name == 'Молочный' ~ 'ЛМ',
                              department_name == 'Рыбный' ~ 'БР',
                              department_name == 'Кулинария' ~ 'ЛК'), .after = code_1c_shop) %>%
    
    mutate(dep_code = case_when(department_name == 'Мясной' ~ 'МЯСО',
                                department_name == 'Колбасный' ~ 'КОЛБАСА',
                                department_name == 'Кондитерский' ~ 'КОНДИТ',
                                department_name == 'Молочный' ~ 'МОЛОЧКА',
                                department_name == 'Рыбный' ~ 'РЫБА',
                                department_name == 'Кулинария' ~ 'КУЛИНАРИЯ'), .after = code_1c_shop) %>%
    
    mutate(dep_name = department_name) %>%
    dplyr::select(-department_name) %>%
    
    mutate(adr = case_when(code_1c_shop == '00001' ~ 'Альпийский25',
                           code_1c_shop == '00021' ~ 'БелыКуна16',
                           code_1c_shop == '00005' ~ 'Будапештская71',
                           code_1c_shop == '00004' ~ 'Будапештская85',
                           code_1c_shop == '00019' ~ 'Европейский8',
                           code_1c_shop == '00016' ~ 'Индустриальный34',
                           code_1c_shop == '00013' ~ 'Комендантский17',
                           code_1c_shop == '00015' ~ 'Комендантский26',
                           code_1c_shop == '00026' ~ 'Ленинградская5',
                           code_1c_shop == '00025' ~ 'Ленинский82',
                           code_1c_shop == '00022' ~ 'Ленинский90',
                           code_1c_shop == '00014' ~ 'Парнас',
                           code_1c_shop == '00008' ~ 'Пражская17',
                           code_1c_shop == '00002' ~ 'Просвещения69',
                           code_1c_shop == '00023' ~ 'Савушкина128',
                           code_1c_shop == '00006' ~ 'Софийская29',
                           code_1c_shop == '00024' ~ 'Художников26',
                           code_1c_shop == '00017' ~ 'Энгельса128',
                           code_1c_shop == '00020' ~ 'Энгельса134',
                           code_1c_shop == '00027' ~ 'Шувалова11',
                           code_1c_shop == '00028' ~ 'Менделеева11',
                           code_1c_shop == '00029' ~ 'Гражданский107',
                           code_1c_shop == '00030' ~ 'ФедораАбрамова8',
                           code_1c_shop == '00033' ~ 'Петровский2',
                           code_1c_shop == '00034' ~ 'Толубеевский14',
                           code_1c_shop == '00035' ~ 'Парашютная63',
                           code_1c_shop == '00040' ~ 'Графская15'), .after = code_1c_shop) %>%
    arrange(code_1c_shop,dep_name,ds) %>%
    mutate(n_month = month(ds),
           n_year  = year(ds),
           margine = sales - prime,
           margine_pers = round(100*margine/prime,2))
  
  return(df_sheck)
}