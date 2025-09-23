library(tidyverse)

is_valid_numeric <- function(x) {
  # Try to convert to numeric and check if it's not NA
  !is.na(as.numeric(as.character(x)))
}

for (i in list.files(".")) {

  num_char = nchar(i)
  name = substr(i,0,num_char-4)
  
  if (file.info(i)$size > 1) {
    file_i <- read.csv(i)
    assign(name,file_i)
  } else {
    message(i, " is empty, skipping")
  }


  
  if (endsWith(name,"data_spreadsheet")) {
    depths = file_i[1,] %>% t() %>% as.data.frame()
    names(depths) = c("depths")
    depths = depths %>% dplyr::filter(depths != "")
    rownames(depths) = NULL
   
    if (any(grepl("#Chron", file_i$Col_1, fixed = TRUE))) {
    starter = tail(grep("#", file_i$Col_1), 1) + 1 
    trim_meta = file_i[1:(starter-1),1:length(file_i)] %>% t() %>% as.data.frame()
    names(trim_meta)[1] = "depths"
    names(trim_meta)[2:length(names(trim_meta))] = sub("^#", "", trim_meta[1,2:length(names(trim_meta))])
    
    trim_meta = trim_meta %>% drop_na()
    
    
    numeric_meta <- apply(trim_meta, 1, function(row) any(sapply(row, is_valid_numeric)))
    
    # Find the first row index where any column is numeric
    first_meta_num <- which(numeric_meta)[[1]]
    
    
    ages = trim_meta[first_meta_num:length(trim_meta[[1]]),1:length(trim_meta)]
    depths = depths[1:length(ages[[1]]),]  %>% as.data.frame() %>% drop_na()
    names(depths) = c("depths")
    
    age_meta = trim_meta[1:(first_meta_num-2),2:length(trim_meta)] %>% as.data.frame()
    names(age_meta) = names(trim_meta)[2:length(trim_meta)] 
    age_meta = age_meta %>%
      select(contains("Chron")) %>% t() %>% as.data.frame()
    age_meta$Col_1 = row.names(age_meta)
    names(age_meta) = c("chroncode","pubtext","na","ageunits")
    
    age_meta = age_meta %>% dplyr::select(chroncode,pubtext,ageunits)
    analythick = trim_meta %>% select(Anal.Thick,depths)
    names(analythick) = c("analysisunit_thickness","depths")
    name3 = paste0(substr(i,0,num_char-15),"age_meta")
    write.csv(age_meta,paste0(name3,".csv"),row.names=FALSE)
    } else {
      starter=2
    }
    
    
    
    trimmed = file_i[starter:length(file_i[[1]]),1:length(file_i)] %>%
      t() %>%
      as.data.frame()
    
    for (k in seq(length(trimmed))) {
      if (trimmed[3,k] != "") {
        trimmed[2,k] = paste0("var_",trimmed[2,k],"__",trimmed[3,k])
      } else {trimmed[2,k] = paste0("var_",trimmed[2,k])}
    }
    names(trimmed) = trimmed[2,]
    
    rows_with_numeric <- apply(trimmed, 1, function(row) any(sapply(row, is_valid_numeric)))
    
    # Find the first row index where any column is numeric
    first_row_with_numeric <- which(rows_with_numeric)[[1]]
    
    trimmed = trimmed[first_row_with_numeric:length(trimmed[[1]]),1:length(trimmed)] %>%
      filter(!if_all(everything(), is.na))
    
    units = file_i[starter:length(file_i[[1]]),1:length(file_i)]  %>% select(Col_2,Col_3,Col_4)
    names(units) = c("variablename","plus","units")
    units = units %>% dplyr::mutate(variablename = case_when(
      plus != "" ~ paste0(variablename,"__",plus),
      TRUE ~ variablename
    )) %>% dplyr::select(!plus)
    
    depths$depths = as.numeric(depths$depths)
    ages$depths = as.numeric(ages$depths)
    if (any(grepl("#Chron", file_i$Col_1, fixed = TRUE))) {
    t2 = depths %>% left_join(ages) %>% cbind(trimmed) %>%
      pivot_longer(cols=starts_with("var_"),names_to="variablename",values_to = "value") %>%
      dplyr::mutate(variablename = substr(variablename,5,nchar(variablename)))
    } else {
      t2 = depths %>% cbind(trimmed) %>%
        pivot_longer(cols=starts_with("var_"),names_to="variablename",values_to = "value") %>%
        dplyr::mutate(variablename = substr(variablename,5,nchar(variablename))) %>% left_join(analythick)
    }
    
    t2 = t2 %>% 
      dplyr::mutate(variablename = case_when(
        grepl("\\.\\d+$", variablename) ~ sub("\\.\\d+$", "",variablename),
        TRUE ~ variablename
      )) %>% dplyr::mutate(value=as.numeric(value)) 
    

    t2 = t2 %>%
      separate(variablename, into = c("variablename", "element"), sep = "__", remove = FALSE)
    
    if (any(grepl("Chron",names(t2)))) {
    t2 = t2 %>% pivot_longer(cols=starts_with("Chron"),names_to="ChronNumber",values_to="Age") %>% 
      dplyr::mutate(ChronNumber = as.numeric(substr(ChronNumber,6,nchar(ChronNumber))))
    
    }
    
    name2 = paste0(substr(i,0,num_char-15),"long")
    assign(name2,t2)
    
    
  
    write.csv(t2,paste0(name2,".csv"),row.names=FALSE)
  }
  
 
  
}

data_dfs <- ls(pattern = "_data_long$")

for (data_name in data_dfs) {
  # Derive the matching site name
  prefix <- sub("_data_long$", "", data_name)
  site_name <- paste0(prefix, "_site")
  collection_name <- paste0(prefix, "_collectionunit")
  model_name <- paste0(prefix, "_agemodels")
  contact_name = paste0(prefix, "_contacts")
  geochron_name = paste0(prefix,"_geochrondataset")
  pub_name = paste0(prefix,"_publications")
  auth_name = paste0(prefix,"_authors")
  datasets_name = paste0(prefix,"_datasets")
  
  # Skip if there's no matching site dataframe
  if (!exists(site_name)) next
  
  # Get dataframes
  data_df <- get(data_name)
  site_df <- get(site_name)
  collection_df = get(collection_name)
  
  if (any(grepl(model_name,ls()))) {
  model_df = get(model_name)}
  contact_df = get(contact_name)
  geochron_df = get(geochron_name)
  pub_df = get(pub_name)
  author_df = get(auth_name)
  datasets_df = get(datasets_name)
  
  # Replicate the single site row to match the number of rows in data_df
  site_expanded <- site_df[rep(1, nrow(data_df)), , drop = FALSE]
  coll_expanded <- collection_df[rep(1, nrow(data_df)), , drop = FALSE]
  
  # Combine
  combined <- cbind(data_df, site_expanded) %>% cbind(coll_expanded)
  
  collectors = combined$Collectors[[1]]
  if (grepl(",",collectors)) {
    collector_list = as.numeric(strsplit(collectors, ",")[[1]]) } else {
      collector_list = collectors[1]
    }
  neocolls = contact_df %>% dplyr::filter(InternalContactID %in% collector_list) %>% select(NeotomaContactID)
  neocolls = neocolls[[1]]
  neocolls = paste0(neocolls, collapse=",")
  
  neocoll_names = contact_df %>% dplyr::filter(InternalContactID %in% collector_list) %>% select(ShortContactName)
  neocoll_names = neocoll_names[[1]]
  neocoll_names = paste0(neocoll_names,collapse="|")
  combined = combined %>% mutate(Collectors_ID = neocolls)
  combined$Collectors_Names = neocoll_names
  #multiplier_coll = length(combined[[1]])/length(collector_list)
  #dec_part = multiplier_coll - floor(multiplier_coll)
  #addon = dec_part*length(collector_list)
  #collector_lengthed = c(rep(collector_list,(multiplier_coll)),rep(collector_list[[1]],addon))
  #combined = combined %>% select(!Collectors) %>% cbind(collector_lengthed) %>% 
  #  rename(Collectors = collector_lengthed )
  
  
  multiplier_contact = length(combined[[1]])/length(contact_df[[1]])
  floor_contact = floor(multiplier_contact)
  dec_part_contact = multiplier_contact - floor(multiplier_contact)
  addon_contact = dec_part_contact*length(contact_df[[1]])
  contact_lengthed = contact_df[rep(1:nrow(contact_df), each = multiplier_contact), ]
  
  if (floor_contact*length(contact_df[[1]]) != length(combined[[1]])) {
  contact_lengthed = contact_lengthed %>% rbind(contact_df[1:addon_contact,]) }
  combined = combined %>% cbind(contact_lengthed)
  
  
  multiplier_geochron = length(combined[[1]])/length(geochron_df[[1]])
  floor_geochron = floor(multiplier_geochron)
  dec_part_geochron = multiplier_geochron - floor(multiplier_geochron)
  addon_geochron = dec_part_geochron*length(geochron_df[[1]])
  geochron_lengthed = geochron_df[rep(1:nrow(geochron_df), each = multiplier_geochron), ]
  if (floor_geochron*length(geochron_df[[1]]) != length(combined[[1]])) {
  geochron_lengthed = geochron_lengthed %>% rbind(geochron_df[1:addon_geochron,])}
  
  geochron_lengthed = geochron_lengthed %>% left_join(contact_df,by=join_by(Investigators_geochron==InternalContactID)) %>% 
    dplyr::select(SampleID,AnalysisUnitID,Investigators_geochron,Method,AgeUnits_Sample,Depth,Thickness,LabNumber,
                  SampleAge,ErrorOlder,ErrorYounger,Sigma,StdDev,GreaterThan,
                  MaterialDated,SampleNotes,PublicationsText,PublicationIDs,NeotomaContactID,ShortContactName)
  
  geochron_lengthed = geochron_lengthed %>% rename(Investigators_geochron_neoID = NeotomaContactID,Investigators_geochron_Name = ShortContactName)
  combined = combined %>% cbind(geochron_lengthed)
  
  
  
  multiplier_pub = length(combined[[1]])/length(pub_df[[1]])
  floor_pub = floor(multiplier_pub)
  dec_part_pub = multiplier_pub - floor(multiplier_pub)
  addon_pub = dec_part_pub*length(pub_df[[1]])
  pub_lengthed = pub_df[rep(1:nrow(pub_df), each = multiplier_pub), ]
  if (floor_pub*length(pub_df[[1]]) != length(combined[[1]])) {
    pub_lengthed = pub_lengthed %>% rbind(pub_df[1:addon_pub,])}
  combined = combined %>% cbind(pub_lengthed)
  
  
  multiplier_author = length(combined[[1]])/length(author_df[[1]])
  floor_author = floor(multiplier_author)
  dec_part_author = multiplier_author - floor(multiplier_author)
  addon_author = dec_part_author*length(author_df[[1]])
  author_lengthed = author_df[rep(1:nrow(author_df), each = multiplier_author), ]
  if (floor_author*length(author_df[[1]]) != length(combined[[1]])) {
    author_lengthed = author_lengthed %>% rbind(author_df[1:addon_author,])}
  combined = combined %>% cbind(author_lengthed)
  
  authors_list = combined %>% 
    group_by(PublicationID_Author) %>% 
    summarize(author_list = unique(ContactID))
  

  neoauths = authors_list %>% left_join(contact_df,by=join_by(author_list==InternalContactID)) %>%
    group_by(PublicationID_Author) %>%
    summarise(
      authorIDs = paste(unique(NeotomaContactID), collapse = ","),
      authorNames = paste(unique(ShortContactName), collapse = "|"),
      .groups = "drop"
    )
  
  combined = combined %>% left_join(neoauths)
  
  investigators = datasets_df$Investigators[[1]]
  processors = datasets_df$Processors[[1]]
  if (grepl(",",processors)) {
    proc_list = as.numeric(strsplit(processors, ",")[[1]]) } else {
    proc_list = processors[1]
    }
  
  if (grepl(",",investigators)) {
  invest_list = as.numeric(strsplit(investigators, ",")[[1]]) } else {
    invest_list = investigators[1]
  }
  
  neoinvest = contact_df %>% dplyr::filter(InternalContactID %in% invest_list) %>% select(NeotomaContactID)
  neoinvest = neoinvest[[1]]
  neoinvest = paste0(neoinvest, collapse=",")
  
  neoinvest_names = contact_df %>% dplyr::filter(InternalContactID %in% invest_list) %>% select(ShortContactName)
  neoinvest_names = neoinvest_names[[1]]
  neoinvest_names = paste0(neoinvest_names,collapse="|")
  
  neoprocs = contact_df %>% dplyr::filter(InternalContactID %in% proc_list) %>% select(NeotomaContactID)
  neoprocs = neoprocs[[1]]
  neoprocs = paste0(neoprocs, collapse=",")
  
  neoproc_names = contact_df %>% dplyr::filter(InternalContactID %in% proc_list) %>% select(ShortContactName)
  neoproc_names = neoproc_names[[1]]
  neoproc_names = paste0(neoproc_names,collapse="|")
  combined = combined %>% mutate(Processors_ID = neoprocs,Investigators_ID = neoinvest)
  combined$Processors_Names = neoproc_names
  combined$Investigators_Names = neoinvest_names
  #multiplier_invest = length(combined[[1]])/length(invest_list)
  #multiplier_proc = length(combined[[1]])/length(proc_list)
  #dec_part_inv = multiplier_invest - floor(multiplier_invest)
  #dec_part_proc = multiplier_proc - floor(multiplier_proc)
  #addon_inv = dec_part_inv*length(invest_list)
  #invest_lengthed = c(rep(invest_list,(multiplier_invest)),rep(invest_list[[1]],round(addon_inv)))
  #addon_proc = dec_part_proc*length(proc_list)
  #proc_lengthed = c(rep(proc_list,(multiplier_proc)),rep(proc_list[[1]],round(addon_proc)))
  #combined = combined %>% cbind(invest_lengthed) %>% cbind(proc_lengthed) %>%
  #  rename(Investigators = invest_lengthed, Processors = proc_lengthed)
  
  datasets_df = datasets_df %>% dplyr::select(DatasetType,Name)
  
  datasets_expanded <- datasets_df[rep(1, nrow(combined)), , drop = FALSE]
  
  combined = combined %>% cbind(datasets_expanded)
  
  if (any(grepl(model_name,ls()))) {
  combined = combined %>% dplyr::left_join(model_df,by=join_by(ChronNumber)) }
  
  # Save with a new name, e.g. prefix_pollen_combined
  assign(paste0(prefix, "_combined"), combined)
  
  
  write.csv(combined,paste0(prefix,"_combined.csv"),row.names=FALSE)
}





