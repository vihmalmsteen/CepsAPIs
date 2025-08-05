from classes.DBData import DBData


if __name__ == '__main__':
    
    db = DBData()

    prompt = input('Query data from DB (1) or list editions (2)?\nAnswer >>> ')

    if int(prompt) == 1:
        
        total_rows_in_batches = input('rows in batches (default: 10.000.000) >>> ')
        edicao = input('edition (default: Teste) >>> ')
        data_compra_ini = input('initial buy date (default: "1900-01-01") >>> ')
        data_compra_fini = input('final buy date (default: "2100-12-31") >>> ')
        limit_max_rows = input('query limit (default: 10.000.000) >>> ')

        db.query_data(
            edicao =                DBData.query_data.__defaults__[0] if edicao == ''                else edicao, 
            data_compra_ini =       DBData.query_data.__defaults__[1] if data_compra_ini == ''       else data_compra_ini, 
            data_compra_fini =      DBData.query_data.__defaults__[2] if data_compra_fini == ''      else data_compra_fini, 
            limit_max_rows =        DBData.query_data.__defaults__[3] if limit_max_rows == ''        else limit_max_rows, 
            total_rows_in_batches = DBData.query_data.__defaults__[4] if total_rows_in_batches == '' else total_rows_in_batches
            )
    

    elif int(prompt) == 2:
        like_param = input('like param (default: None) >>> ')
        
        db.editions(
            like_param = DBData.editions.__defaults__[0] if like_param == '' else like_param
            )
    
    
    else:
        exit()

