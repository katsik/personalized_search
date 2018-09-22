import json
import os
import io
import codecs

def get_all_products():
    path_to_products = '../products.csv'
    path_to_products_categories = '../product-categories.csv'
    path_to_write_file = 'items_info_1/products.json'

    products = dict()
    with open(path_to_products,'r') as prods:
        prods.readline()
        for line in prods:
            info = line.split(';')
            tokens = info[2].split(',')
            tokens[-1] = tokens[-1].strip()
            products[info[0]] = {"pricelog2" : info[1], "product_name_tokens":tokens}
    with open(path_to_products_categories,'r') as categories:
        categories.readline()
        cats = dict()
        for line in categories:
            info = line.split(';')
            itemId = info[0]
            cat = info[1]
            cats[itemId] = cat
    for item in products:
        try:
            products[item]['category'] = cats[item].strip()
        except Exception:
            print ("Item with ID: %s not in categories" %item)
            products[item]['category'] = "N/A"  
    specifics = get_product_specifics()
    print "Passing the specifics..."
    for item in products:
        try:
            products[item]['bought'] = specifics[item]['bought']
            products[item]['viewed'] = specifics[item]['viewed']
            products[item]['clicked'] = specifics[item]['clicked']
        except Exception:
            products[item]['bought'] = 0
            products[item]['viewed'] = 0
            products[item]['clicked'] = 0

    with open(path_to_write_file,'w') as jsonfile:
        json.dump(products,jsonfile,indent = 4, sort_keys = True)


# For each product you get the amount of times it was bought, clicked & visited
# For each user get which products he/she visited, bought and/or clicked
def get_product_specifics():
    path_to_train_purchases = '../train-purchases.csv'
    path_to_train_item_views = '../train-item-views.csv'
    path_to_train_click = '../train-clicks.csv'
    specifics = dict()
    with open(path_to_train_purchases,'r') as purchases:
        purchases.readline()
        for line in purchases:
            info = line.split(';')
            itemId = info[-1].strip()           
            if itemId not in specifics:
                specifics[itemId] = dict()
                specifics[itemId]['viewed'] = 0
                specifics[itemId]['clicked'] = 0
            specifics[itemId]['bought'] = specifics[itemId].get('bought',0) + 1
    with open(path_to_train_item_views,'r') as views:
        views.readline()
        for line in views:
            info = line.split(';')
            itemId = info[2]            
            if itemId not in specifics:
                specifics[itemId] = dict()
                specifics[itemId]['bought'] = 0
                specifics[itemId]['clicked'] = 0
            specifics[itemId]['viewed'] = specifics[itemId].get('viewed',0) + 1
    with open(path_to_train_click,'r') as clicks:
        clicks.readline()
        for line in clicks:
            info = line.split(';')
            itemId = info[2].strip()            
            if itemId not in specifics:
                specifics[itemId] = dict()
                specifics[itemId]['bought'] = 0
                specifics[itemId]['viewed'] = 0
            specifics[itemId]['clicked'] = specifics[itemId].get('clicked',0) + 1
    return specifics

def get_sum_of_specifics():
    specifics = get_product_specifics()
    print "Got specifics..."
    specifics_sum = dict()
    for item in specifics:
        specifics_sum['bought'] = specifics_sum.get('bought',0) + specifics[item].get('bought',0)
        specifics_sum['viewed'] = specifics_sum.get('viewed',0) + specifics[item].get('viewed',0)
        specifics_sum['clicked'] = specifics_sum.get('clicked',0) + specifics[item].get('clicked',0)
    return specifics_sum

def get_item_json_for_elastic():
    if not os.path.exists("items_info_1"):
        os.makedirs("items_info_1")
    os.chdir("items_info_1")
    info_json =dict()

    info_file = open("items_complete_info_elastic_2.json","w")
    specifics = get_sum_of_specifics()
    with open('products.json','r') as productjson:
        products = json.load(productjson)
    

    print "Got products. Starting iterating..."
    for product in products:
        info_json['itemId'] = int(product)
        info_json['category'] = products[product]['category']
        info_json['pricelog2'] = products[product]['pricelog2']
        info_json['name'] = " ".join(products[product]['product_name_tokens'])
        info_json['bought_by_current_user'] = 0
        info_json['viewed_by_current_user'] = 0
        info_json['bought_multiple_times_by_current_user'] = 0
        info_json['viewed_multiple_times_by_current_user'] = 0
        info_json['times_viewed'] = products[product]['viewed']
        info_json['times_bought'] = products[product]['bought']
        info_json['times_clicked'] = products[product]['clicked']
        info_json['recommended_for_current_user'] = 0
        info_json['all_product_buys'] = specifics['bought']
        info_json['all_product_clicks'] = specifics['clicked']
        info_json['all_product_views'] = specifics['viewed']

        json.dump({"index":{"_index":"items","_type":"_doc","_id":product}},info_file)
        info_file.write('\n')
        json.dump(info_json,info_file)
        info_file.write('\n')


    info_file.close()

def get_specifics_for_pharm24():
    path_pharm24_users_info = '../pharm24/data/users_info.json'
    overall = dict()
    with open(path_pharm24_users_info,'r') as users_file:
        users = json.load(users_file)
        for user in users:
            for product in users[user]:
                if(product in overall):
                    overall[product] += users[user][product]["times_viewed"]
                else:
                    overall[product] = users[user][product]["times_viewed"]
    print "Got specifics"
    return overall

def show_prods_user_watched(id):
    path_to_pharm24_products_info = '../pharm24/data/products_info.json'
    path_to_pharm24_users_info = '../pharm24/data/users_info.json'
    overall = list()
    with open(path_to_pharm24_products_info,'r') as prods_file:  
        products = json.load(prods_file)      
        with open(path_to_pharm24_users_info,'r') as users_file:
            users = json.load(users_file)
            for product in users[id]:
                overall.append(products[product]['name'])

    test_file = io.open('test','w',encoding = 'utf8')
    for product in overall:
        test_file.write(product)
        test_file.write('\n'.decode('utf-8'))
            

def get_pharm24_item_json_for_elastic():
    path_to_elastic_json = "../pharm24/data/products_elastic_1.json"
    path_to_pharm24_products_info = '../pharm24/data/products_info.json'
    elastic_dict = dict()
    elastic_file = io.open(path_to_elastic_json,"w",encoding = "utf8")
    overall = get_specifics_for_pharm24()
    with open(path_to_pharm24_products_info,'r') as prods_file:
        prods = json.load(prods_file)

    print "Got products. Starting iterating..."
    
    for prod in prods:        
        elastic_dict['id'] = int(prod)
        elastic_dict['name'] = prods[prod]['name']
        elastic_dict['categories'] = list(prods[prod]['categories'])
        if(prod in overall):
            elastic_dict['all_interactions'] = overall[prod]
        else:
            elastic_dict['all_interactions'] = 0
        elastic_dict['viewed_by_current_user'] = 0
        elastic_dict['viewed_multiple_times_by_current_user'] = 0
        elastic_dict['recommended_for_current_user'] = 0
        to_write = json.dumps({"index":{"_index":"p24_items","_type":"_doc","_id":prod}},ensure_ascii=False)
        elastic_file.write(to_write)
        elastic_file.write('\n'.decode('utf-8'))
        to_write = json.dumps(elastic_dict,ensure_ascii=False)
        elastic_file.write(to_write)
        elastic_file.write('\n'.decode('utf-8'))

    elastic_file.close()

def create_pharm24_products_info(): 
    path_to_pharm24_products_csv ='../pharm24/data/products.csv'
    path_to_pharm24_products_info = '../pharm24/data/products_info1.json'
    prods_info = dict()

    with open(path_to_pharm24_products_csv,'r') as prods_views:
        print "Started iterating through product-views"     

        for line in prods_views:            
            info = line.split(';')

            product_id = info[0].replace('"','')
            name = info[1].replace('"','')            
            categories = info[2].replace('"','').replace('\n','').split(',')

            if not(product_id in prods_info):
                prods_info[product_id] = dict()    
            prods_info[product_id]["name"] = name         
            prods_info[product_id]['categories'] = categories
        
    with io.open(path_to_pharm24_products_info,'w',encoding='utf8') as prods_file:        
        my_json_str = json.dumps(prods_info, indent =4, sort_keys = True ,ensure_ascii=False)
        if isinstance(my_json_str, str):
            my_json_str = my_json_str.decode("utf-8")

        prods_file.write(my_json_str)
    

    print "Done!"

# get_pharm24_item_json_for_elastic()

# create_pharm24_products_info()
# get_item_json_for_elastic()
