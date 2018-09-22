import json
import os

def link_users_queries():
    path_to_train_queries = '../train-queries.csv'
    user_query = dict()
    with open(path_to_train_queries,'r') as queries:
        queries.readline()
        for line in queries:
            info = line.split(';')
            queryId = info[0]
            userId = info[2]
            eventdate = info[5]
            user_query[queryId] = dict()
            user_query[queryId]["userId"] = userId
            user_query[queryId]["eventdate"] = eventdate            

    return user_query

def make_user_profile():
    path_to_train_purchases = '../train-purchases.csv'
    path_to_train_clicks = '../train-clicks.csv'
    path_to_train_item_views = '../train-item-views.csv'
    path_to_write_json = 'users_products_w_time/users_info_1.json'

    user_profiles = dict()
    user_query = link_users_queries()
    print "Linked each query to the respective user..."
    print "Opening purchases..."
    with open(path_to_train_purchases,'r') as purchases:
        purchases.readline()
        for line in purchases:            
            info = line.split(';')
            userId = info[1]
            item = info[-1].strip()
            eventdate = info[3]
            timeframe = info[2]
            if(userId != "NA"):
                if (userId not in user_profiles):
                    user_profiles[userId] = dict()
                    user_profiles[userId]['bought'] = list()
                    user_profiles[userId]['viewed'] = list()
                    user_profiles[userId]['clicked'] = list()
                user_profiles[userId]['bought'].append([item,eventdate,timeframe])
    print "Opening clicks..."
    with open(path_to_train_clicks,'r') as clicks:
        clicks.readline()
        for line in clicks:            
            info = line.split(';')
            queryId = info[0]
            itemId = info[-1].strip()
            timeframe = info[1]            
            try:
                userId = user_query[queryId]["userId"]
                eventdate = user_query[queryId]["eventdate"]
                if(userId != "NA"):
                    if (userId not in user_profiles):
                        user_profiles[userId] = dict()
                        user_profiles[userId]['bought'] = list()
                        user_profiles[userId]['viewed'] = list()
                        user_profiles[userId]['clicked'] = list()
                    user_profiles[userId]['clicked'].append([itemId,eventdate,timeframe])
            except Exception:
                print "query not in user_query dictionary"
                continue
    print "Opening views..."
    with open(path_to_train_item_views,'r') as views:
        views.readline()
        for line in views:            
            info = line.split(';')
            userId = info[1]
            itemId = info[2]
            eventdate = info[-1].strip()
            timeframe = info[-2]
            if(userId != "NA"):
                if(userId not in user_profiles):
                    user_profiles[userId] = dict()
                    user_profiles[userId]['bought'] = list()
                    user_profiles[userId]['viewed'] = list()
                    user_profiles[userId]['clicked'] = list()
                user_profiles[userId]['viewed'].append([itemId,eventdate,timeframe])
    print "Writing to json..."
    with open(path_to_write_json,'w') as jsonfile:
        json.dump(user_profiles,jsonfile,indent=4,sort_keys=True)

def get_recommendations():
    path_to_recommendations = 'recommendations/users_recommendations.json'
    with open(path_to_recommendations,'r') as recommendations_file:
        recomms = json.load(recommendations_file)
        print "Got recommendations for all users"
        return recomms

def make_user_profile_for_elastic():   
    path_to_elastic_users_info =  'items_info_1/users_info.json'
    path_to_users_for_elastic = 'items_info_1/users_for_elastic_1.json'
    with open(path_to_elastic_users_info,'r') as jsonfile:
        users_info = json.load(jsonfile)  
        recommendations = get_recommendations()
        print "Got all the users"      
        with open(path_to_users_for_elastic,'w') as elastic:   
            print "Creating the elastic json..."                     
            info_json = dict()
            for entry in users_info:                
                info_json["userId"] = int(entry)
                info_json['highly_bought'],info_json["bought"] = split_prods(users_info[entry]['bought'])
                info_json['highly_clicked'],info_json['clicked'] = split_prods(users_info[entry]['clicked'])
                info_json['highly_viewed'],info_json['viewed'] = split_prods(users_info[entry]['viewed'])
                
                info_json["recommended_products"] = recommendations[entry][0]
                info_json["recommendation_scores"] = recommendations[entry][1]

                json.dump({"index":{"_index":"user_profile","_type":"_doc","_id":entry}},elastic)
                elastic.write('\n')
                json.dump(info_json,elastic)
                elastic.write('\n')                


def split_prods(list_of_prods):
    popular = list()
    unique = list()
    for item in list_of_prods:
        if list_of_prods.count(item) > 1:
            if int(item) not in popular:
                popular.append(int(item))
        elif list_of_prods.count(item) == 1:
            unique.append(int(item))
    return popular,unique


def create_pharm24_user_profiles(): 

    path_to_pharm24_product_views = '../pharm24/data/products_views.csv'
    path_to_write_json = '../pharm24/data/users_info.json'
    users_info = dict()

    with open(path_to_pharm24_product_views,'r') as prods_views:
        print "Started iterating through product-views"     
        flag = True        
        for line in prods_views:
            info = line.split(',')
            product_id = info[0].replace('"','')
            timestamp = info[1].replace('"','')
            user_id = info[2].replace('"','').replace('\n','')

            if not(user_id in users_info):                
                users_info[user_id] = dict()
            if not(product_id in users_info[user_id]):
                users_info[user_id][product_id] = dict()
                users_info[user_id][product_id]["times_viewed"] = 0
                users_info[user_id][product_id]["timestamps"] = list()
            users_info[user_id][product_id]["times_viewed"] += 1 
            if(users_info[user_id][product_id]["times_viewed"] > 2 and flag):
                print "More than 2"       
                flag = False
            users_info[user_id][product_id]["timestamps"].append(timestamp)
        
    with open(path_to_write_json,'w') as users_file:
        users_file = json.dump(users_info,users_file,indent=4,sort_keys=True)
    print "Done!"

def get_pharm24_recommendations():
    path_to_recommendations = 'recommendations/users_recommendations_pharm24.json'

    with open(path_to_recommendations,'r') as recommendations_file:
        recomms = json.load(recommendations_file)
        print "Got recommendations for all users"
        return recomms

def analyze_users_pharm24():
    path_to_users_info = '../pharm24/data/users_info.json'
    with open(path_to_users_info,'r') as usersfile:
        users = json.load(usersfile)
        print "Got users..."

    result = dict()

    for user in users:
        result[user] = dict()
        result[user]['viewed'] = list()
        result[user]['highly_viewed'] = list()
        for prod in users[user]:
            if(users[user][prod]['times_viewed'] > 1):
                result[user]['highly_viewed'].append(prod)
            else:
                result[user]['viewed'].append(prod)
    
    return result


def make_pharm24_user_profile_for_elastic():    
    path_to_pharm24_users_info = '../pharm24/data/users_info.json'
    with open(path_to_pharm24_users_info,'r') as jsonfile:
        users_info = json.load(jsonfile)  
        recommendations = get_pharm24_recommendations()
        analyzed = analyze_users_pharm24()
        prefix = '../pharm24/data/users_profile_elastic'
        suffix = '.json'
        filepath = prefix + suffix
        print "Ready to start iterating..."      
        elastic = open(filepath,'w')
        print "Creating the elastic json..."                     
        info_json = dict()
        for entry in users_info:                
            info_json["userId"] = long(entry)
            info_json['viewed'] = analyzed[entry]['viewed']
            info_json['highly_viewed'] = analyzed[entry]['highly_viewed']
            info_json["recommended_products"] = recommendations[entry][0]
            info_json["recommendation_scores"] = recommendations[entry][1]

            to_write = json.dumps({"index":{"_index":"p24_user_profile","_type":"_doc","_id":entry}})
            elastic.write(to_write)
            elastic.write('\n')
            to_write = json.dumps(info_json)
            elastic.write(to_write)
            elastic.write('\n')

            if(os.stat(filepath).st_size > 52428800):
                print "file exceeded size. Changing output file..."
                elastic.close()
                prefix = prefix + '1'
                filepath = prefix + suffix
                elastic = open(filepath,'w')  
        elastic.close()


# --------------- uncomment function you want to use--------------
# make_user_profile()
# make_user_profile_for_elastic()

# create_pharm24_user_profiles()
# make_pharm24_user_profile_for_elastic()