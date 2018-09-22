import product_collecting
import json
import datetime
import math


def sort_on_score(prod_list):
    less = []
    equal = []
    greater = []

    if len(prod_list) > 1:
        pivot = prod_list[0]
        for x in prod_list:
            if float(x[1]) < float(pivot[1]):
                less.append(x)
            elif float(x[1]) > float(pivot[1]):
                greater.append(x)
            else:
                equal.append(x)
        return sort_on_score(greater)+equal+sort_on_score(less)
    else:
        return prod_list

def jaccard_similarity(search_tokens, item_tokens):
    #search_tokens & item_tokens are sets.
    intersection = search_tokens.intersection(item_tokens)
    distinct = search_tokens.union(item_tokens)

    return float(len(intersection))/len(distinct)


def get_users_profiles():
    path_to_users_info = 'items_info_1/users_info.json'

    users_profiles = dict()
    with open(path_to_users_info,'r') as jsonfile:
        users_profiles = json.load(jsonfile)
        print "Got users profiles..."
        return users_profiles 

def get_users_profiles_w_time():
    path = 'users_products_w_time/users_info_1.json'

    users_profiles = dict()
    with open(path,'r') as jsonfile:
        users_profiles = json.load(jsonfile)
        print "Got users profiles..."
        return users_profiles

def get_item_data():
    path_to_products = 'items_info_1/products.json'

    with open(path_to_products,'r') as jsonfile:
        data = json.load(jsonfile)
        print "Got Product Data..."
        return data 
    
def get_sum_of_specifics():
    specifics = product_collecting.get_product_specifics()
    print "Got specifics..."
    specifics_sum = dict()
    for item in specifics:
        specifics_sum['bought'] = specifics_sum.get('bought',0) + specifics[item].get('bought',0)
        specifics_sum['viewed'] = specifics_sum.get('viewed',0) + specifics[item].get('viewed',0)
        specifics_sum['clicked'] = specifics_sum.get('clicked',0) + specifics[item].get('clicked',0)
    return specifics_sum

def sigmoid(x):
    return 1/(1 + math.exp(-x))

def personalize_score_w_timestamps(users_profiles, userId, score, item):
    Wp = 3

    if userId not in users_profiles:
        return score
    else:
        date = datetime.datetime.now()
        format_str = '%Y-%m-%d'
        buys = 0
        views = 0
        clicks = 0
        buy_cnt = 0
        view_cnt = 0
        click_cnt = 0
        buy_factor = 0
        click_factor = 0
        view_factor = 0
        
        # 1. find the latest date user had interaction for view, click, buy (just keep the min for every attr)
        # 2. pass those min difference into the sigmoid function and now you have buy, click and view factors set
        # 3. for every component of the score multiply with the respective factor (e.g. buy_weight * (buy/buy_cnt) * buy_factor)
        # 4. add the components to get the whole score.
        for attr in ['bought','clicked','viewed']:
            for product in users_profiles[userId][attr]:
                if attr == 'bought':
                    buys += 1
                    if product[0] == item:
                        buy_cnt+=1
                        bought_date = datetime.datetime.strptime(product[1],format_str)
                        diff = date - bought_date                        
                        buy_factor = max(buy_factor,sigmoid(float(diff.days)/100))
                elif attr == 'clicked':
                    clicks += 1
                    if product[0] == item:
                        click_cnt += 1
                        clicked_date = datetime.datetime.strptime(product[1],format_str)
                        diff = date - clicked_date                        
                        click_factor = max(click_factor,sigmoid(float(diff.days)/100))
                else:
                    views += 1
                    if product[0] == item:
                        view_cnt += 1
                        viewed_date = datetime.datetime.strptime(product[1],format_str)
                        diff = date - viewed_date                         
                        viewed_factor = max(viewed_factor,sigmoid(float(diff.days)/100))
                #making zeros to ones so that we can do the devision
                if buys == 0:
                    buys = 1
                if clicks == 0:
                    clicks = 1
                if views == 0:
                    views = 1

        buy_factor +=1
        click_factor +=1
        view_factor +=1

        score = Wp*((float(5*buy_cnt)/buys)*buy_factor + (float(3*click_cnt)/clicks)*click_factor + (float(view_cnt)/views)*view_factor) + score

        return score
                         

def personalize_score(users_profiles, userId, score, item):
    Wp = 3

    if userId not in users_profiles:
        return score
    else:
        buys = 0
        views = 0
        clicks = 0
        buy_cnt = 0
        view_cnt = 0
        click_cnt = 0
        for attr in ['bought','clicked','viewed']:
            for product in users_profiles[userId][attr]:
                if attr == 'bought':
                    buys += 1
                    if product == item:                    
                        buy_cnt += 1
                elif attr == 'clicked':
                    clicks += 1
                    if product == item:
                        click_cnt += 1
                else:
                    views += 1
                    if product == item:
                        view_cnt += 1
        #making zeros to ones so that we can do the devision
        if buys == 0:
            buys = 1
        if clicks == 0:
            clicks = 1
        if views == 0:
            views = 1
        
        score = Wp*(float(5*buy_cnt)/buys + float(3*click_cnt)/clicks + float(view_cnt)/views) + score
        
        return score

def re_rank_no_recommendation(items, prod_data, specifics, is_queryless = False, category_for_queryless = "",search_tokens=[]):
    items_list = list()                
    for item in items:
        item = item.strip('\n')                    
        if item not in prod_data:
            print ('item %s is not in prod_data | queryId = %s' %(item,queryId))
            prod_data[item] = dict()
        score = float(5*prod_data[item].get('bought',0))/specifics['bought'] + float(2*prod_data[item].get('clicked',0))/specifics['clicked'] + float(prod_data[item].get('viewed',0))/specifics['viewed']
        if is_queryless:
            if(prod_data[item]['category'] != category_for_queryless):
                if(score > 0):
                    score = math.log(score,2)
        else:
            product_tokens = prod_data[item].get('product_name_tokens',[])
            enhancement = jaccard_similarity(search_tokens,product_tokens)
            # score += enhancement * score
            score += enhancement
        items_list.append([item,score])                
    items_list = sort_on_score(items_list)
    return items_list 

def re_rank(items, users_profiles, user_id ,prod_data ,recommendations, is_queryless = False, category_for_queryless = "", search_tokens=[]):    
    Wtp = 1.5

    item_list = list()
    for item in items:
        item = item.strip('\n') 
        added = False
        score = float(5*prod_data[item].get('bought',0))/specifics['bought'] + float(2*prod_data[item].get('clicked',0))/specifics['clicked'] + float(prod_data[item].get('viewed',0))/specifics['viewed']
        score = Wtp*score
        
        # score = personalize_score(users_profiles, user_id, score, item) 
        score = personalize_score_w_timestamps(users_profiles,user_id,score,item)       
        for i in xrange(len(recommendations[0])):
            if item == str(recommendations[0][i]):
                score += float(0.75)*recommendations[1][i]                
                if is_queryless:
                    if(prod_data[item]['category'] != category_for_queryless):
                        if(score > 0):
                            score = math.log(score,2)
                else:
                    product_tokens = prod_data[item].get('product_name_tokens',[])
                    for product in product_tokens:
                        product = product.strip()
                    product_tokens = set(product_tokens)
                    enhancement = jaccard_similarity(search_tokens,product_tokens)
                    # score += enhancement * score
                    score += enhancement

                item_list.append([item,score])     
                added = True                            
        if(not added):
            item_list.append([item,score])
    item_list = sort_on_score(item_list)
    return item_list


def get_recommendations():
    path_to_user_recommendations = 'recommendations/users_recommendations.json'

    with open(path_to_user_recommendations,'r') as recommendations_file:
        recomms = json.load(recommendations_file)
        print "Got recommendations for all users"
        return recomms



#Re-rank the submission file...
item_data = get_item_data()
specifics = get_sum_of_specifics()
# users_profiles = get_users_profiles()
users_profiles = get_users_profiles_w_time()
recommendations_for_all = get_recommendations()
path_to_test_queries = '../test_queries.csv'
with open(path_to_test_queries,'r') as test_queries:
    with open('../submissions/submission.txt','w') as submission_file:
        print "Started iterating through queries."

        for line in test_queries:
            info = line.split(';')
            queryId = info[0]
            items_to_re_rank = info[-1].split(',')
            category_for_queryless = info[-2]
            search_tokens = list()
            if(category_for_queryless != '0' and category_for_queryless != ""):
                is_queryless = True
            else:
                is_queryless = False
                search_tokens = info[-3].split(',')   
                search_tokens = set(search_tokens)     
            # IF userId == NA or not in users_profiles do based on best sellers and popular.
            # else do the recommendation stuff.
            if(info[2] in users_profiles):                                                
                
                
                recommendations_for_user = recommendations_for_all[info[2]]

                # for every item to be re-ranked.
                # ... check -> is it in the recommended?
                # ...   -> if yes assign a score else give 0.
                           
                items_re_ranked = re_rank(items_to_re_rank, users_profiles, userId, item_data ,recommendations_for_user, is_queryless, category_for_queryless,search_tokens)                
            else:                
                userId = info[2]
                items_re_ranked = re_rank_no_recommendation(items_to_re_rank,item_data,specifics, is_queryless, category_for_queryless,search_tokens)

            submission_file.write(queryId)
            submission_file.write(' ')
            for index in xrange(len(items_re_ranked)):
                if index == (len(items_re_ranked) - 1):
                    submission_file.write("%s" %(str(items_re_ranked[index][0]).strip('\n')))
                else:
                    submission_file.write("%s," %(str(items_re_ranked[index][0]).strip('\n')))            
            submission_file.write('\n')
            
print 'Done'