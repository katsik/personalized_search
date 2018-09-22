import json
import math
import product_collecting
import random

def create_submissions():
    prod_data = get_product_data()
    specifics = get_sum_of_specifics()
    users_profiles = get_users_profiles()
    with open('../test_queries.csv','r') as test_queries:
        with open('../submissions/submission_final.txt','w') as submission_file:
            print "Starting iterating through queries..."
            for line in test_queries:
                info = line.split(';')
                queryId = info[0]
                items = info[-1].split(',')
                userId = info[2]
                category_for_queryless = info[-2]
                if(category_for_queryless != '0' or category_for_queryless != ""):
                    is_queryless = True
                else:
                    is_queryless = False
                items_list = list()                
                for item in items:
                    item = item.strip('\n')                    
                    if item not in prod_data:
                        print ('item %s is not in prod_data | queryId = %s' %(item,queryId))
                        prod_data[item] = dict()
                    score = float(5*prod_data[item].get('bought',0))/specifics['bought'] + float(3*prod_data[item].get('clicked',0))/specifics['clicked'] + float(prod_data[item].get('viewed',0))/specifics['viewed'] + 1
                    score = personalize_score(users_profiles,userId,score,item)
                    if is_queryless:
                        if(prod_data[item]['category'] != category_for_queryless):
                            score = math.log(score,2)                                                
                    items_list.append([item,score])                
                items_list = sort_on_score(items_list)                                                            
                for index,item in enumerate(items_list):
                    items_list[index] = item[0]
                submission_file.write(queryId)
                submission_file.write(' ')
                for index,item in enumerate(items_list):
                    if index == len(items_list) - 1:
                        submission_file.write("%s" %item)
                    else:
                        submission_file.write("%s," %item)
                submission_file.write('\n')
        
 

def sort_on_score(prod_list):
    less = []
    equal = []
    greater = []

    if len(prod_list) > 1:
        pivot = prod_list[0]
        for x in prod_list:
            if x[1] < pivot[1]:
                less.append(x)
            elif x[1] > pivot[1]:
                greater.append(x)
            else:
                equal.append(x)
        return sort_on_score(greater)+equal+sort_on_score(less)
    else:
        return prod_list


def get_product_data():
    with open('items_info_1/products.json','r') as jsonfile:
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


def get_users_profiles():
    users_profiles = dict()
    with open('items_info_1/users_info.json','r') as jsonfile:
        users_profiles = json.load(jsonfile)
        print "Got users profiles..."
        return users_profiles 


def personalize_score(users_profiles, userId, score, item):
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
        
        w_tp = 3
        w_p = 1
        score = w_p*(float(5*buy_cnt)/buys + float(3*click_cnt)/clicks + float(view_cnt)/views) + w_tp*score
        # score = float(5*buy_cnt)/buys + float(3*click_cnt)/clicks + float(view_cnt)/views + score
        return score
            

def join_all_submissions():
    with open('../submissions/submissions_from_elastic_wp5_wtp3_wr1/submission.txt','w') as submission_file:        
        with open('../submissions/submissions_from_elastic_wp5_wtp3_wr1/submission_only_queryfull.txt','r') as queryfull_submissions:
            print "Starting iterating to queryfull submissions file..."
            for line in queryfull_submissions:
                submission_file.write(line)
            print "Starting iterating to queryless submissions file..."
        with open('../submissions/submissions_from_elastic_wp5_wtp3_wr1/submission_only_queryless.txt','r') as queryless_submissions:
            for line in queryless_submissions:
                submission_file.write(line)
    print "Done"

def elastic_submission():
    queryless_submissions = dict()
    queryfull_submissions = dict()
    with open('../submissions/elastic_submission.txt','w') as elastic_submission:
        with open('../submissions/submission_queryfull.txt','r') as queryfull:
            for line in queryfull:
                info = line.split(' ')                
                queryfull_submissions[info[0]] = info[1].split(',')
        print "Got queryfull submission..."
        with open('../submissions/submissions_from_elastic.txt','r') as queryless:
            for line in queryless:
                info = line.split(' ')
                queryless_submissions[info[0]] = info[1].split(',')
        print "Got queryless submission..."
        with open('../test_queries.csv','r') as test_queries:
            print "Iterating through test queries..."
            for line in test_queries:                
                queryId = line.split(';')[0]
                elastic_submission.write(queryId)
                elastic_submission.write(' ')
                if queryId in queryfull_submissions:
                    for index,item in enumerate(queryfull_submissions[queryId]):
                        if index == len(queryfull_submissions[queryId])-1:
                            elastic_submission.write('%s' %item)
                        else:
                            elastic_submission.write('%s,' %item)
                else:
                    try:
                        for index,item in enumerate(queryless_submissions[queryId]):
                            if index == len(queryless_submissions[queryId])-1:
                                elastic_submission.write('%s' %item)
                            else:
                                elastic_submission.write('%s,' %item)
                    except Exception, e:
                        print(str(e))                         

def make_a_random_submission():
    with open('../test_queries.csv','r') as test_queries:
        with open('../submissions/random_submission.txt','w') as random_submission:
            print "Starting iterating through queries..."
            for line in test_queries:
                info = line.split(';')
                queryId = info[0]
                items = info[-1].split(',')
                
                random_submission.write(queryId)
                random_submission.write(' ')
                random.shuffle(items)
                for index,item in enumerate(items):
                    item = item.strip()
                    if index == len(items) - 1:
                        random_submission.write("%s" %item)
                    else:
                        random_submission.write("%s," %item)
                random_submission.write('\n')


create_submissions()
# join_all_submissions()
# make_a_random_submission()
# elastic_submission()