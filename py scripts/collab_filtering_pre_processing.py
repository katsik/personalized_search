import json
from collections import Counter 


def make_the_user_item_array():
    
    users_with_prods = dict()
    to_be_in_tsv = dict()

    path_to_users_info_json = 'items_info_1/users_info.json'  

    with open(path_to_users_info_json,'r') as jsonfile:
        users_with_prods = json.load(jsonfile)
    
    for userId in users_with_prods:
        #read bought,viewed,clicked add weights for each item and assign userId /t itemId /t interaction_rate
        #then write for every user-item and change line.
        to_be_in_tsv[userId] = dict()
        if users_with_prods[userId]['bought']:
            c = Counter(users_with_prods[userId]['bought'])            
            for key in c:
                if key not in to_be_in_tsv[userId]:
                    to_be_in_tsv[userId][key] = 5*c[key]
                else:
                    to_be_in_tsv[userId][key] += 5*c[key]
        if users_with_prods[userId]['clicked']:
            c = Counter(users_with_prods[userId]['clicked'])                 
            for key in c:
                if key not in to_be_in_tsv[userId]:
                    to_be_in_tsv[userId][key] = 2*c[key]
                else:
                    to_be_in_tsv[userId][key] += 2*c[key]
        if users_with_prods[userId]['viewed']:
            c = Counter(users_with_prods[userId]['viewed'])
            for key in c:
                if key not in to_be_in_tsv[userId]:                    
                    to_be_in_tsv[userId][key] = c[key]
                else:
                    to_be_in_tsv[userId][key] += c[key]

    print "Done with the dict..."
    print "Opening file for writing..."
    with open('items_info_1/collaborative_filtering_preprocessing/users_items_data.csv','w') as tsv:
        for key in to_be_in_tsv:            
            for itemId in to_be_in_tsv[key]:
                tsv.write(key)
                tsv.write(';')
                tsv.write(itemId)
                tsv.write(';')
                tsv.write(str(to_be_in_tsv[key][itemId]))
                tsv.write('\n')    

            

def pharm24_collab_filtering_pre_processing():
    users_info = dict()

    with open('../pharm24/data/users_info.json','r') as jsonfile:
        users_info = json.load(jsonfile)
    
    print "Iterating through users & products."
    with open('../pharm24/data/users_info.csv','w') as csv_to_write:
        for userId in users_info:
            for itemId in users_info[userId]:
                csv_to_write.write(userId)
                csv_to_write.write(';')
                csv_to_write.write(itemId)
                csv_to_write.write(';')
                csv_to_write.write(str(users_info[userId][itemId]["times_viewed"]))
                csv_to_write.write('\n')

    print "Done!"


# ----------- uncomment the function you need to use------------------
# make_the_user_item_array()
# pharm24_collab_filtering_pre_processing()
    