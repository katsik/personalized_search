import random

import pandas as pd

import numpy as np


import scipy.sparse as sparse
from scipy.sparse.linalg import spsolve
from sklearn.preprocessing import MinMaxScaler

import product_collecting
import json

#-------------------------
# LOAD AND PREP THE DATA
#-------------------------
path_to_users_items = 'items_info_1/collaborative_filtering_preprocessing/users_items_data.csv'

raw_data = pd.read_table(path_to_users_items,sep = ';')

raw_data.columns = ['user','item','interactions']

# Drop any rows with missing values

data = raw_data.dropna()

# Convert items into numerical IDs
data['user_id'] = data['user'].astype("category").cat.codes
data['item_id'] = data['item'].astype("category").cat.codes

# Create a lookup frame so we can get the items back in 
# the given form later.
item_lookup = data[['item_id', 'item']].drop_duplicates()
item_lookup['item_id'] = item_lookup.item_id.astype(str)

# Same procedure for the users
user_lookup = data[['user_id','user']].drop_duplicates()
user_lookup['user_id'] = user_lookup.user_id.astype(str)

data = data.drop(['user','item'],axis=1)

# Drop the rows that have 0 interactions
data = data.loc[data.interactions != 0]

users = list(np.sort(data.user_id.unique()))
items = list(np.sort(data.item_id.unique()))
interactions = list(data.interactions)

# Get the rows and the columns of our new matrix
rows = data.user_id.astype(int)
cols = data.item_id.astype(int)

# Construct a sparse matrix for our users and items containing number of interactions
data_sparse = sparse.csr_matrix((interactions,(rows,cols)),shape = (len(users),len(items)))

print "Data stored into sparse matrix! We're ready for the training..."

# Slower method. Simpler looking but takes a lot of time to train.
def implicit_als(sparse_data, alpha_val = 40, iterations = 10, lambda_val = 0.1, features = 10):
    """ Implementation of Alternating Least Squares with implicit data. We iteratively
    compute the user (x_u) and item (y_i) vectors using the following formulas:
 
    x_u = ((Y.T*Y + Y.T*(Cu - I) * Y) + lambda*I)^-1 * (X.T * Cu * p(u))
    y_i = ((X.T*X + X.T*(Ci - I) * X) + lambda*I)^-1 * (Y.T * Ci * p(i))
 
    Args:
        sparse_data (csr_matrix): Our sparse user-by-item matrix
 
        alpha_val (int): The rate in which we'll increase our confidence
        in a preference with more interactions.
 
        iterations (int): How many times we alternate between fixing and 
        updating our user and item vectors
 
        lambda_val (float): Regularization value
 
        features (int): How many latent features we want to compute.
    
    Returns:     
        X (csr_matrix): user vectors of size users-by-features
        
        Y (csr_matrix): item vectors of size items-by-features
    """

    # Calculate the confidence for each value in our sparse matrix

    confidence = sparse_data * alpha_val

    # Get the size of user rows and item columns 
    user_size, item_size = sparse_data.shape

    # We create the user vectors X of size users-by-features, the item
    # vectors Y of size items-by-features and randomly assign values
    X = sparse.csr_matrix(np.random.normal(size = (user_size,features)))
    Y = sparse.csr_matrix(np.random.normal(size = (item_size,features)))

    # Precompute I and lambda * I
    X_I = sparse.eye(user_size)
    Y_I = sparse.eye(item_size)

    I = sparse.eye(features)

    lI = lambda_val * I

    """
     Here we first precompute X-transpose-X and Y-transpose-Y. 
     We then have two inner loops where we first iterate over all users and update X 
     and then do the same for all items and update Y.     
    """
    # Start main loop. For each iteration we first compute X and then Y.
    for i in xrange(iterations):
        print 'itearation %d of %d ' %(i+1,iterations)

        # Precompute X-Transpose-X and Y-Transpose-Y
        yTy = Y.T.dot(Y)
        xTx = X.T.dot(X)

        # Loop through all users.
        for u in xrange(user_size):

            # Get the user row
            u_row = confidence[u,:].toarray()

            # Calculate the binary preference p(u)
            p_u = u_row.copy()
            p_u[p_u != 0] = 1.0

            # Calculate Cu and Cu - I
            CuI = sparse.diags(u_row,[0])
            Cu = CuI + Y_I

            # Put it all together and compute the final formula
            yT_CuI_y = Y.T.dot(CuI).dot(Y)
            yT_Cu_pu = Y.T.dot(Cu).dot(p_u.T)

            X[u] = spsolve(yTy + yT_Cu_pu + lI, yT_Cu_pu)

        for i in xrange(item_size):

            # Get the item column and transpose it
            i_row = confidence[:,i].T.toarray()

            # Calculate the binary preference p(i)
            p_i = i_row.copy()
            p_i[p_i != 0] = 1.0

            # Calculate Ci and Ci - I
            CiI = sparse.diags(i_row,[0])
            Ci = CiI + X_I  

            # Put it all together and compute the final formula
            xT_CiI_x = X.T.dot(CiI).dot(X)
            xT_Ci_pi = X.T.dot(Ci).dot(p_i.T)

            Y[i] = spsolve(xTx + xT_CiI_x + lI, xT_Ci_pi)
    
    return X, Y


def nonzeros(m, row):
    for index in xrange(m.indptr[row],m.indptr[row+1]):
        yield m.indices[index], m.data[index]

def implicit_als_cg(Cui, features = 20, iterations = 20, lambda_val = 0.1):
    user_size, item_size = Cui.shape

    X = np.random.rand(user_size, features) * 0.01
    Y = np.random.rand(item_size, features) * 0.01

    Cui, Ciu = Cui.tocsr(), Cui.T.tocsr()

    for iteration in xrange(iterations):
        print 'iteration %d of %d' % (iteration+1, iterations)
        least_squares_cg(Cui, X, Y, lambda_val)
        least_squares_cg(Ciu, Y, X, lambda_val)

    return sparse.csr_matrix(X),sparse.csr_matrix(Y)

def least_squares_cg(Cui, X, Y, lambda_val, cg_steps = 3):
    users,features = X.shape

    YtY = Y.T.dot(Y) + lambda_val*np.eye(features)

    for u in xrange(users):

        x = X[u]
        r = -YtY.dot(x)

        for i,confidence in nonzeros(Cui,u):
            r+= (confidence - (confidence - 1) * Y[i].dot(x)) * Y[i]

        p = r.copy()
        rsold = r.dot(r)

        for it in xrange(cg_steps):
            Ap = YtY.dot(p)
            for i, confidence in nonzeros(Cui, u):
                Ap += (confidence - 1) * Y[i].dot(p) * Y[i]

            alpha = rsold / p.dot(Ap)
            x += alpha * p
            r -= alpha * Ap

            rsnew = r.dot(r)
            p = r + (rsnew / rsold) * p
            rsold = rsnew

        X[u] = x

# Faster Implementation of Training.
alpha_val = 40
num_of_iterations = 20
num_of_features = 10
conf_data = (data_sparse * alpha_val).astype('double')
print "Starting the Training..."
user_vecs, item_vecs = implicit_als_cg(conf_data, iterations=num_of_iterations, features=num_of_features)
print "Done"


#--------------------------------
# GET USER RECOMMENDATIONS
#--------------------------------

def recommend(user_id, data_sparse, user_vecs, item_vecs, item_lookup, num_items=10):
    """Recommend items for a given user given a trained model
    
    Args:
        user_id (int): The id of the user we want to create recommendations for.
        
        data_sparse (csr_matrix): Our original training data.
        
        user_vecs (csr_matrix): The trained user x features vectors
        
        item_vecs (csr_matrix): The trained item x features vectors
        
        item_lookup (pandas.DataFrame): Used to map artist ids to artist names
        
        num_items (int): How many recommendations we want to return:
        
    Returns:
        recommendations (pandas.DataFrame): DataFrame with num_items artist names and scores
    
    """

    # Get all interactions of user
    user_interactions = data_sparse[user_id,:].toarray() 

    # We don't want to recommend items the user has consumed. So let's
    # set them all to 0 and the unknowns to 1.
    user_interactions = user_interactions.reshape(-1) + 1 #Reshape to turn into 1D array
    user_interactions[user_interactions > 1] = 0

    # This is where we calculate the recommendation by taking the 
    # dot-product of the user vectors with the item vectors.
    rec_vector = user_vecs[user_id,:].dot(item_vecs.T).toarray()

    # Let's scale our scores between 0 and 1 to make it all easier to interpret.
    min_max = MinMaxScaler()
    rec_vector_scaled = min_max.fit_transform(rec_vector.reshape(-1,1))[:,0]
    recommend_vector = user_interactions*rec_vector_scaled

    # Get all the item indices in order of recommendations (descending) and
    # select only the top "num_items" items. 
    item_idx = np.argsort(recommend_vector)[::-1][:num_items]

    items = []
    scores = []


    # Loop through our recommended artist indicies and look up the actial artist name
    for idx in item_idx:
        items.append(item_lookup.item.loc[item_lookup.item_id == str(idx)].iloc[0])
        scores.append(float(recommend_vector[idx]))    

    # Create a new dataframe with recommended artist names and scores
    # recommendations = pd.DataFrame({'item': items, 'score': scores})
    recommendations = [items,scores]

    return recommendations

# Quicksort alternation for sorting according to score

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


def get_users_profiles():
    users_profiles = dict()
    with open('items_info_1/users_info.json','r') as jsonfile:
        users_profiles = json.load(jsonfile)
        print "Got users profiles..."
        return users_profiles 


def get_item_data():
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
        
        score = 1.2*(float(5*buy_cnt)/buys + float(3*click_cnt)/clicks + float(view_cnt)/views) + score*0.8
        # score = (float(5*buy_cnt)/buys + float(3*click_cnt)/clicks + float(view_cnt)/views) * score
        return score

def re_rank_no_recommendation(items, prod_data, specifics, is_queryless = False, category_for_queryless = ""):
    items_list = list()                
    for item in items:
        item = item.strip('\n')                    
        if item not in prod_data:
            print ('item %s is not in prod_data | queryId = %s' %(item,queryId))
            prod_data[item] = dict()
        score = float(5*prod_data[item].get('bought',0))/specifics['bought'] + float(2*prod_data[item].get('clicked',0))/specifics['clicked'] + float(prod_data[item].get('viewed',0))/specifics['viewed']
        if is_queryless:
            if(prod_data[item]['category'] != category_for_queryless):
                score = math.log(score,2)           
        items_list.append([item,score])                
    items_list = sort_on_score(items_list)
    return items_list 

def re_rank(items, users_profiles, user_id ,prod_data ,recommendations, is_queryless = False, category_for_queryless = ""):    
    item_list = list()
    for item in items:
        item = item.strip('\n') 
        added = False
        score = float(5*prod_data[item].get('bought',0))/specifics['bought'] + float(2*prod_data[item].get('clicked',0))/specifics['clicked'] + float(prod_data[item].get('viewed',0))/specifics['viewed']
        score = personalize_score(users_profiles, user_id, score, item)        
        for i in xrange(len(recommendations[0])):
            if item == str(recommendations[0][i]):
                score += recommendations[1][i]
                if is_queryless:
                    if(prod_data[item]['category'] != category_for_queryless):
                        score = score / 2
                item_list.append([item,score])     
                added = True                            
        if(not added):
            item_list.append([item,score])
    item_list = sort_on_score(item_list)
    return item_list


def get_recommendations():
    with open('items_info_1/recommendation/users_recommendations.json','r') as recommendations_file:
        recomms = json.load(recommendations_file)
        prind "Got recommendations for all users"
        return recomms

#Re-rank the submission file...
item_data = get_item_data()
specifics = get_sum_of_specifics()
users_profiles = get_users_profiles()
recommendations_for_all = get_recommendations()
with open('../test_queries.csv','r') as test_queries:
    with open('../submissions/submission_collab_filtering.txt','w') as submission_file:
        print "Started iterating through queries."

        for line in test_queries:
            info = line.split(';')
            queryId = info[0]
            items_to_re_rank = info[-1].split(',')
            category_for_queryless = info[-2]
            if(category_for_queryless != '0' or category_for_queryless != ""):
                    is_queryless = True
                else:
                    is_queryless = False        
            # IF userId == NA or not in users_profiles do based on best sellers and trending.
            # else do the recommendation stuff.
            if(info[2] in users_profiles):                                                
                
                userId = user_lookup.user_id.loc[user_lookup.user == int(info[2])].iloc[0]

                # recommendations_for_user = recommend(userId, data_sparse, user_vecs, item_vecs, item_lookup, num_items = 30)
                recommendations_for_user = recommendations_for_all[info[2]]

                # for every item to be re-ranked.
                # ... check -> is it in the recommended?
                # ...   -> if yes assign a score else give 0.
                # items_re_ranked = re_rank(items_to_re_rank, item_data ,recommendations_for_user)                
                items_re_ranked = re_rank(items_to_re_rank, users_profiles, userId, item_data ,recommendations_for_user, is_queryless, category_for_queryless)                
            else:                
                userId = info[2]
                items_re_ranked = re_rank_no_recommendation(items_to_re_rank,item_data,specifics, is_queryless, category_for_queryless)

            submission_file.write(queryId)
            submission_file.write(' ')
            for index in xrange(len(items_re_ranked)):
                if index == (len(items_re_ranked) - 1):
                    submission_file.write("%s" %(str(items_re_ranked[index][0]).strip('\n')))
                else:
                    submission_file.write("%s," %(str(items_re_ranked[index][0]).strip('\n')))            
            submission_file.write('\n')
            
print 'Done'
# user_id = user_lookup.user_id.loc[user_lookup.user == 140986].iloc[0]
# print user_id

# # Let's generate and print our recommendations
# recommendations = recommend(user_id, data_sparse, user_vecs, item_vecs, item_lookup)
# print recommendations




