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

raw_data = pd.read_table('items_info_1/collaborative_filtering_preprocessing/users_items_data.csv',sep = ';')

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


alpha_val = 40
conf_data = (data_sparse * alpha_val).astype('double')
print "Starting the Training..."
user_vecs, item_vecs = implicit_als_cg(conf_data, iterations=20, features=10)
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

    items = list()
    scores = list()


    # Loop through our recommended artist indicies and look up the actial artist name
    for idx in item_idx:
        items.append(item_lookup.item.loc[item_lookup.item_id == str(idx)].iloc[0])
        scores.append(float(recommend_vector[idx]))    

    # Create a new dataframe with recommended artist names and scores
    # recommendations = pd.DataFrame({'item': items, 'score': scores})
    recommendations = [items,scores]
    # recommendations = {"items":items, "scores":scores}

    return recommendations


def get_users_profiles():
    path_to_users_info = 'items_info_1/users_info.json'

    users_profiles = dict()
    with open(path_to_users_info,'r') as jsonfile:
        users_profiles = json.load(jsonfile)
        print "Got users profiles..."
        return users_profiles 



users_profiles = get_users_profiles()
path_to_recommendations = 'recommendations/users_recommendations.json'

with open(path_to_recommendations,'w') as recommendations:
    recomms = dict()
    size = len(users_profiles)
    cnt = 0
    for user in users_profiles:
        cnt += 1
        perc = float(cnt)/size
        print "%f %% Done" %(perc)
        userId = user_lookup.user_id.loc[user_lookup.user == int(user)].iloc[0]

        recommendations_for_user = recommend(userId, data_sparse, user_vecs, item_vecs, item_lookup, num_items = 30)

        recomms[user] = recommendations_for_user        

    print "Done with the user recommendations, writting dictionary to file..."
    recommendations.write(json.dumps(recomms,indent = 4,encoding = 'UTF-8',default = str))


print "Done! recommendations written to separate file"

