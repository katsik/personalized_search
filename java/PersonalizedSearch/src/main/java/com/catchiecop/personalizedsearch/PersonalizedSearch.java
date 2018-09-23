package com.catchiecop.personalizedsearch;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.*;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.xcontent.ToXContent;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.index.query.functionscore.ScriptScoreFunctionBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

/**
 *
 * @author George Katsikopoulos
 */
public class PersonalizedSearch {
         
    
    RestHighLevelClient searchClient;
    SearchRequest request;
    SearchResponse searchResponse;
    
    public PersonalizedSearch(){
        final CredentialsProvider credsProvider = new BasicCredentialsProvider();
        credsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("elastic","elastic"));
        
        // Create a client to issue all the requests to the server

        RestHighLevelClient searchClient = new RestHighLevelClient(
                        RestClient.builder(
                                new HttpHost("localhost",9200),
                                new HttpHost("localhost",9201))
                                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                                    @Override
                                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder hacb) {
                                        return hacb.setDefaultCredentialsProvider(credsProvider);
                                    }
        }));
        
    }
    
    public PersonalizedSearch(String requestIndex, String term, String value){
        
        searchClient = new RestHighLevelClient(
                        RestClient.builder(
                                new HttpHost("localhost",9200),
                                new HttpHost("localhost",9201)));
        
        request = new SearchRequest(requestIndex);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.termQuery(term, value));
        
        request.source(searchSourceBuilder);
    }
    
    public PersonalizedSearch(String requestIndex, String term, Integer value){
        
        searchClient = new RestHighLevelClient(
                        RestClient.builder(
                                new HttpHost("localhost",9200),
                                new HttpHost("localhost",9201)));
        
        request = new SearchRequest(requestIndex);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.termQuery(term, value));
        //---           we can do some sorting if we want to        ---
        
        request.source(searchSourceBuilder);
    }
    
    public void performSearch() throws IOException{
        searchResponse = searchClient.search(request);
        
        SearchHits hits = searchResponse.getHits();
        SearchHit[] searchHits = hits.getHits();
        
        for(SearchHit hit : searchHits){
            System.out.println(hit.getSourceAsString());
        }
        
    }
    
    public UpdateRequest createUpdateRequest(String index, String type, String id, String field, String value) throws IOException{
        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index(index);
        updateRequest.type(type);
        updateRequest.id(id);
        updateRequest.doc(jsonBuilder()
                            .startObject()
                                .field(field,value)
                            .endObject());
        
        return updateRequest;
    }
    
    public UpdateRequest createUpdateRequest(String index, String type, String id , String field, Integer value) throws IOException{
        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index(index);
        updateRequest.type(type);
        updateRequest.id(id);
        updateRequest.doc(jsonBuilder()
                            .startObject()
                                .field(field,value)
                            .endObject());
        
        return updateRequest;
    }
    
    public BulkRequest doMultipleUpdates(List<Integer> viewed, List<Integer> bought, List<Integer> highlyBought, List<Integer> highlyViewed, Integer[] values) throws IOException{
        if(values.length==4){
            BulkRequest request = new BulkRequest();
            makeTheUpdate(viewed,"viewed_by_current_user",request,values[0]);
            makeTheUpdate(bought,"bought_by_current_user",request,values[1]);        
            makeTheUpdate(highlyViewed,"viewed_multiple_times_by_current_user",request,values[2]);        
            makeTheUpdate(highlyBought,"bought_multiple_times_by_current_user",request,values[3]);      
            return request;
        }else{
            throw new UnsupportedOperationException("Unsupported Operation for values more or less than 4 current array has lenght of "+ values.length);
        }      
    }
    
    public BulkRequest doMultipleUpdates(List<Integer> viewed, List<Integer> bought, List<Integer> highlyBought, List<Integer> highlyViewed, Integer[] values
            , List<String> recommended, List<Double> recommendationScores) throws IOException{
        
        if(values.length==4 && recommendationScores.size() == recommended.size()){
            BulkRequest request = new BulkRequest();
            makeTheUpdate(viewed,"viewed_by_current_user",request,values[0]);
            makeTheUpdate(bought,"bought_by_current_user",request,values[1]);        
            makeTheUpdate(highlyViewed,"viewed_multiple_times_by_current_user",request,values[2]);        
            makeTheUpdate(highlyBought,"bought_multiple_times_by_current_user",request,values[3]); 
            makeTheUpdate(recommended,"recommended_for_current_user",request,recommendationScores);
            return request;
        }else{
            throw new UnsupportedOperationException("Unsupported Operation for values not equal to 4 currently there are "+ values.length
                    + " or recommendations are not equal in number with scores curently "+ recommendationScores.size() + " != " + recommended.size());
        }
    }
    
    public void makeTheUpdate(List<Integer> v,String field ,BulkRequest request, Integer value) throws IOException{
        if(v.size()>0){
            for(Integer prod : v){
                UpdateRequest updateRqst = new UpdateRequest("items","_doc",prod.toString())
                                                        .doc(jsonBuilder()
                                                            .startObject()
                                                            .field(field,value)
                                                            .endObject());
                request.add(updateRqst);
            }
        }
    }
    
    public void makeTheUpdate(List<String> v, String field, BulkRequest request, List<Double> values) throws IOException{
        if(v.size() == values.size()){
            for(int i=0; i<v.size(); i++){
                String prod = v.get(i);
                Double value = values.get(i);
                
                UpdateRequest updateReq = new UpdateRequest("items","_doc",prod)
                                                        .doc(jsonBuilder()
                                                            .startObject()
                                                            .field(field,value)
                                                            .endObject());
                request.add(updateReq);
            }
        }
    }
    
    public void makeTheUpdate(List<String> v, String field, BulkRequest request, Double value) throws IOException{
        if(v.size()>0){
            for(int i=0; i<v.size(); i++){
                String prod = v.get(i);                
                
                UpdateRequest updateReq = new UpdateRequest("items","_doc",prod)
                                                        .doc(jsonBuilder()
                                                            .startObject()
                                                            .field(field,value)
                                                            .endObject());
                request.add(updateReq);
            }
        }
    }
    
    public void makeThePharm24Update(List<String> v,String field ,BulkRequest request, Integer value) throws IOException{
        if(v.size()>0){
            for(String prod : v){
                UpdateRequest updateRqst = new UpdateRequest("p24_items","_doc",prod.toString())
                                                        .doc(jsonBuilder()
                                                            .startObject()
                                                            .field(field,value)
                                                            .endObject());
                request.add(updateRqst);
            }
        }
    }
       
    
    public void makeThePharm24Update(List<String> v, String field, BulkRequest request, List<Double> values) throws IOException{
        if(v.size() == values.size()){
            for(int i=0; i<v.size(); i++){
                String prod = v.get(i);
                Double value = values.get(i);
                
                UpdateRequest updateReq = new UpdateRequest("p24_items","_doc",prod)
                                                        .doc(jsonBuilder()
                                                            .startObject()
                                                            .field(field,value)
                                                            .endObject());
                request.add(updateReq);
            }
        }
    }
    
    public BulkRequest doMultiplePharm24Updates(List<String> viewed, List<String> highlyViewed, Integer[] values
            , List<String> recommended, List<Double> recommendationScores) throws IOException{
        
        if(values.length==2 && recommendationScores.size() == recommended.size()){
            BulkRequest request = new BulkRequest();
            makeThePharm24Update(viewed,"viewed_by_current_user",request,values[0]);          
            makeThePharm24Update(highlyViewed,"viewed_multiple_times_by_current_user",request,values[1]);                    
            makeThePharm24Update(recommended,"recommended_for_current_user",request,recommendationScores);
            return request;
        }else{
            throw new UnsupportedOperationException("Unsupported Operation for values not equal to 4 currently there are "+ values.length
                    + " or recommendations are not equal in number with scores curently "+ recommendationScores.size() + " != " + recommended.size());
        }
    }
    
    public List<List<Integer>> getInfoForUser(RestHighLevelClient client, String userId) throws IOException{
        List<List<Integer>> toBeReturned = new ArrayList<List<Integer>>();
        
        SearchRequest req = new SearchRequest("user_profile");
        SearchSourceBuilder mBuilder = new SearchSourceBuilder();
        mBuilder.query(QueryBuilders.termQuery("userId", Integer.parseInt(userId)));
        req.source(mBuilder);        
        
        SearchResponse resp = client.search(req);
        
        SearchHits hits = resp.getHits();
        if(hits.getHits().length == 1){
            Map<String,Object> sourceAsMap = hits.getHits()[0].getSourceAsMap();
            toBeReturned.add((List<Integer>)sourceAsMap.get("viewed"));
            toBeReturned.add((List<Integer>)sourceAsMap.get("highly_viewed"));
            toBeReturned.add((List<Integer>)sourceAsMap.get("bought"));
            toBeReturned.add((List<Integer>)sourceAsMap.get("highly_bought"));
            return toBeReturned;
        }else{
            System.out.println("User resutls more than one! \n Exiting...");
            return toBeReturned;
        }
    }
    
    public List<String> performPersonalizedQuerylessSearch(RestHighLevelClient client, Integer userID, String category, String[] items) throws IOException{
        
        List<Integer> boughtItems, viewedItems, highlyBought, highlyViewed;
        List<Double> recommendationScores;
        List<String> reRanked,recommendedProducts;
        String currentItem;
        BulkRequest bulkRequest;
        BulkResponse bulkResponse;
        
        
        
        /**
         * First we'll search about the user and get all the products he's visited/bought
        */
        
        SearchRequest req = new SearchRequest("user_profile");
        SearchSourceBuilder mBuilder = new SearchSourceBuilder();
        mBuilder.query(QueryBuilders.termQuery("userId",userID));
        req.source(mBuilder);
        
        SearchResponse resp = client.search(req);
        
        SearchHits hits = resp.getHits();
        if(hits.getHits().length == 1){
            Map<String,Object> sourceAsMap = hits.getHits()[0].getSourceAsMap();            
            viewedItems = (List<Integer>)sourceAsMap.get("viewed");
            highlyViewed = (List<Integer>)sourceAsMap.get("highly_viewed");
            boughtItems = (List<Integer>)sourceAsMap.get("bought");
            highlyBought = (List<Integer>)sourceAsMap.get("highly_bought");
            recommendationScores = (List<Double>)sourceAsMap.get("recommendation_scores");
            recommendedProducts = (List<String>)sourceAsMap.get("recommended_products");
            
            // For every product in the lists we put 1 in the respective field of the product profile

            Integer[] values = {1,1,1,1};
            if(viewedItems.size()>0 || highlyViewed.size()>0 || boughtItems.size()>0 || highlyBought.size()>0){
                bulkRequest = doMultipleUpdates(viewedItems,boughtItems,highlyBought,highlyViewed,values,recommendedProducts,recommendationScores);
                        
                bulkResponse = client.bulk(bulkRequest);
                if(bulkResponse.hasFailures()){
                    for (BulkItemResponse bulkItemResponse : bulkResponse) {
                        if (bulkItemResponse.isFailed()) { 
                            BulkItemResponse.Failure failure = bulkItemResponse.getFailure(); 
                            System.out.println(failure.getMessage());
                        }
                    }
                }
            }
            
            
            SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
            TermQueryBuilder termQuery = QueryBuilders.termQuery("category", category);

            // We add the log to have the scored not reach too big of values

            ScriptScoreFunctionBuilder scoreFunction = ScoreFunctionBuilders
                                                    .scriptFunction("Math.log("
                                                            + "2 + 1.5*(doc['times_viewed'].value  + 2*doc['times_clicked'].value + "
                                                            + "3*doc['times_bought'].value) + 3*(doc['viewed_by_current_user'].value + "
                                                            + "(5*doc['bought_by_current_user'].value) + (3*doc['clicked_by_current_user'].value)"
                                                            + "(doc['viewed_multiple_times_by_current_user'].value) + "
                                                            + "(5*doc['bought_multiple_times_by_current_user'].value)) + "
                                                            + "(0.75 * doc['recommended_for_current_user'].value))");
                        
            searchBuilder.query(new FunctionScoreQueryBuilder(termQuery,scoreFunction).boostMode(CombineFunction.MULTIPLY));
            searchBuilder.size(200);
            searchBuilder.trackScores(true);
            
            SearchRequest searchRequest = new SearchRequest("items");

            searchRequest.source(searchBuilder);




            SearchResponse response = client.search(searchRequest);

            hits = response.getHits();
            SearchHit[] searchHits = hits.getHits();
            /**
             * We have our hits. We need to see which ones are in the list we need to reorder. We write them in a submission file.If a 
             * a product is in the list for reorder but not in the returned from elasticsearch list goes to the end of the reordered list.
             * 
             * we use hit.getSourceAsString() to transform into json for easier manipulation  
             * e.g. {"itemId":36602,"category":"962","times_bought":1,"viewed_multiple_times_by_current_user":0,"viewed_by_current_user":0,"times_viewed":276,"pricelog2":"7","times_clicked":268,"bought_by_current_user":1,"bought_multiple_times_by_current_user":0}
             * 
             * if prods from hits end and the prods to reorder are more we just fill in the list with the remaining products for reorder.
             */
            
            reRanked = new ArrayList<String>();
            for(SearchHit hit : searchHits){

                  JSONObject jsonObj = new JSONObject(hit.getSourceAsMap());                  
                  currentItem = String.valueOf(jsonObj.get("itemId"));
                  if(Arrays.asList(items).contains(currentItem)){
                      reRanked.add(currentItem);
                  }
            }
            
            /**
             * adding the remaining products to the reRanked list
             */
            
            int i = 0;
            while((reRanked.size()!=items.length)&&(i<items.length)){
                if(!reRanked.contains(items[i])){
                    reRanked.add(items[i]);
                }
                i++;
            }
            recommendationScores.clear();
            for(i=0;i<values.length;i++){values[i]=0;}
            for(i=0;i<recommendedProducts.size();i++){recommendationScores.add(i, (Double)0.0);}
            if(viewedItems.size()>0 || highlyViewed.size()>0 || boughtItems.size()>0 || highlyBought.size()>0){
                bulkRequest = doMultipleUpdates(viewedItems,highlyViewed,boughtItems,highlyBought,values,recommendedProducts,recommendationScores);
            
                bulkResponse = client.bulk(bulkRequest);
                if(bulkResponse.hasFailures()){
                    for (BulkItemResponse bulkItemResponse : bulkResponse) {
                        if (bulkItemResponse.isFailed()) { 
                            BulkItemResponse.Failure failure = bulkItemResponse.getFailure(); 
                            System.out.println(failure.getMessage());
                        }
                    }
                } 
            }                       
            
            return reRanked;
            
        }else if(hits.getHits().length==0){
            return performPersonalizedQuerylessSearch(client,category,items);
        }else{
            throw new UnsupportedOperationException("Got more than one hits for one user!");
        }        
        
    }    
    
    public List<String> performPersonalizedQuerylessSearch(RestHighLevelClient client, String category, String[] items) throws IOException{
        //"Personalized" Anonymus search
        
        List<String> reRanked = new ArrayList<String>();
        String currentItem;
        
        SearchRequest request = new SearchRequest("items");
        SearchSourceBuilder mBuilder = new SearchSourceBuilder();
        
        String functionScript = "Math.log(2 + doc['times_viewed'].value  + 3*doc['times_clicked'].value + 5*doc['times_bought'].value)";
        
        TermQueryBuilder termQuery = QueryBuilders.termQuery("category", category);
        ScriptScoreFunctionBuilder scoreFunction = ScoreFunctionBuilders
                                                .scriptFunction(functionScript);
        
        mBuilder.query(new FunctionScoreQueryBuilder(termQuery,scoreFunction));
        mBuilder.size(200);
        mBuilder.trackScores(true);
        
        request.source(mBuilder);
        
        SearchResponse response = client.search(request);
        
        SearchHit[] hits = response.getHits().getHits();
        for(SearchHit hit : hits){
            JSONObject jsonObj = new JSONObject(hit.getSourceAsMap());                  
            currentItem = String.valueOf(jsonObj.get("itemId"));
            if(Arrays.asList(items).contains(currentItem)){
                reRanked.add(currentItem);
            }
        }
        
        int i = 0;
        while((reRanked.size()!=items.length)&&(i<items.length)){
            if(!reRanked.contains(items[i])){
                reRanked.add(items[i]);
            }
            i++;
        }
        

        return reRanked;
        
    }
    
    public void performQuerylessSearchWithPersonalization(RestHighLevelClient client) throws IOException{
        /**
         * First we open and parse the CSV file with the queries
         */
        
        // add path for the csv file of the queryless searches
        String csvFile = "ADD PATH";
        String pathForSubmissionFile = "ADD PATH";
        BufferedReader br = null;
        PrintWriter writer = null;
        String line = "";
        String csvSpliter = ";";       
        List<String> reRankedProds;
        Long count= 0L;
        
        try{
            System.out.println("Iterating through test_only_queries...");
            br = new BufferedReader(new FileReader(csvFile));
            writer = new PrintWriter(pathForSubmissionFile,"UTF-8");
            while((line = br.readLine())!=null){
                count++;
                String[] info = line.split(csvSpliter);
                String queryId = info[0];
                String userId = info[2];
                String[] items = info[info.length - 1].split(",");
                String category = info[info.length - 2];
                             
                
                if(!userId.trim().equals("NA")){
                    reRankedProds = performPersonalizedQuerylessSearch(client,Integer.parseInt(userId),category,items);
                    writer.println(queryId + " " + reRankedProds.toString().replace("[", "").replace("]", "").replace(" ", ""));
                }else{
                    reRankedProds = performPersonalizedQuerylessSearch(client,category,items);
                    writer.println(queryId + " " + reRankedProds.toString().replace("[", "").replace("]", "").replace(" ",""));
                }
                
            }
            writer.close();
        } catch(FileNotFoundException e){
            e.printStackTrace();
        } catch (IOException e){
            e.printStackTrace();
        } finally{
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        client.close();
    }
    
    public String getQuery(String[] tokens){
        
        String query = "";
        
        for(int i=0; i<tokens.length; i++){
            query += tokens[i];
            if(i != tokens.length - 1){
                query+= " ";
            }
        }
        
        return query;
    }
    
    public List<List<String>> performPharm24PersonalizedQueryfullSearch(RestHighLevelClient client, Long userID, String query) throws IOException{
        
        List<String> viewedItems, highlyViewed;
        List<Double> recommendationScores;
        List<String> reRanked,recommendedProducts,original;
        String currentItem;
        BulkRequest bulkRequest;
        BulkResponse bulkResponse;
        List<List<String>> result = new ArrayList<List<String>>();
        
        
        /**
         * First we'll search about the user and get all the products he's visited/bought
        */
        
        SearchRequest req = new SearchRequest("p24_user_profile");
        SearchSourceBuilder mBuilder = new SearchSourceBuilder();
        mBuilder.query(QueryBuilders.termQuery("userId",userID));
        req.source(mBuilder);
        
        SearchResponse resp = client.search(req);
        
        SearchHits hits = resp.getHits();
        if(hits.getHits().length == 1){
            Map<String,Object> sourceAsMap = hits.getHits()[0].getSourceAsMap();            
            viewedItems = (List<String>)sourceAsMap.get("viewed");
            highlyViewed = (List<String>)sourceAsMap.get("highly_viewed");           
            recommendationScores = (List<Double>)sourceAsMap.get("recommendation_scores");
            recommendedProducts = (List<String>)sourceAsMap.get("recommended_products");
            
            Integer[] values = {1,1};
            if(viewedItems.size()>0 || highlyViewed.size()>0){
                bulkRequest = doMultiplePharm24Updates(viewedItems,highlyViewed,values,recommendedProducts,recommendationScores);
                        
                bulkResponse = client.bulk(bulkRequest);
                if(bulkResponse.hasFailures()){
                    for (BulkItemResponse bulkItemResponse : bulkResponse) {
                        if (bulkItemResponse.isFailed()) { 
                            BulkItemResponse.Failure failure = bulkItemResponse.getFailure(); 
                            System.out.println(failure.getMessage());
                        }
                    }
                }
            }            

            
            req = new SearchRequest("p24_items");
            mBuilder = new SearchSourceBuilder();
            mBuilder.query(QueryBuilders.matchQuery("name",query));
            mBuilder.size(20);
            req.source(mBuilder);

            resp = client.search(req);

            hits = resp.getHits();
                        
            SearchHit[] searchHitsNonPersonalized = hits.getHits();            
            
            
            SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
            MatchQueryBuilder matchQuery = QueryBuilders.matchQuery("name", query);

            ScriptScoreFunctionBuilder scoreFunction = ScoreFunctionBuilders
                                                    .scriptFunction("Math.log(2 + doc['all_interactions'].value + "
                                                            + "(2*doc['viewed_by_current_user'].value) + "
                                                            + "(5*doc['viewed_multiple_times_by_current_user'].value) + "
                                                            + "(0.75*doc['recommended_for_current_user'].value))");
                        
            searchBuilder.query(new FunctionScoreQueryBuilder(matchQuery,scoreFunction).boostMode(CombineFunction.MULTIPLY));
            searchBuilder.size(20);
            searchBuilder.trackScores(true);
            
            SearchRequest searchRequest = new SearchRequest("p24_items");

            searchRequest.source(searchBuilder);




            SearchResponse response = client.search(searchRequest);

            hits = response.getHits();
            SearchHit[] searchHits = hits.getHits();
            
            
            original = new ArrayList<String>();
            reRanked = new ArrayList<String>();
            for(SearchHit hit : searchHits){
                  JSONObject jsonObj = new JSONObject(hit.getSourceAsMap());                  
                  currentItem = String.valueOf(jsonObj.get("name"));
                  reRanked.add(currentItem);                
            }
            
            for(SearchHit hit : searchHitsNonPersonalized){
                  JSONObject jsonObj = new JSONObject(hit.getSourceAsMap());                  
                  currentItem = String.valueOf(jsonObj.get("name"));
                  original.add(currentItem);                
            }
                        
            
            recommendationScores.clear();
            for(int i=0;i<values.length;i++){values[i]=0;}
            for(int i=0;i<recommendedProducts.size();i++){recommendationScores.add(i, (Double)0.0);}
            if(viewedItems.size()>0 || highlyViewed.size()>0){
                bulkRequest = doMultiplePharm24Updates(viewedItems,highlyViewed,values,recommendedProducts,recommendationScores);
            
                bulkResponse = client.bulk(bulkRequest);
                if(bulkResponse.hasFailures()){
                    for (BulkItemResponse bulkItemResponse : bulkResponse) {
                        if (bulkItemResponse.isFailed()) { 
                            BulkItemResponse.Failure failure = bulkItemResponse.getFailure(); 
                            System.out.println(failure.getMessage());
                        }
                    }
                } 
            }
            
            result.add(original);
            result.add(reRanked);
            
            
            return result;
            
        }else{
            throw new UnsupportedOperationException("Got more than one hits for one user!");
        }        
        
    }
    
    public List<List<String>> performPharm24PersonalizedQuerylessSearch(RestHighLevelClient client, Long userID, String query) throws IOException{
        
        List<String> viewedItems, highlyViewed;
        List<Double> recommendationScores;
        List<String> reRanked,recommendedProducts,original;
        String currentItem;
        BulkRequest bulkRequest;
        BulkResponse bulkResponse;
        List<List<String>> result = new ArrayList<List<String>>();        
        
        
        /**
         * First we'll search about the user and get all the products he's visited/bought
        */
        
        SearchRequest req = new SearchRequest("p24_user_profile");
        SearchSourceBuilder mBuilder = new SearchSourceBuilder();
        mBuilder.query(QueryBuilders.termQuery("userId",userID));
        req.source(mBuilder);
        
        SearchResponse resp = client.search(req);
        
        SearchHits hits = resp.getHits();
        if(hits.getHits().length == 1){
            Map<String,Object> sourceAsMap = hits.getHits()[0].getSourceAsMap();            
            viewedItems = (List<String>)sourceAsMap.get("viewed");
            highlyViewed = (List<String>)sourceAsMap.get("highly_viewed");           
            recommendationScores = (List<Double>)sourceAsMap.get("recommendation_scores");
            recommendedProducts = (List<String>)sourceAsMap.get("recommended_products");
            
            Integer[] values = {1,1};
            if(viewedItems.size()>0 || highlyViewed.size()>0){
                bulkRequest = doMultiplePharm24Updates(viewedItems,highlyViewed,values,recommendedProducts,recommendationScores);
                        
                bulkResponse = client.bulk(bulkRequest);
                if(bulkResponse.hasFailures()){
                    for (BulkItemResponse bulkItemResponse : bulkResponse) {
                        if (bulkItemResponse.isFailed()) { 
                            BulkItemResponse.Failure failure = bulkItemResponse.getFailure(); 
                            System.out.println(failure.getMessage());
                        }
                    }
                }
            }
            

            
            req = new SearchRequest("p24_items");
            mBuilder = new SearchSourceBuilder();
            mBuilder.query(QueryBuilders.matchQuery("categories",query));
            mBuilder.size(20);
            req.source(mBuilder);

            resp = client.search(req);

            hits = resp.getHits();
                        
            SearchHit[] searchHitsNonPersonalized = hits.getHits();            
            
            
            SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
            MatchQueryBuilder matchQuery = QueryBuilders.matchQuery("categories", query);

            ScriptScoreFunctionBuilder scoreFunction = ScoreFunctionBuilders
                                                    .scriptFunction("Math.log(2 + doc['all_interactions'].value + "
                                                            + "(2*doc['viewed_by_current_user'].value) + "
                                                            + "(5*doc['viewed_multiple_times_by_current_user'].value) + "
                                                            + "(0.75*doc['recommended_for_current_user'].value))");
                        
            searchBuilder.query(new FunctionScoreQueryBuilder(matchQuery,scoreFunction).boostMode(CombineFunction.MULTIPLY));
            searchBuilder.size(20);
            searchBuilder.trackScores(true);
            
            SearchRequest searchRequest = new SearchRequest("p24_items");

            searchRequest.source(searchBuilder);


            SearchResponse response = client.search(searchRequest);

            hits = response.getHits();
            SearchHit[] searchHits = hits.getHits();
            
            
            original = new ArrayList<String>();
            reRanked = new ArrayList<String>();
            for(SearchHit hit : searchHits){

                  JSONObject jsonObj = new JSONObject(hit.getSourceAsMap());                  
                  currentItem = String.valueOf(jsonObj.get("name"));
                  reRanked.add(currentItem);                
            }
            
            for(SearchHit hit : searchHitsNonPersonalized){

                  JSONObject jsonObj = new JSONObject(hit.getSourceAsMap());                  
                  currentItem = String.valueOf(jsonObj.get("name"));
                  original.add(currentItem);                
            }
            
            
            
            recommendationScores.clear();
            for(int i=0;i<values.length;i++){values[i]=0;}
            for(int i=0;i<recommendedProducts.size();i++){recommendationScores.add(i, (Double)0.0);}
            if(viewedItems.size()>0 || highlyViewed.size()>0){
                bulkRequest = doMultiplePharm24Updates(viewedItems,highlyViewed,values,recommendedProducts,recommendationScores);
            
                bulkResponse = client.bulk(bulkRequest);
                if(bulkResponse.hasFailures()){
                    for (BulkItemResponse bulkItemResponse : bulkResponse) {
                        if (bulkItemResponse.isFailed()) { 
                            BulkItemResponse.Failure failure = bulkItemResponse.getFailure(); 
                            System.out.println(failure.getMessage());
                        }
                    }
                } 
            }
            
            result.add(original);
            result.add(reRanked);
            
            
            return result;
            
        }else{
            throw new UnsupportedOperationException("Got more than one hits for one user!");
        }        
        
    }
    
    public List<String> performPersonalizedQueryfullSearch(RestHighLevelClient client, Integer userID, String query, String[] items) throws IOException{
        
        List<Integer> boughtItems, viewedItems, highlyBought, highlyViewed;
        List<Double> recommendationScores;
        List<String> reRanked,recommendedProducts;
        String currentItem;
        BulkRequest bulkRequest;
        BulkResponse bulkResponse;
        
        
        
        /**
         * First we'll search about the user and get all the products he's visited/bought
        */
        
        SearchRequest req = new SearchRequest("user_profile");
        SearchSourceBuilder mBuilder = new SearchSourceBuilder();
        mBuilder.query(QueryBuilders.termQuery("userId",userID));
        req.source(mBuilder);
        
        SearchResponse resp = client.search(req);
        
        SearchHits hits = resp.getHits();
        if(hits.getHits().length == 1){
            Map<String,Object> sourceAsMap = hits.getHits()[0].getSourceAsMap();            
            viewedItems = (List<Integer>)sourceAsMap.get("viewed");
            highlyViewed = (List<Integer>)sourceAsMap.get("highly_viewed");
            boughtItems = (List<Integer>)sourceAsMap.get("bought");
            highlyBought = (List<Integer>)sourceAsMap.get("highly_bought");
            recommendationScores = (List<Double>)sourceAsMap.get("recommendation_scores");
            recommendedProducts = (List<String>)sourceAsMap.get("recommended_products");
            
            Integer[] values = {1,1,1,1};
            if(viewedItems.size()>0 || highlyViewed.size()>0 || boughtItems.size()>0 || highlyBought.size()>0){
                bulkRequest = doMultipleUpdates(viewedItems,boughtItems,highlyBought,highlyViewed,values,recommendedProducts,recommendationScores);
                        
                bulkResponse = client.bulk(bulkRequest);
                if(bulkResponse.hasFailures()){
                    for (BulkItemResponse bulkItemResponse : bulkResponse) {
                        if (bulkItemResponse.isFailed()) { 
                            BulkItemResponse.Failure failure = bulkItemResponse.getFailure(); 
                            System.out.println(failure.getMessage());
                        }
                    }
                }
            }
            
            
            SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
            MatchQueryBuilder matchQuery = QueryBuilders.matchQuery("name", query);

            ScriptScoreFunctionBuilder scoreFunction = ScoreFunctionBuilders
                                                    .scriptFunction("Math.log("
                                                            + "2 + 1.5*(doc['times_viewed'].value  + 3*doc['times_clicked'].value + "
                                                            + "5*doc['times_bought'].value) + 3*(doc['viewed_by_current_user'].value + "
                                                            + "(5*doc['bought_by_current_user'].value) + (3*doc['clicked_by_current_user'].value) "
                                                            + "(doc['viewed_multiple_times_by_current_user'].value) + "
                                                            + "(5*doc['bought_multiple_times_by_current_user'].value)) + "
                                                            + "(0.75 * doc['recommended_for_current_user'].value))");
                        
            searchBuilder.query(new FunctionScoreQueryBuilder(matchQuery,scoreFunction).boostMode(CombineFunction.MULTIPLY));
            searchBuilder.size(200);
            searchBuilder.trackScores(true);
            
            SearchRequest searchRequest = new SearchRequest("items");

            searchRequest.source(searchBuilder);




            SearchResponse response = client.search(searchRequest);

            hits = response.getHits();
            SearchHit[] searchHits = hits.getHits();
            
            
            reRanked = new ArrayList<String>();
            for(SearchHit hit : searchHits){
                  JSONObject jsonObj = new JSONObject(hit.getSourceAsMap());                  
                  currentItem = String.valueOf(jsonObj.get("itemId"));
                  if(Arrays.asList(items).contains(currentItem)){
                      reRanked.add(currentItem);
                  }
            }
            
            
            
            int i = 0;
            while((reRanked.size()!=items.length)&&(i<items.length)){
                if(!reRanked.contains(items[i])){
                    reRanked.add(items[i]);
                }
                i++;
            }
            recommendationScores.clear();
            for(i=0;i<values.length;i++){values[i]=0;}
            for(i=0;i<recommendedProducts.size();i++){recommendationScores.add(i, (Double)0.0);}
            if(viewedItems.size()>0 || highlyViewed.size()>0 || boughtItems.size()>0 || highlyBought.size()>0){
                bulkRequest = doMultipleUpdates(viewedItems,highlyViewed,boughtItems,highlyBought,values,recommendedProducts,recommendationScores);
            
                bulkResponse = client.bulk(bulkRequest);
                if(bulkResponse.hasFailures()){
                    for (BulkItemResponse bulkItemResponse : bulkResponse) {
                        if (bulkItemResponse.isFailed()) { 
                            BulkItemResponse.Failure failure = bulkItemResponse.getFailure(); 
                            System.out.println(failure.getMessage());
                        }
                    }
                } 
            }                       
            
            return reRanked;
            
        }else if(hits.getHits().length==0){
            return performPersonalizedQueryfullSearch(client,query,items);
        }else{
            throw new UnsupportedOperationException("Got more than one hits for one user!");
        }        
        
    }
    
    public List<String> performPersonalizedQueryfullSearch(RestHighLevelClient client, String query, String[] items) throws IOException{
        //"Personalized" Anonymus search
        
        List<String> reRanked = new ArrayList<String>();
        String currentItem;
        
        SearchRequest request = new SearchRequest("items");
        SearchSourceBuilder mBuilder = new SearchSourceBuilder();
        
        String functionScript = "Math.log(2 + doc['times_viewed'].value  + 3*doc['times_clicked'].value + 5*doc['times_bought'].value)";
        
        MatchQueryBuilder matchQuery = QueryBuilders.matchQuery("name", query);
        ScriptScoreFunctionBuilder scoreFunction = ScoreFunctionBuilders
                                                .scriptFunction(functionScript);
        
        mBuilder.query(new FunctionScoreQueryBuilder(matchQuery,scoreFunction));
        mBuilder.size(200);
        mBuilder.trackScores(true);
        
        request.source(mBuilder);
        
        SearchResponse response = client.search(request);
        
        SearchHit[] hits = response.getHits().getHits();
        for(SearchHit hit : hits){
            JSONObject jsonObj = new JSONObject(hit.getSourceAsMap());                  
            currentItem = String.valueOf(jsonObj.get("itemId"));
            if(Arrays.asList(items).contains(currentItem)){
                reRanked.add(currentItem);
            }
        }
        
        int i = 0;
        while((reRanked.size()!=items.length)&&(i<items.length)){
            if(!reRanked.contains(items[i])){
                reRanked.add(items[i]);
            }
            i++;
        }
        
//        client.close();       
        return reRanked;
        
    }
    
    public void performQueryfullSearchWithPersonalization(RestHighLevelClient client) throws IOException{
        /**
         * First we open and parse the CSV file with the queries
         */
        
        String csvFile = "ADD PATH TO QUERYFULL SEARCHES";
        String pathToSubmissionFile = "ADD PATH TO SUBMISSION FILE";
        BufferedReader br = null;
        PrintWriter writer = null;
        String line = "";
        String csvSpliter = ";";       
        List<String> reRankedProds;
        Long count= 0L;
        
        try{
            System.out.println("Iterating through test_only_queries...");
            br = new BufferedReader(new FileReader(csvFile));
            writer = new PrintWriter(pathToSubmissionFile,"UTF-8");
            while((line = br.readLine())!=null){
                count++;
                String[] info = line.split(csvSpliter);
                String queryId = info[0];
                String userId = info[2];
                String[] items = info[info.length - 1].split(",");
                String[] nameTokens = info[info.length - 3].split(",");
                String name = getQuery(nameTokens);
                
                
                if(!userId.trim().equals("NA")){
                    reRankedProds = performPersonalizedQueryfullSearch(client,Integer.parseInt(userId),name,items);
                    writer.println(queryId + " " + reRankedProds.toString().replace("[", "").replace("]", "").replace(" ", ""));
                }else{
                    reRankedProds = performPersonalizedQuerylessSearch(client,name,items);
                    writer.println(queryId + " " + reRankedProds.toString().replace("[", "").replace("]", "").replace(" ",""));
                }
                
            }
            writer.close();
        } catch(FileNotFoundException e){
            e.printStackTrace();
        } catch (IOException e){
            e.printStackTrace();
        } finally{
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        client.close();
    }
    
    public static void main(String[] args) throws IOException{
        // will make requests for the elasticsearch 
                
        List<List<String>> results;
        String query = "ADD QUERY OR CATEGORY FOR SEARCH";
        
        //ADD USER ID WHO SEARCHES
        Long userId = 1;
        PersonalizedSearch searchObj = new PersonalizedSearch();
        
        final CredentialsProvider credsProvider = new BasicCredentialsProvider();
        credsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("elastic","elastic"));
        
        
        RestHighLevelClient client = new RestHighLevelClient(
                        RestClient.builder(
                                new HttpHost("localhost",9200),
                                new HttpHost("localhost",9201))
                                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                                    @Override
                                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder hacb) {
                                        return hacb.setDefaultCredentialsProvider(credsProvider);
                                    }
        }));
        
        /**
         * ---------COMMENT OUT ONE OF THE FOLLOWING TO DO A PERSONALIZED SEARCH FOR A SIGNED IN OR/AND AN ANONYMOUS USER--------
        */

        System.out.println("Starting collecting queryless/queryfull submissions...");
        System.out.println("");
//        searchObj.performQuerylessSearchWithPersonalization(client);
//        searchObj.performQueryfullSearchWithPersonalization(client);
//        results = searchObj.performPharm24PersonalizedQueryfullSearch(client, userId, query);
//        results = searchObj.performPharm24PersonalizedQuerylessSearch(client, userId, query);
        
        for(int i=0; i<results.size(); i++){
            for(int j = 0; j<results.get(i).size();j++){
                System.out.println(results.get(i).get(j));
            }
            System.out.println("-------RERANKED---------");
        }

        System.out.println("");
        System.out.println("I\'m done!");
        
        
    }

}
