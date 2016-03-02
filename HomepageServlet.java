package cc.cmu.edu.minisite;

import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONArray;
import org.json.JSONObject;


import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;

public class HomepageServlet extends HttpServlet {
	private static AmazonDynamoDBClient client;
	private final static String dynamoDBTableName = "postTable";
	
    public HomepageServlet() throws Exception {
    	connectDynamoDB();
    }

    @Override
    protected void doGet(final HttpServletRequest request, 
            final HttpServletResponse response) throws ServletException, IOException {

        JSONObject result = new JSONObject();
        JSONArray postsObject = new JSONArray();
        String id = request.getParameter("id");
        
        List<JSONObject> postsList = getPosts(id);
        for(JSONObject post : postsList) {
        	postsObject.put(post);
        }
        
        result.put("posts", postsObject);
        
        PrintWriter out = response.getWriter();
        out.print(String.format("returnRes(%s)", result.toString()));
        out.close();
    }

    @Override
    protected void doPost(final HttpServletRequest req, final HttpServletResponse resp) throws ServletException, IOException {
        doGet(req, resp);
    }
    
    /* connect to dynamoDB */
    private void connectDynamoDB() {
    	try {
        	client = new AmazonDynamoDBClient(new EnvironmentVariableCredentialsProvider());
    	} catch(Exception e) {
    		e.printStackTrace();
    	}
    }
    
    /* return a list of post in JSONObject based on id from DynamoDB*/
    private List<JSONObject> getPosts(String id) {
    	//use mapper to query
    	DynamoDBMapper mapper = new DynamoDBMapper(client);
    	Post postKey = new Post();
    	postKey.setUid(Integer.parseInt(id)); //set user id as primary hash key
    	
    	SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    	String currentDate = dateFormat.format(new Date());
    	Condition rangeKeyCondition = new Condition()
        	.withComparisonOperator(ComparisonOperator.LE.toString())
        	.withAttributeValueList(new AttributeValue(currentDate));
    	
    	DynamoDBQueryExpression<Post> queryExpression  = new DynamoDBQueryExpression<Post>()
    			.withHashKeyValues(postKey)
    			.withRangeKeyCondition("Timestamp", rangeKeyCondition);
    	
    	//get a list of Post objects parsing by DynamoDB
    	List<Post> postList = mapper.query(Post.class, queryExpression);
    	
    	List<JSONObject> postJsonList = new ArrayList<JSONObject>();
    	for(Post postObject : postList) { //convert Post object to JSON object and add to list
    		String postString = postObject.getPost();
    		JSONObject jsonObject = new JSONObject(postString);
    		postJsonList.add(jsonObject);
    	}
    	return postJsonList;
    }
    
    /* nested class for DynamoDBTable mapper */
    @DynamoDBTable(tableName=dynamoDBTableName)
    public static class Post {
        private int uid;
        private String timestamp;
        private String post;

        @DynamoDBHashKey(attributeName="UserID")
        public int getUid() { return uid; }
        public void setUid(int uid) { this.uid = uid; }
        
        @DynamoDBRangeKey(attributeName="Timestamp")
        public String getTimestamp() { return timestamp; }
        public void setTimestamp(String timestamp) { this.timestamp = timestamp; }
 
        @DynamoDBAttribute(attributeName="Post")
        public String getPost() { return post; }
        public void setPost(String post) { this.post = post; }

        @Override
        public String toString() {
            return "uid: " + uid + ", timestamp: " + timestamp + ", post: " + post;            
        }
    }
}
