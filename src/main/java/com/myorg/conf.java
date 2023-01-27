package com.myorg;
import java.util.*;

public class conf {
    
    public String s3_bucket      = "s3-bucket-java-cdk";
    public String role_name       = "iam-cdk-java-role";
    public HashMap<String,Object> iam_group1 = new HashMap<String,Object>();
    public HashMap<String,Object> iam_group2 = new HashMap<String,Object>();
    public HashMap<String,Object> iam_group3 = new HashMap<String,Object>();
    public HashMap<String,Object> iam_group4 = new HashMap<String,Object>();
    public HashMap<String,Object> iam_group5 = new HashMap<String,Object>();
    public HashMap<String,Object> iam_group6 = new HashMap<String,Object>();
    public ArrayList<HashMap<String,Object>> groups = new ArrayList<HashMap<String,Object>>();
    
    // list of users you want to create
    List<String> users = List.of("user1","user2","user3","user6","user7","user8","user9","user10");
    
    // list of groups you want to create
    public ArrayList<HashMap<String,Object>> groupsList(){
        
        // group1
        this.iam_group1.put("groupname", "group1");
        this.iam_group1.put("users",List.of("user1","user2","user3"));
        this.iam_group1.put("actions", List.of("s3:*","sagemaker:*","ec2:*"));
        this.iam_group1.put("managedpolicies",List.of(
            "arn:aws:iam::aws:policy/AmazonSageMakerFullAccess"
        ));
        
        // group2
        this.iam_group2.put("groupname","group2");
        this.iam_group2.put("users", List.of("user1","user2","user3"));
        this.iam_group2.put("actions", List.of("iam:*","eks:*"));
        // group3
        this.iam_group3.put("groupname","group3");
        this.iam_group3.put("users", List.of("user1","user2"));
        this.iam_group3.put("actions", List.of("iam:*","eks:*"));
        
        // group4
        this.iam_group4.put("groupname","group4");
        this.iam_group4.put("users", List.of("user1","user2","user3"));
        this.iam_group4.put("actions", List.of("iam:*","eks:*"));
        
        // group5
        // group4
        this.iam_group5.put("groupname","group5");
        this.iam_group5.put("users", List.of("user1","user2","user3"));
        this.iam_group5.put("actions", List.of("iam:*","eks:*"));
        
        this.iam_group6.put("groupname","group6");
        this.iam_group6.put("users", List.of("user8","user9","user10"));
        this.iam_group6.put("actions", List.of("iam:*","ecs:*"));
       
        
        this.groups.add(this.iam_group1);
        this.groups.add(this.iam_group2);
        this.groups.add(this.iam_group3);
        this.groups.add(this.iam_group4);
        this.groups.add(this.iam_group5);
        this.groups.add(this.iam_group6);
        
        return this.groups;
    }
    
    
    
}
