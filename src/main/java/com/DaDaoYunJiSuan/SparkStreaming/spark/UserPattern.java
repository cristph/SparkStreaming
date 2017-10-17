package com.DaDaoYunJiSuan.SparkStreaming.spark;

import org.bson.Document;

import java.io.Serializable;
import java.util.Date;
import java.util.List;

/**
 * Created by Cristph on 2017/10/16.
 */
public class UserPattern implements Serializable{

    private String name;
    private long addTime;
    private String url;
    private List<String> tags;
    private String gname;
    private Date joinTime;

    public UserPattern(Document document) {
        this.name=(String)document.get("name");
        this.url=(String)document.get("url");
        this.addTime = System.currentTimeMillis();
        this.joinTime=(Date)((Document)document.get("join_groups")).get("time");
        this.gname=(String)((Document)document.get("join_groups")).get("group_url");
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getAddTime() {
        return addTime;
    }

    public void setAddTime(long addTime) {
        this.addTime = addTime;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public List<String> getTags() {
        return tags;
    }

    public void setTags(List<String> tags) {
        this.tags = tags;
    }

    public String getGname() {
        return gname;
    }

    public void setGname(String gname) {
        this.gname = gname;
    }

    public Date getJoinTime() {
        return joinTime;
    }

    public void setJoinTime(Date joinTime) {
        this.joinTime = joinTime;
    }

    public UserPattern addTags(String tagName){
        tags.add(tagName);
        return this;
    }

    @Override
    public String toString() {
        return "UserPattern{" +
                "name='" + name + '\'' +
                ", addTime=" + addTime +
                ", url='" + url + '\'' +
                ", tags=" + tags +
                ", gname='" + gname + '\'' +
                ", joinTime='" + joinTime + '\'' +
                '}';
    }
}
