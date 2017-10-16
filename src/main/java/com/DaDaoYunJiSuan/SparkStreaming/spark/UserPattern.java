package com.DaDaoYunJiSuan.SparkStreaming.spark;

import java.io.Serializable;
import java.util.List;

/**
 * Created by Cristph on 2017/10/16.
 */
public class UserPattern implements Serializable{

    private String name;
    private String url;
    private List<String> tags;
    private List<String> groups;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
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

    public List<String> getGroups() {
        return groups;
    }

    public void setGroups(List<String> groups) {
        this.groups = groups;
    }

    @Override
    public String toString() {
        return "UserPattern{" +
                "name='" + name + '\'' +
                ", url='" + url + '\'' +
                ", tags=" + tags +
                ", groups=" + groups +
                '}';
    }
}
