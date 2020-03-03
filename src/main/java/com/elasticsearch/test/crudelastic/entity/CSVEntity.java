package com.elasticsearch.test.crudelastic.entity;

import java.time.LocalDateTime;
import java.util.List;

public class CSVEntity {

    private LocalDateTime lastClickedMin;
    private String vendorNewId;
    private Long count;
    private String scrollId;
    private String subHeadLine;
    private String sources;
    private List<String> rcsCodes;
    private String vendorId;
    private List<String> rickcs;
    private String permIds;
    private List<Integer> entitlements;
    private String headLang;

    public LocalDateTime getLastClickedMin() {
        return lastClickedMin;
    }

    public void setLastClickedMin(LocalDateTime lastClickedMin) {
        this.lastClickedMin = lastClickedMin;
    }

    public String getVendorNewId() {
        return vendorNewId;
    }

    public void setVendorNewId(String vendorNewId) {
        this.vendorNewId = vendorNewId;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    public String getScrollId() {
        return scrollId;
    }

    public void setScrollId(String scrollId) {
        this.scrollId = scrollId;
    }

    public String getSubHeadLine() {
        return subHeadLine;
    }

    public void setSubHeadLine(String subHeadLine) {
        this.subHeadLine = subHeadLine;
    }

    public String getSources() {
        return sources;
    }

    public void setSources(String sources) {
        this.sources = sources;
    }

    public List<String> getRcsCodes() {
        return rcsCodes;
    }

    public void setRcsCodes(List<String> rcsCodes) {
        this.rcsCodes = rcsCodes;
    }

    public String getVendorId() {
        return vendorId;
    }

    public void setVendorId(String vendorId) {
        this.vendorId = vendorId;
    }

    public List<String> getRickcs() {
        return rickcs;
    }

    public void setRickcs(List<String> rickcs) {
        this.rickcs = rickcs;
    }

    public String getPermIds() {
        return permIds;
    }

    public void setPermIds(String permIds) {
        this.permIds = permIds;
    }

    public List<Integer> getEntitlements() {
        return entitlements;
    }

    public void setEntitlements(List<Integer> entitlements) {
        this.entitlements = entitlements;
    }

    public String getHeadLang() {
        return headLang;
    }

    public void setHeadLang(String headLang) {
        this.headLang = headLang;
    }
}
