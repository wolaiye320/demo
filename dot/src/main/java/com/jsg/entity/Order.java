package com.jsg.entity;

/**
 * @Auther: sam
 * @Date: 2019/4/12
 * @Description:
 * @return
 */
public class Order {
    String customId;
    Long price;
    String itemId;
    String cityId;

    public Order() {

    }

    public Long getPrice() {
        return price;
    }

    public void setPrice(Long price) {
        this.price = price;
    }

    public String getCityId() {
        return cityId;
    }

    public void setCityId(String cityId) {
        this.cityId = cityId;
    }

    public String getItemId() {
        return itemId;
    }

    public void setItemId(String itemId) {
        this.itemId = itemId;
    }

    public String getCustomId() {
        return customId;
    }

    public void setCustomId(String customId) {
        this.customId = customId;
    }

    public  Order(String customId, Long price, String itemId, String cityId) {
        this.setCustomId(customId);
        this.setPrice(price);
        this.setItemId(itemId);
        this.setCityId(cityId);
    }
}
