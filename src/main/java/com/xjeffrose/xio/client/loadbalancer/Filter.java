package com.xjeffrose.xio.client.loadbalancer;

public interface Filter {
  <T> boolean contains(T serviceName, T item);
}

public class ServiceFiler implements Filter {

  @Override
  public <T> boolean contains(T serviceName, T item) {
    if (!serviceName.getClass().equals(item.getClass())) {
      return false;
    }

  }
}