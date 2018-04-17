package kr.tanbbang01.converter.util;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.ResourceBundle;

/**
 * 리소스 파일을 관리하는 클래스
 */
public class Resourcer {
  private static Resourcer resourcer = null;
  private static final String QUERY_FILE = "QUERY_FILE";
  private ResourceBundle bundle;
  private ResourceBundle query;

  public static Resourcer getInstance(String config) {
    if (resourcer == null)
      resourcer = new Resourcer(config);
    return resourcer;
  }

  private Resourcer(String config) {
    bundle = ResourceBundle.getBundle(config);
  }

  /**
   * 키값으로 리소스의 Data를 가져온다.
   *
   * @param key
   * @return
   */
  public String getString(String key) {
    return getString(key, null);
  }

  /**
   * 키값으로 리소스의 Data를 가져온다.
   *
   * @param key
   * @param defaultValue
   * @return
   */
  public String getString(String key, String defaultValue) {
    try {
      return bundle.getString(key).trim();
    } catch (Exception e) {
      return defaultValue;
    }
  }

  /**
   * 키값으로 리소스의 Value를 List<String>로 리턴한다.
   * @param key
   * @param delimiter Value를 split할 구분자
   * @return List<String>
   */
  public List<String> getArrayList(String key, String delimiter) {
    return getArrayList(key, null, delimiter);
  }

  /**
   * 키값으로 리소스의 Value를 List<String>로 리턴한다.
   * @param key
   * @param defaultValue 값이 없을 경우 사용할 기본값
   * @param delimiter Value를 split할 구분자
   * @return List<String>
   */
  public List<String> getArrayList(String key, String defaultValue, String delimiter) {
    List<String> list = new ArrayList<String>();
    try {
      String value = bundle.getString(key).trim();
      String[] values = value.split(delimiter);
      for (String v : values)
        list.add(v);
      return list;
    } catch (Exception e) {
      try {
        if (defaultValue != null) {
          String[] defaults = defaultValue.split(delimiter);
          for (String v : defaults)
            list.add(v);
        }
      } catch (Exception e1) {
      }
      return list;
    }
  }

  /**
   * 키값으로 integer value를 가져온다.
   *
   * @param key
   * @return int
   */
  public int getInt(String key) {
    return getInt(key, 0);
  }

  /**
   * 키값으로 integer value를 가져온다.
   *
   * @param key
   * @param defaultValue
   * @return int
   */
  public int getInt(String key, int defaultValue) {
    try {
      String value = bundle.getString(key).trim();
      if (value != null)
        return Integer.parseInt(value);
      return defaultValue;
    } catch (Exception e) {
      return defaultValue;
    }
  }

  /**
   * 키값으로 Boolean value를 가져온다.
   *
   * @param key
   * @return boolean
   */
  public boolean getBoolean(String key) {
    return getBoolean(key, false);
  }

  /**
   * 키값으로 Boolean value를 가져온다.
   *
   * @param key
   * @param defaultValue
   * @return boolean
   */
  public boolean getBoolean(String key, boolean defaultValue) {
    try {
      String value = bundle.getString(key).trim();
      if (value != null)
        return Boolean.parseBoolean(value);
      return defaultValue;
    } catch (Exception e) {
      return defaultValue;
    }
  }

  public List<String> getPropertyList() {
    Enumeration<String> keys = bundle.getKeys();
    List<String> list = new ArrayList<String>();
    while (keys.hasMoreElements()) {
      String key = keys.nextElement();
      list.add(String.format("%s=%s", key, bundle.getString(key)));
    }
    return list;
  }

  private void instanceQuery() {
    if (query == null) {
      String queryFile = bundle.getString(QUERY_FILE);
      query = ResourceBundle.getBundle(queryFile);
    }
  }

  /**
   * Query문을 리턴한다.
   *
   * @param key
   * @return
   */
  public String getQueryString(String key) {
    try {
      instanceQuery();
      return query.getString(key);
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * 조건문을 지정하여 Query문을 리턴한다.
   *
   * @param key
   * @param condition
   * @return
   */
  public String getQueryString(String key, String condition) {
    try {
      instanceQuery();
      return String.format(query.getString(key), query.getString(condition));
    } catch (Exception e) {
      return null;
    }
  }
}
