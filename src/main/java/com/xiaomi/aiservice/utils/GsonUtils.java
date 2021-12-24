package com.xiaomi.aiservice.utils;

import java.util.Map.Entry;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import org.apache.commons.lang3.StringUtils;

/**
 * @author wanglingda@xiaomi.com
 */

public class GsonUtils {

  private static final Gson gson = new Gson();

  public static String getAsJson(Object object) {
    return gson.toJson(object);
  }

  public static JsonElement getAsJsonTree(Object object) {
    return gson.toJsonTree(object);
  }

  public static JsonObject getJsonObject(String json) {
    try {
      return gson.fromJson(json, JsonObject.class);
    } catch (JsonSyntaxException e) {
      return null;
    }
  }

  public static <T> T getTObject(String json, Class<T> classOfT) {
    try {
      return gson.fromJson(json, classOfT);
    } catch (JsonSyntaxException e) {
      return null;
    }
  }

  public static <T> T getTObject(JsonElement jsonElement, Class<T> classOfT) {
    try {
      return gson.fromJson(jsonElement, classOfT);
    } catch (JsonSyntaxException e) {
      return null;
    }
  }

  public static String getStrProp(JsonObject jsonObject, String prop) {
    if (hasProp(jsonObject, prop)) {
      return jsonObject.get(prop).getAsString();
    }
    return StringUtils.EMPTY;
  }

  public static int getStrPropAsInt(JsonObject jsonObject, String prop) {
    return Integer.parseInt(getStrProp(jsonObject, prop));
  }

  public static int getIntProp(JsonObject jsonObject, String prop) {
    if (hasProp(jsonObject, prop)) {
      return jsonObject.get(prop).getAsInt();
    }
    return Integer.MIN_VALUE;
  }

  public static int getIntProp(JsonObject jsonObject, String prop, int defaultValue) {
    if (hasProp(jsonObject, prop)) {
      return jsonObject.get(prop).getAsInt();
    }
    return defaultValue;
  }

  public static double getDoubleProp(JsonObject jsonObject, String prop) {
    if (hasProp(jsonObject, prop)) {
      return jsonObject.get(prop).getAsDouble();
    }
    return 0D;
  }

  public static double getDoubleProp(JsonObject jsonObject, String prop, double defaultValue) {
    if (hasProp(jsonObject, prop)) {
      return jsonObject.get(prop).getAsDouble();
    }
    return defaultValue;
  }

  public static long getLongProp(JsonObject jsonObject, String prop) {
    if (hasProp(jsonObject, prop)) {
      return jsonObject.get(prop).getAsLong();
    }
    return Long.MIN_VALUE;
  }

  public static long getLongProp(JsonObject jsonObject, String prop, long defaultValue) {
    if (hasProp(jsonObject, prop)) {
      return jsonObject.get(prop).getAsLong();
    }
    return defaultValue;
  }

  public static boolean getBooleanProp(JsonObject jsonObject, String prop) {
    if (hasProp(jsonObject, prop)) {
      return jsonObject.get(prop).getAsBoolean();
    }
    return false;
  }

  public static JsonObject getJsonObjectProp(JsonObject jsonObject, String prop) {
    if (hasJsonObjectProp(jsonObject, prop)) {
      return jsonObject.get(prop).getAsJsonObject();
    }
    return null;
  }

  public static JsonArray getJsonArrayProp(JsonObject jsonObject, String prop) {
    if (hasJsonArrayProp(jsonObject, prop)) {
      return jsonObject.get(prop).getAsJsonArray();
    }
    return null;
  }

  public static boolean isBlankStrProp(JsonObject jsonObject, String prop) {
    return StringUtils.isBlank(getStrProp(jsonObject, prop));
  }

  public static boolean isNotBlankStrProp(JsonObject jsonObject, String prop) {
    return StringUtils.isNotBlank(getStrProp(jsonObject, prop));
  }

  public static boolean hasProp(JsonObject jsonObject, String prop) {
    return jsonObject.has(prop) && jsonObject.get(prop).isJsonPrimitive();
  }

  public static boolean hasJsonObjectProp(JsonObject jsonObject, String prop) {
    return jsonObject.has(prop) && jsonObject.get(prop).isJsonObject();
  }

  public static boolean hasJsonArrayProp(JsonObject jsonObject, String prop) {
    return jsonObject.has(prop) && jsonObject.get(prop).isJsonArray();
  }

  public static String getJsonStrProp(String json, String prop) {
    if (StringUtils.isBlank(json) || StringUtils.isBlank(prop)) {
      return StringUtils.EMPTY;
    }
    JsonObject jsonObject = getJsonObject(json);
    if (jsonObject == null) {
      return StringUtils.EMPTY;
    }
    return getStrProp(jsonObject, prop);
  }

  public static String mergeJson(String json1, String json2) {
    if (StringUtils.isBlank(json1)) {
      return StringUtils.EMPTY;
    }
    if (StringUtils.isBlank(json2)) {
      return json1;
    }
    return mergeJson(getJsonObject(json1), getJsonObject(json2)).toString();
  }

  public static JsonObject mergeJson(JsonObject obj1, JsonObject obj2) {
    if (obj1 == null) {
      return new JsonObject();
    }
    if (obj2 == null) {
      return obj1;
    }
    for (Entry<String, JsonElement> entry : obj1.entrySet()) {
      String key = entry.getKey();
      obj2.add(key, entry.getValue());
    }
    return obj2;
  }

  public static JsonObject putAll(JsonObject appendable, JsonObject jsonObject) {
    for (Entry<String, JsonElement> entry : jsonObject.entrySet()) {
      String key = entry.getKey();
      appendable.add(key, entry.getValue());
    }
    return appendable;
  }

  public static boolean isNotEmpty(JsonArray array) {
    return array != null && array.size() > 0;
  }

  public static boolean isEmpty(JsonArray array) {
    return !isNotEmpty(array);
  }
}
