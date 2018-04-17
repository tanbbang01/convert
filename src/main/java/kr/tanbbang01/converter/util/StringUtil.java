package kr.tanbbang01.converter.util;

import java.io.UnsupportedEncodingException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 문자열 Util
 */
public class StringUtil {
  private static Pattern numberPattern = Pattern.compile("\\d+");

  /**
   * 문자열을 UTF-8 바이트길이로 리턴한다.
   *
   * @param source
   * @param len    UTF-8 Byte 길이
   * @return String
   */
  public static String subByte(String source, int len) {
    if (source == null || len <= 0)
      return source;
    try {
      byte[] sourceBytes = source.getBytes("UTF-8");
      int offset = len >= source.length() ? source.length() - 1 : len;
      for (; sourceBytes.length > len; offset--) {
        source = source.substring(0, offset);
        sourceBytes = source.getBytes("UTF-8");
      }
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    }
    return source.trim();
  }

  /**
   * 문자열중에서 유효한 숫자영역만을 리턴한다.
   * 예) 200,300원(100,000원) => 200300만 리턴한다.
   *
   * @param input
   * @return String
   */
  public static String extractNumber(String input) {
    if (input == null || input.length() <= 0)
      return input;

    StringBuffer sb = new StringBuffer("");
    for (int i = 0; i < input.length(); i++) {
      if (Character.isDigit(input.charAt(i))) {
        sb.append(input.charAt(i++));
        for (; i < input.length(); i++) {
          if (input.charAt(i) == ',')
            continue;
          if (input.charAt(i) == '.' || Character.isDigit(input.charAt(i)))
            sb.append(input.charAt(i));
          else
            break;
        }
        break;
      }
    }
    return sb.toString();
  }

  /**
   * 문자열앞에 padding문자열을 numberOfChars수만큼 붙혀서 리턴한다.
   *
   * @param src
   * @param numberOfChars
   * @param padding
   * @return
   */
  public static String padLeft(String src, int numberOfChars, String padding) {
    if (numberOfChars <= src.length())
      return src;
    else
      return getPadding(padding, numberOfChars - src.length()) + src;
  }

  public static String multiply(String padding, Number factor) {
    int size = factor.intValue();
    if (size == 0)
      return "";
    else if (size < 0) {
      throw new IllegalArgumentException("multiply() should be called with a number of 0 or greater not: " + size);
    }
    StringBuilder answer = new StringBuilder(padding);
    for (int i = 1; i < size; i++) {
      answer.append(padding);
    }
    return answer.toString();
  }

  private static String getPadding(String padding, int length) {
    if (padding.length() < length)
      return multiply(padding, length / padding.length() + 1).substring(0, length);
    else
      return padding.substring(0, length);
  }

  /**
   * 문자열속의 숫자만 빼서 zeroCnt만큼 앞에 0을 채워서 해당 숫자부분만 리턴한다.
   *
   * @param src
   * @param zeroCnt
   * @return
   */
  public static String extractNumberAndLeftPadZero(String src, int zeroCnt) {
    if (src == null || src.length() <= 0)
      return src;
    Matcher matcher = numberPattern.matcher(src);
    if (matcher.find())
      return padLeft(matcher.group(), zeroCnt, "0");
    return src;
  }

  /**
   * 문자열속의 숫자를 add만큼 더해서 리턴한다.
   *
   * @param src
   * @param add
   * @return
   */
  public static String plusNumberInString(String src, int add) {
    if (src == null || src.length() <= 0 || add == 0)
      return src;
    Matcher matcher = numberPattern.matcher(src);
    if (matcher.find()) {
      int beginOffset = matcher.start();
      int endOffset = beginOffset + matcher.group().length();
      String digits = src.substring(beginOffset, endOffset);
      try {
        return src.substring(0, beginOffset) + (Integer.parseInt(digits) + add) + src.substring(endOffset);
      } catch (NumberFormatException e) {
        return src;
      }
    }
    return src;
  }

  /**
   * 영문 알파벳인지 확인
   *
   * @param uniChar
   * @return boolean
   */
  public static boolean isEngAlpha(char uniChar) {
    // Basic Latin 영역의 영문 알파벳만을 영어의 알파벳으로 인정한다. ( A~Z, a~z )
    if ((uniChar >= 0x0041 && uniChar <= 0x005A) || (uniChar >= 0x0061 && uniChar <= 0x007A))
      return true;
    return false;
  }

  /**
   * 한자인지 확인
   *
   * @param c
   * @return boolean
   */
  public static boolean isHanja(char c) {
    if ((c >= 0x3400 && c <= 0x4dbf) || (c >= 0x4e00 && c <= 0x9fff) || (c >= 0xf900 && c <= 0xfaff))
      return true;
    return false;
  }

  /**
   * IP 포맷팅 (192.000.000.001 -> 192.0.0.1)
   *
   * @param ip
   * @return
   */
  public static String formatIp4(String ip) {
    if (ip == null) return ip;
    StringBuffer formattedIp = new StringBuffer();
    try {
      String[] ips = ip.split("\\.");
      for (String part : ips) {
        if (formattedIp.length() > 0)
          formattedIp.append('.');
        formattedIp.append(Integer.parseInt(part));
      }
    } catch (NumberFormatException e) {
      return ip;
    }
    return formattedIp.toString();
  }

}
