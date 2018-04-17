package kr.tanbbang01.converter.util;

import com.ibm.icu.util.ChineseCalendar;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 날짜함수
 */
public class DateUtil {
  private static Pattern yearPattern = Pattern.compile("\\d{4}");
  private static Pattern twoYearPattern = Pattern.compile("(\\d{4}).*(\\d{4})");

  /**
   * 년월일만으로 오늘날짜를 구한다
   *
   * @return
   */
  public static Date getToday() {
    Calendar c = new GregorianCalendar();
    c.set(Calendar.HOUR_OF_DAY, 0);
    c.set(Calendar.MINUTE, 0);
    c.set(Calendar.SECOND, 0);
    c.set(Calendar.MILLISECOND, 0);
    return c.getTime();
  }

  /**
   * 현재 Date에서 시간정보를 지운다.
   *
   * @param self
   * @return
   */
  public static Date clearTime(final Date self) {
    if (self == null) return self;
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(self);
    calendar.set(Calendar.HOUR_OF_DAY, 0);
    calendar.set(Calendar.MINUTE, 0);
    calendar.set(Calendar.SECOND, 0);
    calendar.set(Calendar.MILLISECOND, 0);
    return calendar.getTime();
  }

  public static final String DATE8_FORMAT = "yyyyMMdd";
  public static final String DATE14_FORMAT = "yyyyMMddHHmmss";
  public static final String DATE_FORMAT = "yyyy-MM-dd";
  public static final String DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
  public static final String GMT_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'";

  /**
   * GMT format 날짜형식으로 변환
   *
   * @param d java.util.Date
   * @return
   */
  public static String getGMTFormattedDate(Date d) {
    if (d == null) return null;
    SimpleDateFormat sdf = new SimpleDateFormat(GMT_FORMAT);
    sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
    return sdf.format(d);
  }

  /**
   * GMT format 날짜형식으로 변환
   *
   * @param d java.sql.Date
   * @return
   */
  public static String getGMTFormattedDate(java.sql.Date d) {
    if (d == null) return null;
    return getGMTFormattedDate(new Date(d.getTime()));
  }

  /**
   * 지정된 형식으로 날짜를 문자열로 변환한다.
   *
   * @param d      java.util.Date
   * @param format 날짜형식
   * @return String
   */
  public static String getFormattedDate(Date d, String format) {
    if (d == null) return null;
    return new SimpleDateFormat(format).format(d);
  }

  /**
   * 지정된 형식으로 날짜를 문자열로 변환한다.
   *
   * @param d      java.sql.Date
   * @param format 날짜형식
   * @return String
   */
  public static String getFormattedDate(java.sql.Date d, String format) {
    if (d == null) return null;
    return getFormattedDate(new Date(d.getTime()), format);
  }

  /**
   * "yyyyMMdd"형식의 현재 날짜를 가져온다.
   *
   * @return String 현재 날짜("yyyyMMdd")
   */
  public static String getDate8() {
    return getFormattedDate(new Date(), DATE8_FORMAT);
  }

  /**
   * "yyyyMMdd"형식의 날짜를 가져온다.
   *
   * @param d java.util.Date
   * @return String 날짜("yyyyMMdd")
   */
  public static String getDate8(Date d) {
    if (d == null) return null;
    return getFormattedDate(d, DATE8_FORMAT);
  }

  /**
   * "yyyyMMdd"형식의 날짜를 가져온다.
   *
   * @param d java.sql.Date
   * @return String 날짜("yyyyMMdd")
   */
  public static String getDate8(java.sql.Date d) {
    if (d == null) return null;
    return getFormattedDate(new Date(d.getTime()), DATE8_FORMAT);
  }

  /**
   * "yyyyMMddHHmmss"형식의 현재 일시를 가져온다.
   *
   * @return String 현재 일시("yyyyMMddHHmmss")
   */
  public static String getDate14() {
    return getFormattedDate(new Date(), DATE14_FORMAT);
  }

  /**
   * "yyyyMMddHHmmss"형식의 현재 일시를 가져온다.
   *
   * @return String 현재 일시("yyyyMMddHHmmss")
   */
  public static String getDate14(Date d) {
    if (d == null) return null;
    return getFormattedDate(d, DATE14_FORMAT);
  }

  /**
   * yyyy-MM-dd 형식의 문자열을 파싱해서 Date를 구한다
   *
   * @param str String 'yyyy-MM-dd'
   * @return Date or null if str is null
   * @throws ParseException
   */
  public static Date parseDateString(String str) throws ParseException {
    if (str == null) return null;
    return new SimpleDateFormat(DATE_FORMAT).parse(str);
  }

  /**
   * 'yyyy-MM-dd HH:mm:ss' 형식의 문자열을 파싱해서 Date를 구한다
   *
   * @param str String 'yyyy-MM-dd HH:mm:ss'
   * @return Date or null if str is null
   * @throws ParseException
   */
  public static Date parseDateTimeString(String str) throws ParseException {
    if (str == null) return null;
    return new SimpleDateFormat(DATETIME_FORMAT).parse(str);
  }

  /**
   * 'yyyyMMdd' 형식의 문자열을 파싱해서 Date를 구한다
   *
   * @param str String 'yyyyMMdd'
   * @return Date or null if str is null
   * @throws ParseException
   */
  public static Date parseDate8String(String str) throws ParseException {
    if (str == null) return null;
    return new SimpleDateFormat(DATE8_FORMAT).parse(str);
  }

  /**
   * 'yyyyMMddHHmmss' 형식의 문자열을 파싱해서 Date를 구한다
   *
   * @param str String 'yyyyMMddHHmmss'
   * @return Date or null if str is null
   * @throws ParseException
   */
  public static Date parseDate14String(String str) throws ParseException {
    if (str == null) return null;
    return new SimpleDateFormat(DATE14_FORMAT).parse(str);
  }

  /**
   * 해당 Date의 마지막 시분초를 구한다
   *
   * @param self Date
   * @return Date
   */
  public static Date getEndDateOfDay(final Date self) {
    return getDate(self, 23, 59, 59);
  }

  /**
   * 해당 Date에 시/분/초를 지정한다
   * @param self Date
   * @param hour int 시
   * @param minute int 분
   * @param second int 초
   * @return Date
   */
  public static Date getDate(final Date self, int hour, int minute, int second) {
    if (self == null) return self;
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(self);
    calendar.set(Calendar.HOUR_OF_DAY, hour);
    calendar.set(Calendar.MINUTE, minute);
    calendar.set(Calendar.SECOND, second);
    calendar.set(Calendar.MILLISECOND, 0);
    return calendar.getTime();
  }

  /**
   * yyyy-MM-dd 형식의 문자열을 파싱해서 그날의 마지막 시분초 Date를 구한다
   *
   * @param str String 'yyyy-MM-dd'
   * @return Date or today if str is null or invalid
   */
  public static Date getEndDateOfDay(String str) {
    Date d;
    try {
      if (str == null)
        d = getToday();
      else
        d = parseDateString(str);
    } catch (ParseException e) {
      d = getToday();
    }
    return getEndDateOfDay(d);
  }

  /**
   * 네자리 년도만 리턴한다.
   *
   * @param input
   * @return 년도가 현재년도보다 크면 두개이상의 년도가 입력되었는지를 조사해서 유효한 년도를 리턴한다
   */
  public static String getValidYear(String input) {
    if (input == null) return null;
    Matcher matcher = yearPattern.matcher(input);
    if (!matcher.find())
      return null;
    String year = matcher.group();
    if (year.equals("0000"))
      return null;
    if (isValidYear(year))
      return year;
    return getValidYearFromTwoYears(input);
  }

  /**
   * 여러 년도가 입력된 경우 유효한 년도만 리턴한다.
   *
   * @param input
   * @return 년도가 현재년도보다 크면 현재년도를, 값이 없으면 null을 리턴한다.
   */
  private static String getValidYearFromTwoYears(String input) {
    if (input == null) return null;
    Matcher matcher = twoYearPattern.matcher(input);
    if (!matcher.find())
      return null;
    String year = matcher.group(1);
    if (isValidYear(year))
      return year;
    year = matcher.group(2);
    if (isValidYear(year))
      return year;
    return null;
  }

  /**
   * 입력된 발행년도가 유효한지를 체크한다. 즉 올해가 2017년인데 2020년이 입력된 경우는 유효하지 않는 발행년도로 간주한다.
   * 유효발행년도의 범위는 0001년에서 내년까지로 한다. 내년이 포함된 이유는 년말에 다음년도에 발행될 도서의 발행년도를 미래도
   * 지정되는 경우가 있기 때문.
   * @param year
   * @return
   */
  private static boolean isValidYear(String year) {
    Calendar cal = Calendar.getInstance();
    String currYear = String.valueOf(cal.get(Calendar.YEAR) + 1);
    if (year.equals("0000"))
      return false;
    if (currYear.compareTo(year) < 0)
      return false;
    else
      return true;
  }

  /**
   * 달의 첫날을 구한다.
   *
   * @param year  년도
   * @param month 월
   * @return Date 달의 첫날
   */
  public static Date getFirstDateOfMonth(int year, int month) {
    Calendar calendar = Calendar.getInstance();
    calendar.set(year, month - 1, 1);
    return clearTime(calendar.getTime());
  }

  /**
   * 주어진 날짜의 달의 첫날을 구한다.
   *
   * @param date Date
   * @return Date 달의 첫날
   */
  public static Date getFirstDateOfMonth(Date date) {
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(date);
    calendar.set(Calendar.DATE, 1);
    return clearTime(calendar.getTime());
  }

  /**
   * 달의 마지막 날을 구한다.
   *
   * @param year  년도
   * @param month 월
   * @return Date 달의 마지막날
   */
  public static Date getLastDateOfMonth(int year, int month) {
    Calendar calendar = Calendar.getInstance();
    calendar.set(year, month - 1, 1);
    calendar.set(Calendar.DATE, calendar.getActualMaximum(Calendar.DAY_OF_MONTH));
    return clearTime(calendar.getTime());
  }

  /**
   * 주어진 날짜의 달의 마지막 날을 구한다.
   *
   * @param date Date
   * @return Date 달의 마지막날
   */
  public static Date getLastDateOfMonth(Date date) {
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(date);
    calendar.set(Calendar.DATE, calendar.getActualMaximum(Calendar.DAY_OF_MONTH));
    return clearTime(calendar.getTime());
  }

  /**
   * 날짜의 년도
   *
   * @param date
   * @return 년도 int
   */
  public static int getYear(Date date) {
    return getIntDate(date, Calendar.YEAR);
  }

  /**
   * 날짜의 월
   *
   * @param date
   * @return 월 int
   */
  public static int getMonth(Date date) {
    return getIntDate(date, Calendar.MONTH) + 1;
  }

  /**
   * 날짜의 일
   *
   * @param date
   * @return 일 int
   */
  public static int getDayOfMonth(Date date) {
    return getIntDate(date, Calendar.DAY_OF_MONTH);
  }

  /**
   * 날짜의 요일
   *
   * @param date
   * @return 요일 int (1~7 - 일~토)
   */
  public static int getDayOfWeek(Date date) {
    return getIntDate(date, Calendar.DAY_OF_WEEK);
  }

  /**
   * 날짜의 Int값
   *
   * @param date
   * @param flag (ex: Calendar.YEAR, MONTH, DAY_OF_MONTH, DAY_OF_WEEK, ...)
   * @return 날짜의 Int값
   */
  public static int getIntDate(Date date, int flag) {
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(date);
    return calendar.get(flag);
  }

  /**
   * 음력날짜로 양력날짜를 구함
   *
   * @param lunarDate yyyyMMdd 음력
   * @return 양력날짜 Date
   */
  public static Date getDateFromLunar(String lunarDate) {
    if (lunarDate == null) return null;
    lunarDate = lunarDate.trim();
    if (lunarDate.length() != 8) return null;

    ChineseCalendar cc = new ChineseCalendar();
    cc.set(ChineseCalendar.EXTENDED_YEAR, Integer.parseInt(lunarDate.substring(0, 4)) + 2637);
    cc.set(ChineseCalendar.MONTH, Integer.parseInt(lunarDate.substring(4, 6)) - 1);
    cc.set(ChineseCalendar.DAY_OF_MONTH, Integer.parseInt(lunarDate.substring(6)));
    cc.set(ChineseCalendar.IS_LEAP_MONTH, getLeepMonth(lunarDate));
    return clearTime(cc.getTime());
  }

  private static int getLeepMonth(String lunarDate) {
    Calendar cal = Calendar.getInstance();
    cal.set(Calendar.YEAR, Integer.parseInt(lunarDate.substring(0,4)));
    cal.set(Calendar.MONTH, Integer.parseInt(lunarDate.substring(4,6))-1 );
    cal.set(Calendar.DAY_OF_MONTH, Integer.parseInt(lunarDate.substring(6,8)));

    ChineseCalendar cc = new ChineseCalendar();
    cc.setTimeInMillis(cal.getTimeInMillis());

    return cc.get(ChineseCalendar.IS_LEAP_MONTH);
  }

  /**
   * 주어진 시간에 분을 더한다.
   * @param date
   * @param minute
   * @return Date
   */
  public static Date addMinute(Date date, int minute) {
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(date);
    calendar.add(Calendar.MINUTE, minute);
    return calendar.getTime();
  }

  /**
   * 주어진 시간에 시를 더한다.
   * @param date
   * @param hour
   * @return Date
   */
  public static Date addHour(Date date, int hour) {
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(date);
    calendar.add(Calendar.HOUR, hour);
    return calendar.getTime();
  }

  /**
   * 주어진 시간에 일을 더한다.
   * @param date
   * @param day
   * @return Date
   */
  public static Date addDay(Date date, int day) {
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(date);
    calendar.add(Calendar.DATE, day);
    return calendar.getTime();
  }

  /**
   * 주어진 시간에 월을 더한다.
   * @param date
   * @param month
   * @return Date
   */
  public static Date addMonth(Date date, int month) {
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(date);
    calendar.add(Calendar.MONTH, month);
    return calendar.getTime();
  }

  /**
   * 주어진 시간에 년을 더한다.
   * @param date
   * @param year
   * @return Date
   */
  public static Date addYear(Date date, int year) {
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(date);
    calendar.add(Calendar.YEAR, year);
    return calendar.getTime();
  }
}
