package kr.tanbbang01.converter.db.init.table;

import com.google.common.base.Stopwatch;
import kr.tanbbang01.converter.util.Resourcer;
import org.apache.commons.dbcp.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.concurrent.TimeUnit;

/**
 * 프로그램 시작전 할일
 * 1.LOPE_ERD.sql
 *  > 'sysdate' > sysdate
 *
 * 2.SMUF_v2_ERD.sql
 * 이름이 너무 길어서 생성 불가함으로 길이를 맞춰줌
 *  > FK_SEAT_CHARGE_METHOD_SETTING_0 -> FK_SEAT_CHARGE_METHOD_SETTIN_0
 *  > FK_SEAT_CHARGE_METHOD_SETTING_1 -> FK_SEAT_CHARGE_METHOD_SETTIN_1
 *  > FK_ROOM_RESERVATION_EQUIPMENT_0 -> FK_ROOM_RESERVATION_EQUIPMEN_0
 *  > FK_ROOM_RESERVATION_EQUIPMENT_1 -> FK_ROOM_RESERVATION_EQUIPMEN_1
 *  > FK_ROOM_RESERVATION_EQUIPMENT_2 -> FK_ROOM_RESERVATION_EQUIPMEN_2
 *
 * Created by InSeong on 17. 11. 20..
 */
public class TableInit {
  private static Logger logger = LoggerFactory.getLogger(TableInit.class);

  private String s8FilePath = null;
  private String smufFilePath = null;
  private String dbLinkName = null;
  private String owner = null;
  private Resourcer config = null;
  private Connection conn = null;
  private Connection destConn = null;

  LinkedHashMap<String, LinkedHashMap<String, String>> targetMap = null;
  LinkedHashMap<String, String> constraintsMap = new LinkedHashMap<String, String>();

  public TableInit(String configFile) throws SQLException {
    this.config = Resourcer.getInstance(configFile);

    BasicDataSource sourceBds = new BasicDataSource();
    sourceBds.setDriverClassName(config.getString("SOURCE_JDBC"));
    sourceBds.setUrl(config.getString("SOURCE_CONNECTION_STRING"));
    sourceBds.setUsername(config.getString("SOURCE_USER"));
    sourceBds.setPassword(config.getString("SOURCE_PASSWORD"));
    sourceBds.setMaxActive(-1);
    sourceBds.setMaxIdle(-1);
    sourceBds.setDefaultAutoCommit(true);
    conn = sourceBds.getConnection();

    BasicDataSource destBds = new BasicDataSource();
    destBds.setDriverClassName(config.getString("DEST_JDBC"));
    destBds.setUrl(config.getString("DEST_CONNECTION_STRING"));
    destBds.setUsername(config.getString("DEST_USER"));
    destBds.setPassword(config.getString("DEST_PASSWORD"));
    destBds.setMaxActive(-1);
    destBds.setMaxIdle(-1);
    destBds.setDefaultAutoCommit(true);
    destConn = destBds.getConnection();

    dbLinkName = config.getString("DB_LINK_NAME");
    s8FilePath = config.getString("SOLARS8_INIT_FILE_PATH");
    smufFilePath = config.getString("SMUF2_INIT_FILE_PATH");
    owner = config.getString("DEST_USER");
  }

  private void connClose() {
    try{
      conn.close();
      destConn.close();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  public void create(){
    Stopwatch stopwatch = new Stopwatch();
    stopwatch.start();

    targetMap = new LinkedHashMap<String, LinkedHashMap<String, String>>();  // 모든 대상을 담는 Map
    createSolars8CodeDBLink();      // DB LINK 생성
    createS8Table();                // Solars8 테이블 생성
    createSmuf2Table();             // Smuf2 테이블 생성
    disabledFk();                   // FK 비활성화
    connClose();

    stopwatch.stop(); // optional

    logger.info("실행 시간 : {} ms", stopwatch.elapsedTime(TimeUnit.MILLISECONDS)); //실행 시간
  }


  /**
   * 변환을 위해 설정되어 있는 FK 를 비활성화 시킨다.
   */
  private void disabledFk() {
    PreparedStatement pstmt = null;
    ResultSet rs = null;
    String alterSql = "";

    StringBuilder sb = new StringBuilder();
    sb.append("  SELECT 'ALTER TABLE '||x.table_name ||' DISABLE CONSTRAINT '|| x.constraint_name ");
    sb.append("    FROM ( SELECT table_name, constraint_name ");
    sb.append("             FROM USER_CONSTRAINTS ");
    sb.append("           WHERE TABLE_NAME IN ( SELECT table_name ");
    sb.append("                                   FROM all_tables ");
    sb.append("                                   WHERE owner = ? ) ");
    sb.append("             AND constraint_type = 'R' ");
    sb.append("         ) x ");
    sb.append("   ORDER BY x.table_name , x.constraint_name ");

    try{
      pstmt = destConn.prepareStatement(sb.toString());
      pstmt.setString(1, owner.toUpperCase());
      rs = pstmt.executeQuery();
      pstmt.clearParameters();

      while (rs.next()) {
        alterSql = rs.getString(1);
        logger.info("Alter SQL [ {} ]", alterSql);
        pstmt = destConn.prepareStatement(alterSql);
        pstmt.executeUpdate();
        pstmt.close();
      }

    } catch (SQLException e) {
      logger.error("SQL Exception !! SQL [ {} ]", alterSql);
      e.printStackTrace();
    } finally {
      if (pstmt != null) {
        try {
          pstmt.close();
        } catch (SQLException e) {  }
      }
      if (rs != null) {
        try {
          rs.close();
        } catch (SQLException e) {  }
      }
    }

  }

  /**
   * DB_LINK 생성
   * @return
   */
  private boolean createSolars8CodeDBLink() {
    PreparedStatement pstmt = null;
    ResultSet rs = null;
    Boolean result = false;

    try{
      logger.debug("========================================== ");
      boolean isExistsLink = isExistsDatabaseLink();    //DataBase Link 가 존재하는지 여부 확인

      if(!isExistsLink) {   //없으면 생성
        logger.debug("00.SOLARS8_CODE DBLink Creating Start .....");

        StringBuilder sb = new StringBuilder();
        sb.append(" CREATE DATABASE LINK " + dbLinkName.toUpperCase() + " ");
        sb.append(" CONNECT TO solars8_code IDENTIFIED BY solars8_code ");
        sb.append(" USING '(DESCRIPTION =(ADDRESS = (PROTOCOL = TCP)(HOST = 61.97.135.20)(PORT = 1521))(CONNECT_DATA = (SID=AL32UTF8)))' ");

        pstmt = destConn.prepareStatement(sb.toString());

        result = pstmt.execute();
      } else {      //있으면 Pass
        logger.debug("00.SOLARS8_CODE DBLINK is Exist. Passing...");
      }

      result = isExistsDatabaseLink();
      logger.debug("00.SOLARS8_CODE DBLINK Create END");
      logger.debug("========================================== ");
    } catch(SQLException e) {
      e.printStackTrace();
    } finally {
      if(pstmt != null) {
        try {
          pstmt.close();
        }catch (SQLException e) { }
      }
      if(rs != null) {
        try {
          rs.close();
        }catch (SQLException e) { }
      }
    }
    return result;
  }

  /**
   * DBLINK 가 존재하는지 확인
   * @return DBLINK 가 존재할 시 true / 없을 시 false
   */
  private boolean isExistsDatabaseLink() {
    PreparedStatement pstmt = null;
    ResultSet rs = null;
    Boolean result = true;

    try {
      Integer cntTable = 0;

      StringBuffer sb = new StringBuffer();
      sb.append(" SELECT count(1) ");
      sb.append(" FROM dba_objects ");
      sb.append(" WHERE object_type = 'DATABASE LINK' ");
      sb.append("  AND object_name = ? ");

      pstmt = destConn.prepareStatement(sb.toString());
      pstmt.setString(1, dbLinkName.toUpperCase());
      rs = pstmt.executeQuery();

      if(rs.next()) {
        cntTable = rs.getInt(1);
      }

      if(cntTable == 0) {
        result = false;
      }

    } catch (SQLException e){
      e.printStackTrace();
    } finally {
      if (pstmt != null) {
        try{
          pstmt.close();
        } catch (SQLException e) {        }
      }
      if (rs != null) {
        try{
          rs.close();
        } catch (SQLException e) {        }
      }
    }

    return result;
  }

  /**
   * 테이블이 존재하는지 확인
   * @param tableName
   * @return 테이블이 존재할 시 true / 없을 시 false
   */
  private boolean isExistsTable(String tableName) {
    PreparedStatement pstmt = null;
    ResultSet rs = null;
    Boolean result = true;

    try {
      Integer cntTable = 0;

      StringBuffer sb = new StringBuffer();
      sb.append(" SELECT count(1) ");
      sb.append("   FROM DBA_TABLES ");
      sb.append("  WHERE table_name = ? ");

      pstmt = destConn.prepareStatement(sb.toString());
      pstmt.setString(1, tableName.toUpperCase());
      rs = pstmt.executeQuery();

      if(rs.next()) {
        cntTable = rs.getInt(1);
      }

      if(cntTable == 0) {
        result = false;
      }

    } catch (SQLException e){
      e.printStackTrace();
    } finally {
      if (pstmt != null) {
        try{
          pstmt.close();
        } catch (SQLException e) {        }
      }
      if (rs != null) {
        try{
          rs.close();
        } catch (SQLException e) {        }
      }
    }

    return result;
  }

  /**
   * PK / INDEX 제약 조건이 존재하는 지 여부를 체크한다.
   * @param tableName
   * @return
   */
  private boolean isExistsIndex(String tableName, String indexName, String gubun) {
    PreparedStatement pstmt = null;
    ResultSet rs = null;
    Boolean result = true;

    try {
      Integer cntIndex = 0;

      StringBuffer sb = new StringBuffer();
      if (gubun.equals("PK") || gubun.equals("IDX") || gubun.equals("UIX")) {
        sb.append(" SELECT count(1) ");
        sb.append("   FROM ALL_INDEXES A, ALL_IND_COLUMNS B ");
        sb.append("  WHERE A.INDEX_NAME = B.INDEX_NAME ");
        sb.append("    AND A.TABLE_NAME= ? ");
        sb.append("    AND B.INDEX_NAME = ? ");
      } else if (gubun.equals("FK")) {
        sb.append(" SELECT count(1) ");
        sb.append(" FROM ALL_CONSTRAINTS ");
        sb.append(" WHERE constraint_name = ? ");
        sb.append("  AND table_name = ? ");
      }

      pstmt = destConn.prepareStatement(sb.toString());

      if ( gubun.equals("PK") || gubun.equals("IDX") || gubun.equals("UIX") ) {
        pstmt.setString(1, tableName.toUpperCase());
        pstmt.setString(2, indexName.toUpperCase());
      } else if ( gubun.equals("FK") ) {
        pstmt.setString(1, indexName.toUpperCase());
        pstmt.setString(2, tableName.toUpperCase());
      }

      rs = pstmt.executeQuery();

      if(rs.next()) {
        cntIndex = rs.getInt(1);
      }

      if(cntIndex == 0) {
        result = false;
      }

    } catch (SQLException e){
      e.printStackTrace();
    } finally {
      if (pstmt != null) {
        try{
          pstmt.close();
        } catch (SQLException e) {        }
      }
      if (rs != null) {
        try{
          rs.close();
        } catch (SQLException e) {        }
      }
    }

    return result;
  }

  /**
   * CREATE TABLE  xxx
   * CREATE INDEX  xxxx
   * CREATE UNIQUE INDEX  xxxx  ON yyyy
   * ALTER TABLE xxxxx ADD CONSTRAINT yyyy
   *
   * 전체 구조
   * 테이블명 < 인덱스 Type , Scripts >
   *   ex> COPY_T < COPY_T , CREATE TABLE .... >
   *       DB_TOKEN < PK , ALTER TABLE .... >
   *       DB_TOKEN < UIX , CREATE UNIQUE INDEX .... >
   *       DB_TOKEN < IDX , CREATE INDEX .... >
   * @return
   */
  private LinkedHashMap<String, LinkedHashMap<String, String>> makeSqlMap(String gubun) {

    BufferedReader fileReadbr = null; // 파일 읽기
    StringBuffer sb = new StringBuffer();
    try {
      if(gubun.equals("Solars8")) {
        fileReadbr = new BufferedReader(new FileReader(s8FilePath));
      } else if (gubun.equals("Smuf2")) {
        fileReadbr = new BufferedReader(new FileReader(smufFilePath));
      }

      String sqlInfo = "";
      String str;
      while ((str = fileReadbr.readLine()) != null) {
        sqlInfo = sb.append(str).toString();
      }

      String[] queryArr = sqlInfo.split(";");

      String tableName = "";
      String indexName = "";
      LinkedHashMap<String, String> valueMap = null;
      LinkedHashMap<String, String> existsValueMap = null;
      LinkedHashMap<String, String> existsConstMap = null;

      for (String query : queryArr) {
        String[] tmpTarget = query.split(" ");    // CREATE || TABLE || COPY_T

        if (query.indexOf("CREATE TABLE") == 0) {
          tableName = tmpTarget[2]; // CREATE TABLE xxxxx 에서 xxxx 만 가져온다.
          if (!targetMap.containsKey(tableName)) {
            existsConstMap = new LinkedHashMap<String, String>();
            existsConstMap.put(tableName, "TABLE");
            constraintsMap.putAll(existsConstMap);

            valueMap = new LinkedHashMap<String, String>();
            valueMap.put(tableName, query);
            targetMap.put(tableName, valueMap);
          }
        } else if (query.indexOf("ALTER TABLE") == 0 && query.indexOf("ADD CONSTRAINT") > 0) {

          if (targetMap.containsKey(tmpTarget[2])) {
            existsValueMap = targetMap.get(tmpTarget[2]);
            String constraintType = tmpTarget[5].substring(0,2);

            if (!existsValueMap.containsKey(tmpTarget[5])) {
              existsConstMap = new LinkedHashMap<String, String>();
              existsConstMap.put(tmpTarget[5], constraintType);
              constraintsMap.putAll(existsConstMap);

              valueMap = new LinkedHashMap<String, String>();
              valueMap.put(tmpTarget[5], query);
              existsValueMap.putAll(valueMap);
            }
          }
          targetMap.put(tmpTarget[2], existsValueMap);

        } else if (query.indexOf("ON") > 0) {
          valueMap = new LinkedHashMap<String, String>();

          if (query.indexOf("CREATE INDEX") == 0 && !valueMap.containsKey(tmpTarget[2])) {

            if (targetMap.containsKey(tmpTarget[4])) {     // 테이블 명으로 찾고
              existsValueMap = targetMap.get(tmpTarget[4]);      // 있으면 가져온다
              String constraintType = tmpTarget[2].substring(0,3);

              if (!existsValueMap.containsKey(tmpTarget[2])) {   // 제약조건 명으로 찾고
                existsConstMap = new LinkedHashMap<String, String>();
                existsConstMap.put(tmpTarget[2], constraintType);
                constraintsMap.putAll(existsConstMap);

                //valueMap = new LinkedHashMap<>();
                valueMap.put(tmpTarget[2], query);         // 없으면 넣는다.
                existsValueMap.putAll(valueMap);
              }
            }
            targetMap.put(tmpTarget[4], existsValueMap);

          } else if (query.indexOf("CREATE UNIQUE INDEX") == 0 && !valueMap.containsKey(tmpTarget[3])) {

            if (targetMap.containsKey(tmpTarget[5])) {     // 테이블 명으로 찾고
              existsValueMap = targetMap.get(tmpTarget[5]);      // 있으면 가져온다
              String constraintType = tmpTarget[3].substring(0,3);

              if (!existsValueMap.containsKey(tmpTarget[3])) {   // 제약조건 명으로 찾고
                constraintsMap.put(tmpTarget[3], constraintType);
                //valueMap = new LinkedHashMap<>();
                valueMap.put(tmpTarget[3], query);         // 없으면 넣는다.
                existsValueMap.putAll(valueMap);
              }
            }
            targetMap.put(tmpTarget[5], existsValueMap);
          }
        }
      }
    } catch (IOException e) {
      e.printStackTrace();

    } finally {
      try{
        fileReadbr.close();
      } catch (IOException e) {      }

    }

    return targetMap;
  }

  /**
   * ConstraintsType 으로 테이블 및 INDEX 가 존재하는지 확인한다.
   *
   * @param gubun TABLE 인지 Constraint 인지 여부
   * @param tableName  인덱스 존재 여부를 체크시 사용
   * @param constraintName Constraint 명
   */
  private boolean isExistsInDb (String tableName, String constraintName, String gubun) {
    Boolean isExist = false;
    ArrayList<String> tableKey = new ArrayList<String>();
    tableKey.add("TABLE");

    ArrayList<String> indexKey = new ArrayList<String>();
    indexKey.add("IDX");
    indexKey.add("UIX");
    indexKey.add("PK");
    indexKey.add("FK");

    if (tableKey.contains(gubun)) {
      isExist = isExistsTable(tableName);
    } else if (indexKey.contains(gubun)) {
      isExist = isExistsIndex(tableName, constraintName, gubun);
    }

    return isExist;
  }

  /**
   * Solars8 테이블 생성
   * @return result 개 중 하나만 false 여도 false
   */
  private boolean createS8Table() {
    PreparedStatement pstmt = null;
    Boolean result = false;
    String exceptionObjectName = "";
    String exceptionSql = "";

    try{
      logger.debug("========================================== ");
      logger.debug("01. SOLARS8 Tables && Indexes Creating .....");
      HashMap<String, LinkedHashMap<String, String>> s8SqlMap = makeSqlMap("Solars8");
      Iterator s8Iterator = s8SqlMap.keySet().iterator();   // 전체 KEY

      while (s8Iterator.hasNext()) {
        String key = (String) s8Iterator.next();
        LinkedHashMap<String, String> valuesMap = s8SqlMap.get(key);
        Iterator queryIter = valuesMap.keySet().iterator();

        while (queryIter.hasNext()) {

          String constKey = queryIter.next().toString();    //tableName
          exceptionObjectName = constKey;
          String query = valuesMap.get(constKey);         // CREATE TABLE 일 시는 TableName
                                                          // ALTER TABLE 일때는 PK || FK
                                                          // CREATE INDEX 일 때는 UIX || IDX
          exceptionSql = query;
          Iterator constIter = constraintsMap.keySet().iterator();
          while(constIter.hasNext()) {
            Boolean isExists = false;
            String constName = (String) constIter.next(); // Constraint Name
            if (constName.equals(constKey)) {

              String type = constraintsMap.get(constName);
              if(type.equals("TABLE")) {
                isExists = isExistsInDb(constKey, constKey, type);
              } else { // PK 와 FK 를 분리..
                isExists = isExistsInDb(key, constName, type);
              }

              if(!isExists) {
                pstmt = destConn.prepareStatement(query);
                // 결과 값을 받아봐야 의미가 없다... CREATE || ALTER 는 result 가 항상 false 내지 0 이다.
                pstmt.executeUpdate();
                pstmt.close();

                if(type.equals("TABLE")) {
                  isExists = isExistsInDb(constKey, constKey, type);
                  logger.info("[Solars8] [ {} ] - [ {}  : {} ] is Exists ? : {}", key, constKey, type, isExists);
                } else {
                  isExists = isExistsInDb(key, constName, type);
                  logger.info("[Solars8] [ {} ] - [ {} : {} ] is Exists ? : {}", key, constName, type, isExists);
                }
                result = isExists;
              } else {
                logger.info("[Solars8] [ {} ] is Exists. Passing.. ", key);
              }
            }

          }
        }
      }
      logger.debug("01. SOLARS8 Tables && Indexes Create End ");
      logger.debug("========================================== ");
    }catch(Exception e){
      logger.error("[ {} ] Query : {}", exceptionObjectName, exceptionSql);
      e.printStackTrace();
    } finally {
      if(pstmt != null) {
        try{
          pstmt.close();
        }catch(SQLException e) { }
      }
    }
    return result;
  }


  /**
   * Solars8 테이블 생성
   * @return result 개 중 하나만 false 여도 false
   */
  private boolean createSmuf2Table() {
    PreparedStatement pstmt = null;
    Boolean result = false;
    String exceptionObjectName = "";
    String exceptionSql = "";

    try{
      logger.debug("========================================== ");
      logger.debug("02. SMUF2 Tables && Indexes Creating .....");
      HashMap<String, LinkedHashMap<String, String>> s8SqlMap = makeSqlMap("Smuf2");
      Iterator s8Iterator = s8SqlMap.keySet().iterator();   // 전체 KEY

      while (s8Iterator.hasNext()) {
        String key = (String) s8Iterator.next();
        LinkedHashMap<String, String> valuesMap = s8SqlMap.get(key);
        Iterator queryIter = valuesMap.keySet().iterator();

        while (queryIter.hasNext()) {

          String constKey = queryIter.next().toString();    //tableName
          exceptionObjectName = constKey;
          String query = valuesMap.get(constKey);         // CREATE TABLE 일 시는 TableName
                                                          // ALTER TABLE 일때는 PK || FK
                                                          // CREATE INDEX 일 때는 UIX || IDX
          exceptionSql = query;
          Iterator constIter = constraintsMap.keySet().iterator();
          while(constIter.hasNext()) {
            Boolean isExists = false;
            String constName = (String) constIter.next(); // Constraint Name
            if (constName.equals(constKey)) {
              String type = constraintsMap.get(constName);
              if(type.equals("TABLE")) {
                isExists = isExistsInDb(constKey, constKey, type);
              } else {
                isExists = isExistsInDb(key, constName, type);
              }

              if(!isExists) {
                pstmt = destConn.prepareStatement(query);
                // 결과 값을 받아봐야 의미가 없다... CREATE || ALTER 는 result 가 항상 false 내지 0 이다.
                pstmt.executeUpdate();
                pstmt.close();

                if(type.equals("TABLE")) {
                  isExists = isExistsInDb(constKey, constKey, type);
                  logger.info("[SMUF2] [ {} ] - [ {}  : {} ] is Exists ? : {}", key, constKey, type, isExists);
                } else {
                  isExists = isExistsInDb(key, constName, type);
                  logger.info("[SMUF2] [ {} ] - [ {} : {} ] is Exists ? : {}", key, constName, type, isExists);
                }
                result = isExists;
              } else {
                logger.info("[SMUF2] [ {} ] is Exists. Passing... ", key);
              }
            }
          }
        }
      }
      logger.debug("02. SMUF2 Tables && Indexes Create End ");
      logger.debug("========================================== ");
    }catch(Exception e){
      logger.error("[ {} ] Query : {}", exceptionObjectName, exceptionSql);
      e.printStackTrace();
    } finally {
      if(pstmt != null) {
        try{
          pstmt.close();
        }catch(SQLException e) { }
      }
    }
    return result;
  }

}
