package kr.tanbbang01.converter.db.init;

import kr.tanbbang01.converter.db.init.data.FirstDataInit;
import kr.tanbbang01.converter.db.init.sequence.SeqAndFuncInit;
import kr.tanbbang01.converter.db.init.table.TableInit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;

/**
 * 실행 순서
 * 1. table : DBLINK & TABLE 생성
 * 2. seqFunc : 시퀀스 및 변환시 필요한 Function 생성
 * 3. data : SOLARS8_CODE 에서 기초 데이터를 가져온다.
 *
 * Uses ConfigFile target
 * Created by InSeong on 17. 11. 20..
 */
public class ConvertStarter {
  private static Logger logger = LoggerFactory.getLogger(ConvertStarter.class);

  public static void main(String[] args) throws SQLException, IOException, ParseException {
    if (args.length != 2) {
      System.err.println("Usage: configFile target");
      System.exit(-1);
    } else {
      logger.info("Converter Started !!");
      ConvertStarter convert = new ConvertStarter();
      convert.convert(args[0], args[1]);
      logger.info("Converter End !!");
    }
  }

  private void convert(String configFileName, String target) throws SQLException{
    if(target.equals("table")) {
      TableInit table = new TableInit(configFileName);
      table.create();
    } else if(target.equals("seqFunc")) {
      SeqAndFuncInit init = new SeqAndFuncInit(configFileName);
      init.create();
    } else if(target.equals("data")) {
      FirstDataInit init = new FirstDataInit(configFileName);
      init.create();
    } else if (target.equals("all")) {
      TableInit table = new TableInit(configFileName);
      table.create();
      SeqAndFuncInit seqAndFuncInit = new SeqAndFuncInit(configFileName);
      seqAndFuncInit.create();
      FirstDataInit firstDataInit = new FirstDataInit(configFileName);
      firstDataInit.create();
    }
  }

}
