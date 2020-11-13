/**
 * Copyright 2020-9999 the original author or authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.mykit.db.transfer.sync.impl;

import io.mykit.db.common.constants.MykitDbSyncConstants;
import io.mykit.db.common.utils.StringUtils;
import io.mykit.db.transfer.entity.JobInfo;
import io.mykit.db.transfer.sync.DBSync;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * @author binghe
 * @version 1.0.0
 * @description Oracle数据库同步实现
 */
public class OracleSync extends AbstractDBSync implements DBSync {

    private Logger logger = LoggerFactory.getLogger(OracleSync.class);
    private DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Override
    public String assembleSQL(String srcSql, Connection conn, JobInfo jobInfo) throws SQLException {
        String replaceSql = jobInfo.getReplaceSql();
        if(replaceSql!=null&&replaceSql.length()>0){
            String replaceVal = getParamValue(conn, replaceSql);
            if(replaceVal!=null&&!replaceVal.equals(""))
                srcSql = srcSql.replaceAll("\\$\\{\\w+\\}", replaceVal);
        }
        String syncTime = df.format(new Date());
        String[] destFields = jobInfo.getDestTableFields().split(",");
        destFields = this.trimArrayItem(destFields);
        String[] srcFields = destFields;
        String srcField = jobInfo.getSrcTableFields();
        if(!StringUtils.isEmpty(jobInfo.getSrcTableFields())){
            srcFields = this.trimArrayItem(srcField.split(MykitDbSyncConstants.FIELD_SPLIT));
        }
        Map<String, String> fieldMapper = this.getFieldsMapper(srcFields, destFields);
//        System.out.println(fieldMapper.toString());
        String[] updateFields = jobInfo.getDestTableUpdate().split(MykitDbSyncConstants.FIELD_SPLIT);
        updateFields = this.trimArrayItem(updateFields);
        String destTable = jobInfo.getDestTable();
        String destTableKey = jobInfo.getDestTableKey();
        PreparedStatement pst = conn.prepareStatement(srcSql);
        ResultSet rs = pst.executeQuery();
        StringBuilder sql = new StringBuilder();
        sql.append("begin ");
        while (rs.next()){
            sql.append(" update ").append(destTable).append(" set ");
            //拼接更新语句
            for(int i = 0; i < updateFields.length - 1; i++){
                //取得查询的数据
                String currentColumn = fieldMapper.get(updateFields[i].trim());
//                System.out.println(currentColumn);
                Object fieldValue = rs.getObject(currentColumn);
                if (fieldValue == null){
                    sql.append(updateFields[i] + " = " + fieldValue + ", ");
                }else{
                    String fieldValueStr = fieldValue.toString();
                    //时间分割符
                    if (/*fieldValueStr.contains(MykitDbSyncConstants.DATE_SPLIT)*/fieldValueStr.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}")){
                        sql.append(updateFields[i] + " = to_date('" + fieldValue + "', 'yyyy-mm-dd hh24:mi:ss'), ");
                    }else if(fieldValueStr.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d+")){
                        sql.append(updateFields[i] + " = to_timestamp('" + fieldValue + "', 'yyyy-mm-dd hh24:mi:ss.ff'), ");
                    }else{
                        sql.append(updateFields[i] + " = '" + fieldValue + "', ");
                    }
                }
            }
            Object fieldValue = rs.getObject(fieldMapper.get(updateFields[updateFields.length - 1].trim()));
            if (fieldValue == null){
                sql.append(updateFields[updateFields.length - 1] + " = " + fieldValue);
            }else{
                String fieldValueStr = fieldValue.toString();
                //时间分割符
                if (/*fieldValueStr.contains(MykitDbSyncConstants.DATE_SPLIT)*/fieldValueStr.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}")){
                    sql.append(updateFields[updateFields.length - 1] + " = to_date('" + fieldValue + "', '"+MykitDbSyncConstants.ORACLE_DATE_FORMAT+"') ");
                }else if(fieldValueStr.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d+")){
                    sql.append(updateFields[updateFields.length - 1] +  " = to_timestamp('" + fieldValue + "', 'yyyy-mm-dd hh24:mi:ss.ff') ");
                }else{
                    sql.append(updateFields[updateFields.length - 1] + " = '" + fieldValue + "'");
                }
            }
            sql.append(", sync_time = to_date('" + syncTime + "', 'yyyy-mm-dd hh24:mi:ss')");
            sql.append( " where " );
//            sql.append(destTableKey).append(" = '"+rs.getObject(fieldMapper.get(destTableKey))+"';");
            String[] keys = destTableKey.split(",");
            for(int i=0;i<keys.length;i++){
                sql.append(keys[i]).append("='").append(rs.getObject(fieldMapper.get(keys[i]))).append("' ").append(i<keys.length-1?" and ":";");
            };
            sql.append("  if sql%notfound then ");
            sql.append(" insert into ").append(destTable).append(" (").append(jobInfo.getDestTableFields()).append(",sync_time").append(") values ( ");
            for (int index = 0; index < destFields.length; index++) {
                Object value = rs.getObject(fieldMapper.get(destFields[index].trim()));
                if (value == null){
                    sql.append(value).append(index == (destFields.length - 1) ? "" : ",");
                }else{
                    String valueStr = value.toString();
                    if (/*valueStr.contains(MykitDbSyncConstants.DATE_SPLIT)*/valueStr.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}")){
                        sql.append(" to_date('" + fieldValue + "', '"+MykitDbSyncConstants.ORACLE_DATE_FORMAT+"') ").append(index == (destFields.length - 1) ? "" : ",");
                    }else if(valueStr.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d+")){
                        sql.append(" to_timestamp('" + fieldValue + "', 'yyyy-mm-dd hh24:mi:ss.ff') ").append(index == (destFields.length - 1) ? "" : ",");
                    }else{
                        sql.append("'").append(value).append(index == (destFields.length - 1) ? "'" : "',");
                    }
                }
            }
            sql.append(",to_date('" + syncTime + "', 'yyyy-mm-dd hh24:mi:ss')");
            sql.append(" );");
            sql.append(" end if; ");
        }
        if("true".equalsIgnoreCase(System.getProperty("isDelete")))
            sql.append("delete from ").append(destTable).append(" where sync_time is null or sync_time < to_date('" + syncTime + "', 'yyyy-mm-dd hh24:mi:ss');");
        sql.append(" end; ");
        logger.debug(sql.toString());
        return sql.toString();
    }

    private String getParamValue(Connection conn, String replaceSql) {
        Statement statement = null;
        ResultSet rs = null;
        try {
            statement = conn.createStatement();
            rs = statement.executeQuery(replaceSql);
            if(rs.next()) return rs.getString(1);
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            try {
                if (rs != null)
                    rs.close();
                if (statement != null)
                    statement.close();
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        return "";
    }

    @Override
    public void executeSQL(String sql, Connection conn) throws SQLException {
        PreparedStatement pst = conn.prepareStatement(sql);
        pst.executeUpdate();
        conn.commit();
        pst.close();
    }
}
