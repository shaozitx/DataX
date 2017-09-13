package com.alibaba.datax.plugin.reader.mongodbreader;
import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.mongodbreader.util.CollectionSplitUtil;
import com.alibaba.datax.plugin.reader.mongodbreader.util.MongoUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.Document;

import java.util.*;

public class MongoDBReader extends Reader {

    public static class Job extends Reader.Job {

        private Configuration originalConfig = null;

        private MongoClient mongoClient;

        private String userName = null;
        private String password = null;

        @Override
        public List<Configuration> split(int adviceNumber) {
            return CollectionSplitUtil.doSplit(originalConfig,adviceNumber,mongoClient);
        }

        @Override
            public void init() {
            this.originalConfig = super.getPluginJobConf();
                this.userName = originalConfig.getString(KeyConstant.MONGO_USER_NAME);
                this.password = originalConfig.getString(KeyConstant.MONGO_USER_PASSWORD);
                String database =  originalConfig.getString(KeyConstant.MONGO_DB_NAME);
            if(!Strings.isNullOrEmpty(this.userName) && !Strings.isNullOrEmpty(this.password)) {
                this.mongoClient = MongoUtil.initCredentialMongoClient(originalConfig,userName,password,database);
            } else {
                this.mongoClient = MongoUtil.initMongoClient(originalConfig);
            }
        }

        @Override
        public void destroy() {

        }
    }


    public static class Task extends Reader.Task {

        private Configuration readerSliceConfig;

        private MongoClient mongoClient;

        private String userName = null;
        private String password = null;

        private String database = null;
        private String collection = null;

        private String query = null;

        private JSONArray mongodbColumnMeta = null;
        private Long batchSize = null;
        /**
         * 用来控制每个task取值的offset
         */
        private Long skipCount = null;
        /**
         * 每页数据的大小
         */
        private int pageSize = 1000;


        /**
         * 更新时间差
         */
        private Integer updateseconds = null;


        /**
         * 数组模式
         */
        private String arrayname = null;


        @Override
        public void startRead(RecordSender recordSender) {

            if(batchSize == null ||
                             mongoClient == null || database == null ||
                             collection == null  || mongodbColumnMeta == null) {
                throw DataXException.asDataXException(MongoDBReaderErrorCode.ILLEGAL_VALUE,
                        MongoDBReaderErrorCode.ILLEGAL_VALUE.getDescription());
            }
            MongoDatabase db = mongoClient.getDatabase(database);
            MongoCollection col = db.getCollection(this.collection);
            BsonDocument sort = new BsonDocument();
            sort.append(KeyConstant.MONGO_PRIMIARY_ID_META, new BsonInt32(1));

            long pageCount = batchSize / pageSize;
            int modCount = (int)(batchSize % pageSize);

            for(int i = 0; i <= pageCount; i++) {
                if(modCount == 0 && i == pageCount) {
                    break;
                }
                if (i == pageCount) {
                    pageSize = modCount;
                }
                MongoCursor<Document> dbCursor = null;
                if(Strings.isNullOrEmpty(arrayname)) {  // 不是数组模式

                    if (updateseconds == 0){  // 全量更新
                        dbCursor = col.find().sort(sort)
                                .skip(skipCount.intValue()).limit(pageSize).iterator();

                    }else { // 增量更新
                        Date date  = new Date();
                        Long datel = date.getTime();
                        date.setTime(datel - updateseconds * 1000);
                        dbCursor = col.find(Filters.gt("updatetime",date.getTime())).sort(sort)
                                .skip(skipCount.intValue()).limit(pageSize).iterator();

                    }

                }else {   // 数组模式取表

                    if (updateseconds == 0){

                        Document unwind = new Document("$unwind","$"+arrayname);
                        List<Document> bsons = new ArrayList<Document>();
                        Document limit = new Document("$limit",1000);
                        bsons.add(limit);
                        bsons.add(unwind);
                        dbCursor = col.aggregate(bsons).iterator();
                    }else {

                        Date date  = new Date();
                        Long datel = date.getTime();
                        date.setTime(datel - updateseconds * 1000);

                        Document unwind = new Document("$unwind","$"+arrayname);
                        List<Document> bsons = new ArrayList<Document>();
                        Document limit = new Document("$limit",1000);
                        Document skip = new Document("$skip",skipCount);
                        Document group = new Document();
                        group.put("$match", new Document("updatetime", new Document("$gt", date.getTime())));
                        bsons.add(group);
                        bsons.add(skip);
                        bsons.add(limit);
                        bsons.add(unwind);
                        dbCursor = col.aggregate(bsons).iterator();
                    }

                }


                int i1 = 0;
                while (dbCursor.hasNext()) {

                    Document item = dbCursor.next();
                    Record record = recordSender.createRecord();
                    Iterator columnItera = mongodbColumnMeta.iterator();

                    while (columnItera.hasNext()) {
                        JSONObject column = (JSONObject)columnItera.next();
                        String col_ = column.getString(KeyConstant.COLUMN_NAME);

                        Object tempCol = item.get(col_);

                        if (col_.contains("-")){
                            String[] cols = col_.split("-");
                            Document item1 = (Document)item.get(cols[0]);
                            tempCol = item1.get(cols[1]);
                        }



                        if (tempCol instanceof Double) {
                            record.addColumn(new DoubleColumn((Double) tempCol));
                        } else if (tempCol instanceof Boolean) {
                            record.addColumn(new BoolColumn((Boolean) tempCol));
                        } else if (tempCol instanceof Date) {
                            record.addColumn(new DateColumn((Date) tempCol));
                        } else if (tempCol instanceof Integer) {
                            record.addColumn(new LongColumn((Integer) tempCol));
                        }else if (tempCol instanceof Long) {
                            record.addColumn(new LongColumn((Long) tempCol));
                        }else if (tempCol == null){
                            String type = column.getString(KeyConstant.COLUMN_TYPE);
                            if (type.equals("Double")){
                                record.addColumn(new DoubleColumn((Double) tempCol));

                            }else if (type.equals("Boolean")){
                                record.addColumn(new BoolColumn((Boolean) tempCol));

                            }else if (type.equals("Date")){
                                record.addColumn(new DateColumn((Date) tempCol));

                            }else if (type.equals("Integer")){
                                record.addColumn(new LongColumn((Integer) tempCol));

                            }else{
                                record.addColumn(new LongColumn((Long) tempCol));

                            }

                        } else {
                            if(KeyConstant.isArrayType(column.getString(KeyConstant.COLUMN_TYPE))) {
                                String splitter = column.getString(KeyConstant.COLUMN_SPLITTER);
                                if(Strings.isNullOrEmpty(splitter)) {
                                    throw DataXException.asDataXException(MongoDBReaderErrorCode.ILLEGAL_VALUE,
                                            MongoDBReaderErrorCode.ILLEGAL_VALUE.getDescription());
                                } else {
                                    ArrayList array = (ArrayList)tempCol;
                                    String tempArrayStr = Joiner.on(splitter).join(array);
                                    record.addColumn(new StringColumn(tempArrayStr));
                                }
                            } else {
                                record.addColumn(new StringColumn(tempCol.toString()));
                            }
                        }
                    }
                    recordSender.sendToWriter(record);
                }

                skipCount += pageSize;
            }
        }


        @Override
        public void init() {
            this.readerSliceConfig = super.getPluginJobConf();
            this.userName = readerSliceConfig.getString(KeyConstant.MONGO_USER_NAME);
            this.password = readerSliceConfig.getString(KeyConstant.MONGO_USER_PASSWORD);
            this.database = readerSliceConfig.getString(KeyConstant.MONGO_DB_NAME);
            if(!Strings.isNullOrEmpty(userName) && !Strings.isNullOrEmpty(password)) {
                mongoClient = MongoUtil.initCredentialMongoClient(readerSliceConfig,userName,password,database);
            } else {
                mongoClient = MongoUtil.initMongoClient(readerSliceConfig);
            }

            this.collection = readerSliceConfig.getString(KeyConstant.MONGO_COLLECTION_NAME);
            this.query = readerSliceConfig.getString(KeyConstant.MONGO_QUERY);
            this.mongodbColumnMeta = JSON.parseArray(readerSliceConfig.getString(KeyConstant.MONGO_COLUMN));
            this.batchSize = readerSliceConfig.getLong(KeyConstant.BATCH_SIZE);
            this.skipCount = readerSliceConfig.getLong(KeyConstant.SKIP_COUNT);
            this.updateseconds = readerSliceConfig.getInt(KeyConstant.UPDATESECONDS, 0);
            this.arrayname = readerSliceConfig.getString(KeyConstant.ARRAYNAME);

        }
        @Override
        public void destroy() {

        }
    }
}
