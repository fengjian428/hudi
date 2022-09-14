package org.apache.hudi;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.PartialUpdateAvroPayload;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.action.commit.FlinkWriteHelper;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class TestFlinkWriteHelper {
  private transient Schema avroSchema;

  private String preCombineFields = "";

  public static final String SCHEMA = "{\n"
      + "  \"type\": \"record\",\n"
      + "  \"name\": \"partialRecord\", \"namespace\":\"org.apache.hudi\",\n"
      + "  \"fields\": [\n"
      + "    {\"name\": \"id\", \"type\": [\"null\", \"string\"]},\n"
      + "    {\"name\": \"fa\", \"type\": [\"null\", \"string\"]},\n"
      + "    {\"name\": \"_ts1\", \"type\": [\"null\", \"long\"]},\n"
      + "    {\"name\": \"fb\", \"type\": [\"null\", \"string\"]},\n"
      + "    {\"name\": \"_ts2\", \"type\": [\"null\", \"long\"]}\n"
      + "  ]\n"
      + "}";

  @TempDir
  File tempFile;

  @BeforeEach
  public void setUp() throws Exception {
    this.preCombineFields = "_ts1:fa;_ts2:fb";
    this.avroSchema = new Schema.Parser().parse(SCHEMA);
  }

  @Test
  void deduplicateRecords() throws IOException, InterruptedException {
    List<HoodieAvroRecord> records = data();
    List<HoodieAvroRecord> mergedRecords = FlinkWriteHelper.newInstance().deduplicateRecords(records, (HoodieIndex) null, -1, this.avroSchema.toString());
    GenericRecord record1 = HoodieAvroUtils.bytesToAvro(((PartialUpdateAvroPayload) records.get(records.size() - 1).getData()).recordBytes, this.avroSchema);
    GenericRecord record = HoodieAvroUtils.bytesToAvro(((PartialUpdateAvroPayload) mergedRecords.get(0).getData()).recordBytes, this.avroSchema);
    System.out.println("======================================================================================");
    System.out.println("last in set: " + record1);
    System.out.println("last: " + record);
  }

  public List<HoodieAvroRecord> data() throws InterruptedException {
    AtomicInteger faCnt = new AtomicInteger(1);
    AtomicInteger fbCnt = new AtomicInteger(1);
    List<GenericRecord> records = new ArrayList<>();
    for (int i = 1; i <= 100; i++) {
      long ts = System.currentTimeMillis();
      GenericRecord row1 = new GenericData.Record(this.avroSchema);
      row1.put("id", "jack");
      row1.put("fa", faCnt.getAndIncrement() + "");
      row1.put("_ts1", ts);
      GenericRecord row2 = new GenericData.Record(this.avroSchema);
      row2.put("id", "jack");
      row2.put("fb", fbCnt.getAndIncrement() + "");
      row2.put("_ts2", ts);
      records.add(row1);
      records.add(row2);
//      Thread.sleep(1);
    }

    return records.stream().map(genericRowData -> {
      try {
        return new HoodieAvroRecord(new HoodieKey("1", "default"),
            new PartialUpdateAvroPayload(genericRowData, preCombineFields), HoodieOperation.INSERT);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }).collect(Collectors.toList());
  }
}
