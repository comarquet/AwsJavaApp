import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.S3Object;

public class SummarizeWorkerApp {
  
  private static final String INPUT_BUCKET = "rawdata64987321";
  private static final String OUTPUT_BUCKET = "summarizeworker2569874";
  
  public static void main(String[] args) {
    
    SummarizeWorker worker = new SummarizeWorker(OUTPUT_BUCKET);
    
    try {
      S3Client s3Client = S3Client.builder().build();
      
      ListObjectsRequest listReq = ListObjectsRequest.builder()
        .bucket(INPUT_BUCKET)
        .build();
      
      ListObjectsResponse listRes = s3Client.listObjects(listReq);
      
      for (S3Object obj : listRes.contents()) {
        String key = obj.key();
        System.out.println("Processing file: " + key);
        String result = worker.run(INPUT_BUCKET, key);
        System.out.println(result);
      }
      
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}

