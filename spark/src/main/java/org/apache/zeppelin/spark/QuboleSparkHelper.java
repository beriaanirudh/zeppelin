package org.apache.zeppelin.spark;

/** Qubole Helper
 */
public class QuboleSparkHelper {

  public static String jobUrl(boolean isQuboleSpark, String sparkUrl, int jobId) {
    String jobUrl = null;
    if (isQuboleSpark) {
      // Example: From Spark UI
      // UI url:
      //https://qa2.qubole.net/cluster-proxy?encodedUrl=http%3A%2F%2F<dns>
      //%3A8088%2Fproxy%2Fapplication_1481645009353_0002%3Fspark%3Dtrue
      // Job url:
      // https://qa2.qubole.net/cluster-proxy?encodedUrl=http%3A%2F%2F<dns>
      //%3A8088%2Fproxy%2Fapplication_1481645009353_0002%/jobs/job?id=1
      int indexOf = sparkUrl.indexOf("%3F");
      jobUrl = sparkUrl.substring(0, indexOf);
      jobUrl = jobUrl + "/jobs/job?id=" + jobId;
    } else {
      jobUrl = sparkUrl + "/jobs/job?id=" + jobId;
    }
    return jobUrl;
  }
}
